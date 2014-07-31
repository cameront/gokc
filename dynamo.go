package gokc

import (
	"log"
	"math"
	"math/rand"
	"time"
)

const (
	NUM_RETRIES = 3
)

// HACK: Figure out better way to set dynamo config.

func NewDynamoLeaseManager(config *Config) *DynamoLeaseManager {
	return &DynamoLeaseManager{
		workerId:  config.WorkerId,
		tableName: config.LeaseTable.Name,
	}
}

// DynamoLeaseManager is...
type DynamoLeaseManager struct {
	tableName string
	workerId  string
}

func (self *DynamoLeaseManager) CreateTable(readCapacity, writeCapacity uint64) error {
	return CreateTable(self.tableName, readCapacity, writeCapacity)
}

func (self *DynamoLeaseManager) WaitForTable(pollSeconds, timeoutSeconds time.Duration) error {
	return WaitUntilTableExists(self.tableName, pollSeconds, timeoutSeconds)
}

func (self *DynamoLeaseManager) Create(lease *Lease) error {
	return PutLease(self.tableName, lease)
}

func (self *DynamoLeaseManager) List() []*Lease {
	result, err := Scan(self.tableName, 0)
	if err != nil {
		log.Fatal(err)
	}
	return result
}

func (self *DynamoLeaseManager) Renew(lease *Lease) error {
	return UpdateLease(self.tableName, lease, self.workerId)
}

func (self *DynamoLeaseManager) Take(lease *Lease) error {
	return UpdateLease(self.tableName, lease, self.workerId)
}

func (self *DynamoLeaseManager) WorkerId() string {
	return self.workerId
}

func (self *DynamoLeaseManager) TableName() string {
	return self.tableName
}

//
//

func NewDynamoLeaseTaker(config *Config) *DynamoLeaseTaker {
	rand.Seed(time.Now().Unix())
	return &DynamoLeaseTaker{
		manager:              NewDynamoLeaseManager(config),
		leaseMap:             make(map[string]*Lease),
		leaseDurationSeconds: time.Duration(config.LeaseDurationSeconds) * time.Second,
	}
}

// DynamoLeaseTaker is...
type DynamoLeaseTaker struct {
	manager              LeaseManager
	leaseMap             map[string]*Lease
	leaseDurationSeconds time.Duration
}

func (self *DynamoLeaseTaker) Start(shards <-chan Shard) <-chan Lease {
	// Taker runs with fixed DELAY because we want it to run slower
	// in the event of performance degredation. The first timer fires
	// immediately for aggressive lease-load-balacing purposes.
	takeTimer := time.NewTimer(500 * time.Millisecond).C
	outChannel := make(chan Lease, 10)
	go func(newLeases chan<- Lease) {
		defer close(newLeases)
		for {
			select {
			case <-takeTimer:
				taken := self.Take()
				for _, t := range taken {
					newLeases <- *t
				}
				takeTimer = time.NewTimer(self.leaseDurationSeconds * 2).C
			case shard, ok := <-shards:
				if !ok {
					log.Print("Take: Channel closed. Exiting.")
					return
				}
				if _, found := self.leaseMap[shard.ShardId]; !found {
					if !isReadyForProcessing(shard, self.leaseMap) {
						log.Printf("Take: New shard found %s, but not ready for processing. Not taking.", shard.ShardId)
						continue
					}
					log.Print("Take: Received unknown shard. Attempting to create lease: ", shard.ShardId)
					lease := &Lease{key: shard.ShardId, owner: self.manager.WorkerId(), counter: 0}
					self.leaseMap[shard.ShardId] = lease
					if err := self.manager.Create(lease); err != nil {
						log.Print("Take: Error creating lease: ", err)
					} else {
						newLeases <- *lease
					}
				}
			}
		}
	}(outChannel)
	return outChannel
}

func isReadyForProcessing(shard Shard, leaseMap map[string]*Lease) bool {
	if shard.ParentShardId != "" && !isShardDone(shard.ParentShardId, leaseMap) {
		log.Printf("Parent shard of %s not yet completed.", shard.ShardId)
		return false
	}
	if shard.AdjacentParentShardId != "" && !isShardDone(shard.AdjacentParentShardId, leaseMap) {
		log.Printf("Adjacent parent shard of %s not yet completed.", shard.ShardId)
	}
	return true
}

func isShardDone(shardId string, leaseMap map[string]*Lease) bool {
	if lease, found := leaseMap[shardId]; found {
		return lease.ShardDone()
	}
	return false
}

func (self *DynamoLeaseTaker) take(lease *Lease) bool {
	for i := 0; i < NUM_RETRIES; i++ {
		err := self.manager.Take(lease)
		if err != nil {
			log.Printf("Error taking on attempt: %d. %s", i+1, err)
		} else {
			return true
		}
	}
	return false
}

func (self *DynamoLeaseTaker) Take() []*Lease {
	self.updateAllLeases()
	expired, ownerCount := self.getExpiredLeases()
	toTake := self.getLeasesToTake(expired, ownerCount)
	if len(toTake) > 0 {
		log.Printf("Take: Attempting to take %d leases for %s.", len(toTake), self.manager.WorkerId())
	}
	taken := []*Lease{}
	for _, lease := range toTake {
		if self.take(lease) {
			log.Printf("Take: Successfully took lease: %s", lease.key)
			taken = append(taken, lease)
		} else {
			log.Printf("Take: Failed to take lease: %s", lease.key)
		}
	}
	return taken
}

// TODO(Could the manager do this?) Share with TaskRenewer?
func (self *DynamoLeaseTaker) updateAllLeases() {
	latestLeases := self.manager.List()
	staleLeases := map[string]bool{}
	for _, lease := range self.leaseMap {
		staleLeases[lease.key] = true
	}
	for _, lease := range latestLeases {
		old, found := self.leaseMap[lease.key]
		self.leaseMap[lease.key] = lease
		delete(staleLeases, lease.key)
		if found {
			if old.counter == lease.counter {
				// Propagate the last increment time to the new lease.
				lease.lastUpdate = old.lastUpdate
			} else {
				// The value changed, so reset the isStale clock.
				lease.lastUpdate = time.Now().UTC()
			}
		} else {
			if lease.owner == "" {
				lease.lastUpdate = time.Time{} // Zero value.
			} else {
				lease.lastUpdate = time.Now().UTC()
			}
		}
	}
	for stale, _ := range staleLeases {
		// TODO: If config.DeleteOldLeases { self.manager.Delete(lease) }
		delete(self.leaseMap, stale)
	}
}

func (self *DynamoLeaseTaker) getExpiredLeases() ([]*Lease, map[string]int) {
	expired := []*Lease{}
	ownerLiveLeaseCount := map[string]int{}
	for _, lease := range self.leaseMap {
		if lease.ShardDone() {
			// Leases aren't renewed once completed. So ignore them.
			continue
		}
		if time.Since(lease.lastUpdate) > self.leaseDurationSeconds {
			expired = append(expired, lease)
		} else {
			ownerLiveLeaseCount[lease.owner]++
		}
	}
	myId := self.manager.WorkerId()
	if _, found := ownerLiveLeaseCount[myId]; !found {
		ownerLiveLeaseCount[myId] = 0
	}
	return expired, ownerLiveLeaseCount
}

func (self *DynamoLeaseTaker) getLeasesToTake(expired []*Lease, ownerCount map[string]int) []*Lease {
	numWorkers := len(ownerCount)
	numActiveLeases := getNumActiveLeases(self.leaseMap)
	log.Print("Taker: Num available leases: ", numActiveLeases)
	numTargetLeases := int(math.Ceil(float64(numActiveLeases) / float64(numWorkers)))
	myId := self.manager.WorkerId()
	numCurrentLeases := ownerCount[myId]
	// Shuffle the expired leases so that not every worker goes after the same ones.
	shuffleIndex := rand.Perm(len(expired))
	toTake := []*Lease{}
	for i := 0; numCurrentLeases < numTargetLeases && i < len(expired); i++ {
		expiredLease := expired[shuffleIndex[i]]
		// If we already own the lease, don't try to take it.
		if expiredLease.owner != self.manager.WorkerId() {
			log.Printf("Take: Taking expired lease: %s", expired[shuffleIndex[i]].key)
			toTake = append(toTake, expired[shuffleIndex[i]])
			numCurrentLeases++
		}
	}
	need := numTargetLeases - numCurrentLeases
	if len(expired) == 0 && need > 0 {
		// Steal ONE if another worker has:
		// a) more than target leases and I need at least one.
		// b) exactly target leases and I need more than one.
		owner, count := getMostLoadedWorker(myId, ownerCount)
		if owner != "" && (count > numTargetLeases || (count == numTargetLeases && need > 1)) {
			// Steal one at random.
			toTake = append(toTake, getRandomFrom(owner, count, self.leaseMap))
			log.Printf("T: Stealing lease: %s", toTake[len(toTake)-1].key)
		}
	}
	return toTake
}

func getNumActiveLeases(leaseMap map[string]*Lease) int {
	num := 0
	for _, lease := range leaseMap {
		if !lease.ShardDone() {
			num++
		}
	}
	return num
}

func getRandomFrom(owner string, count int, leaseMap map[string]*Lease) *Lease {
	random := rand.Intn(count)
	for _, lease := range leaseMap {
		if lease.ShardDone() {
			continue
		}
		if lease.owner == owner {
			if random--; random <= 0 {
				return lease
			}
		}
	}
	panic("Error getting random entry from leaseMap")
}

func getMostLoadedWorker(workerId string, ownerCount map[string]int) (string, int) {
	// First find the most loaded worker.
	maxOwner := ""
	currentMax := 0
	for owner, count := range ownerCount {
		if count > currentMax && owner != workerId {
			currentMax = count
			maxOwner = owner
		}
	}
	return maxOwner, currentMax
}

//
//

func NewDynamoLeaseRenewer(config *Config) *DynamoLeaseRenewer {
	return &DynamoLeaseRenewer{
		manager:              NewDynamoLeaseManager(config),
		leaseDurationSeconds: time.Duration(config.LeaseDurationSeconds) * time.Second,
		owned:                make(map[string]*Lease),
	}
}

// DynamoLeaseRenewer is...
type DynamoLeaseRenewer struct {
	manager              LeaseManager
	leaseDurationSeconds time.Duration
	owned                map[string]*Lease
}

func (self *DynamoLeaseRenewer) Start(newLease <-chan Lease, checkpoint <-chan ShardCheckpoint) <-chan LeaseNotification {
	// Renewer runs at fixed INTERVAL because we want it to run at the same
	// rate in the event of degredation.
	renewTicker := time.NewTicker(self.leaseDurationSeconds / 3).C

	outChannel := make(chan LeaseNotification, 10)
	go func(notifications chan<- LeaseNotification) {
		defer close(notifications)
		for {
			select {
			case <-renewTicker:
				lostLeases := self.RenewCurrent()
				for _, lost := range lostLeases {
					notifications <- LeaseNotification{Lost: true, Lease: *lost}
				}
			case gained, ok := <-newLease:
				if !ok {
					log.Print("Renew: Channel closed. Exiting.")
					return
				}
				// The taker shouldn't be taking leases we already own, but just in case.
				if _, found := self.owned[gained.key]; !found {
					log.Printf("Renew: Gained new lease: %+v", gained.key)
					self.owned[gained.key] = &gained
					notifications <- LeaseNotification{Lost: false, Lease: gained}
				}
			case c := <-checkpoint:
				if lease, found := self.owned[c.ShardId]; found {
					if lease.checkpoint != c.SequenceNumber {
						log.Printf("Renew: Updating lease checkpoint for %s to %s", lease.key, c.SequenceNumber)
					}
					lease.checkpoint = c.SequenceNumber
					lease.lastUpdate = time.Now().UTC()
				}
			}
		}
	}(outChannel)
	return outChannel
}

func (self *DynamoLeaseRenewer) renew(lease *Lease) bool {
	for i := 0; i < NUM_RETRIES; i++ {
		err := self.manager.Renew(lease)
		if err != nil {
			log.Printf("Error renewing on attempt: %d. %s", i+1, err)
		} else {
			return true
		}
	}
	return false
}

func (self *DynamoLeaseRenewer) RenewCurrent() []*Lease {
	lost := []*Lease{}
	for key, lease := range self.owned {
		// If we haven't heard from the consumer in a while, it might be stuck
		// or dead. Don't renew it.
		// TODO: What if it's stuck? How do we kill it?
		// TODO: Make consumer timeout configurable.
		if time.Since(lease.lastUpdate) > (self.leaseDurationSeconds * 3) {
			continue
		}
		if self.renew(lease) {
			if lease.ShardDone() {
				// Now that we've saved it, we can let it expire.
				delete(self.owned, key)
			}
		} else {
			lost = append(lost, lease)
			log.Printf("Lost lease: %s", lease.key)
			delete(self.owned, key)
		}
	}
	if len(self.owned) > 0 {
		//log.Printf("Renew: Now own %d lease(s): %v", len(self.owned), self.owned)
	}
	return lost
}

func (self *DynamoLeaseRenewer) AddToRenew(toAdd []*Lease) {
	for _, newLease := range toAdd {
		self.owned[newLease.key] = newLease
	}
}
