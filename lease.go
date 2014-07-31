package gokc

import (
	"time"
)

// Lease is a representation of a processes temporary "ownership" of a shard by a
// worker, which can be used to partition work across a fleet of workers. Each
// unit of work (identified by its key) has a corresponding lease. Every worker
// will contend for all leases - only one worker will successfully take each
// one. The worker should hold the lease until it is ready to stop processing
// the corresponding unit of work, or until it fails. When the worker stops
// holding the lease, another worker will take and hold the lease.
type Lease struct {
	// Key uniquely identifies a lease. For dynamo, it is the shard id.
	key string
	// Owner is the worker id of the current owner.
	owner string
	// The last processed sequence number of the shard.
	checkpoint string
	// A value incremented on every update. Provides heartbeat and consistency
	// check.
	counter int64
	// lastCounterIncrementTime isn't stored in the dynamo table, but is used by
	// lease takers/renewers to test lease staleness.
	lastUpdate time.Time
	// TODO(Cameron): need to figure out concurrency token stuff.

}

// The checkpoint value indicating that we've completely processed all
// records in this shard.
const SHARD_END = "SHARD_END"

func (lease Lease) ShardDone() bool {
	return lease.checkpoint == SHARD_END
}

// LeaseNotification carries information about whether a lease has been gained
// (Lost == false) or lost (Lost == true).
type LeaseNotification struct {
	Lost  bool
	Lease Lease
}

// LeaseTaker is  are greedy lease-loving things. Implementers of the interface
// are meant to be called periodically to see if there are leases that this
// worker can assume ownership of.
type LeaseTaker interface {
	// Take computes the set of leases available to be taken... and takes them.
	// Lease taking rules are:
	// 1) If a lease's counter hasn't changed in long enough, try to take it.
	// 2) If we see a lease we've never seen before, take it only if
	//    owner == null. If it's owned, odds are the owner is holding it. We
	//    can't tell until we see it more than once.
	// 3) For load balancing purposes, you may violate rules 1 and 2 for ONE
	//   lease per call to Take()
	// Returns a slice of the leases successfully taken.

	// TODO: Why even have a Take() method? Why not just have the lease taker
	// decide how often to poll, based on the lease time, and just communicate
	// via the channel?
	Take() []*Lease
	// Start
	Start(<-chan Shard) <-chan Lease
}

// LeaseRenewer is a mother duckling, making sure that her family crosses the
// street safely. It should be called periodically to renew the currently-owned
// leases.
type LeaseRenewer interface {
	// Attempt to renew all active leases.
	RenewCurrent() []*Lease
	AddToRenew([]*Lease)
	Start(<-chan Lease, <-chan ShardCheckpoint) <-chan LeaseNotification
}

// LeaseManager is the slightly-higher-level-than-crud interface for lease
// objects.
type LeaseManager interface {
	Create(*Lease) error
	// List synchronously retrieves all leases.
	List() []*Lease
	// Get synchronously retrieves the least for the specified shardId.
	//Get(shardId string) Lease
	// Renew a lease by incrementing the lease counter. Conditional on the
	// lease counter in DynamoDB matching the leaseCounter of the input. Mutates
	// the counter of the passed-in lease object after updating the record in
	// DynamoDB.
	Renew(lease *Lease) error
	// Take a lease for the given owner by incrementing its leaseCounter and
	// setting its owner field. Conditional on the leaseCounter in DynamoDB
	// matching the leaseCounter of the input. Mutates the leaseCounter and
	// owner of the passed-in lease object after updating DynamoDb.
	Take(lease *Lease) error
	// Delete the given lease. Does nothing if lease does not exist.
	//Delete(lease Lease)
	// TableName returns the name of the table where leases are stored.
	TableName() string
	// WorkerId returns the string id of the current worker (set at app-level).
	WorkerId() string
}

/*func NewLeaseCoodinator(manager LeaseManager) *LeaseCoordinator {
	return &LeaseCoordinator{
	//taker:   NewDynamoLeaseTaker(manager),
	//renewer: NewDynamoLeaseRenewer(manager),
	}
}

// LeaseCoordinator abstracts away LeaseTaker and LeaseRenewer from the
// application code, owns the scheduling of lease taking/renewing, and tells
// the renewer about the leases taken by the taker.
type LeaseCoordinator struct {
	Id      string
	Done    bool
	taker   LeaseTaker
	renewer LeaseRenewer
}

func (self *LeaseCoordinator) run(leaseTimeSeconds time.Duration) {
	takeInterval := leaseTimeSeconds * 2
	renewInterval := leaseTimeSeconds / 3
	// Taker runs with fixed DELAY because we want it to run slower
	// in the event of performance degredation. The first timer fires
	// immediately for aggressive load-balacing purposes.
	takeChannel := time.NewTimer(500 * time.Millisecond).C
	// Renewer runs at fixed INTERVAL because we want it to run at the same
	// rate in the event of degredation.
	renewChannel := time.NewTicker(renewInterval).C

	for !self.Done {
		select {
		case <-takeChannel:
			self.renewer.AddToRenew(self.taker.Take())
			// Schedule the next iteration!
			leaseTimeSeconds * 2
			takeChannel = time.NewTimer(takeInterval).C
		case <-renewChannel:
			self.renewer.RenewAll()
		}
	}
	log.Println("Lease coordinator quitting.")
}
*/
