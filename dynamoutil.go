package gokc

// RENAME TO: dynamo/util.go or dynamo/client.go

import (
	"encoding/json"
	"errors"
	"fmt"
	e "github.com/smugmug/godynamo/endpoint"
	create "github.com/smugmug/godynamo/endpoints/create_table"
	describe "github.com/smugmug/godynamo/endpoints/describe_table"
	put_item "github.com/smugmug/godynamo/endpoints/put_item"
	scan "github.com/smugmug/godynamo/endpoints/scan"
	update_item "github.com/smugmug/godynamo/endpoints/update_item"
	"log"
	"strconv"
	"strings"
	"time"
)

func CreateTable(name string, readCapacity, writeCapacity uint64) error {
	if active, err := describe.IsTableStatus(name, describe.ACTIVE); err == nil && active {
		// Don't bother creating the table if it already exists.
		log.Printf("Table already exists: %s", name)
		return nil
	}
	c := create.NewCreate()
	c.TableName = name
	c.AttributeDefinitions = append(c.AttributeDefinitions, e.AttributeDefinition{"leaseKey", e.S})
	c.KeySchema = append(c.KeySchema, e.KeyDefinition{"leaseKey", e.HASH})
	c.ProvisionedThroughput = e.ProvisionedThroughput{
		readCapacity, writeCapacity}
	c.LocalSecondaryIndexes = nil

	body, code, err := c.EndpointReq()
	if code == 400 && strings.Contains(body, "#ResourceInUseException") {
		// This table already existed. Fine, but how was this not caught above?
		log.Printf("Table already exists?: %s", name)
		return nil
	}
	if code == 200 {
		return nil
	}
	if err != nil {
		return err
	}
	return errors.New(body)
}

func WaitUntilTableExists(name string, pollIntervalSeconds, timeoutSeconds time.Duration) error {
	start := time.Now()
	for {
		if active, err := describe.IsTableStatus(name, describe.ACTIVE); err == nil && active {
			return nil
		}
		if time.Since(start) > (timeoutSeconds) {
			return errors.New(fmt.Sprintf("Error waiting for table (%s) to exist. Timed out after %d seconds.", name, timeoutSeconds/time.Second))
		}
		log.Printf("Table not active. Sleeping for %s seconds.", pollIntervalSeconds)
		time.Sleep(pollIntervalSeconds)
	}
}

func Scan(name string, limit int) ([]*Lease, error) {
	request := scan.NewScan()
	request.TableName = name
	request.Limit = e.NullableUInt64(limit)
	result := []*Lease{}
	for request != nil {
		body, code, err := request.EndpointReq()
		if err != nil || code != 200 {
			log.Print("Problem in Scan!")
			return nil, err
		}
		response := &scan.Response{}
		if err = json.Unmarshal([]byte(body), response); err != nil {
			return nil, err
		}
		for _, item := range response.Items {
			lease := leaseFromDynamoItem(item)
			result = append(result, lease)
		}
		if response.LastEvaluatedKey == nil {
			request = nil
		} else {
			request.ExclusiveStartKey = response.LastEvaluatedKey
		}
	}
	return result, nil
}

func PutLease(tableName string, lease *Lease) error {
	request := put_item.NewPut()
	request.TableName = tableName
	request.Item = leaseToDynamoItem(lease)
	request.Expected["leaseKey"] = e.Constraints{Exists: false}
	body, code, err := request.EndpointReq()
	if err != nil {
		return err
	}
	if code != 200 {
		return errors.New(fmt.Sprintf("Unable to create lease. Response body: %s", body))
	}
	return nil
}

func UpdateLease(tableName string, lease *Lease, workerId string) error {
	request := getRequestFromLease(tableName, *lease, workerId)

	body, code, err := request.EndpointReq()
	if err != nil {
		return err
	}
	if code != 200 {
		return errors.New(fmt.Sprintf("Unable to renew lease. Response body: %s", body))
	}
	// Persist locally the remote changes we just made. This is a little muddy,
	// as the manager technically owns these objects, but nice to do here
	// because it's handled for both Take() and Renew(). Should fix.
	if workerId != lease.owner {
		lease.owner = workerId
		lease.counter = 0
	} else {
		lease.counter++
	}
	return nil
}

func getRequestFromLease(tableName string, lease Lease, workerId string) *update_item.Update {
	request := update_item.NewUpdate()
	request.TableName = tableName
	request.Key["leaseKey"] = e.AttributeValue{S: lease.key}
	// Set the new values.
	if lease.checkpoint != "" {
		request.AttributeUpdates["leaseCheckpoint"] = update_item.AttributeAction{Value: e.AttributeValue{S: lease.checkpoint}, Action: update_item.ACTION_PUT}
	}
	if workerId != lease.owner {
		request.AttributeUpdates["leaseOwner"] = update_item.AttributeAction{Value: e.AttributeValue{S: workerId}, Action: update_item.ACTION_PUT}
		// Reset the counter.
		request.AttributeUpdates["leaseCounter"] = update_item.AttributeAction{Value: e.AttributeValue{N: "0"}, Action: update_item.ACTION_PUT}
	} else {
		// Just increment the counter.
		request.AttributeUpdates["leaseCounter"] = update_item.AttributeAction{Value: e.AttributeValue{N: strconv.FormatInt(lease.counter+1, 10)}, Action: update_item.ACTION_PUT}
	}
	// Expect the old values to still be set at update time.
	request.Expected["leaseOwner"] = e.Constraints{Value: e.AttributeValue{S: lease.owner}}
	request.Expected["leaseCounter"] = e.Constraints{Value: e.AttributeValue{N: strconv.FormatInt(lease.counter, 10)}}
	return request
}

func leaseFromDynamoItem(item e.Item) *Lease {
	result := Lease{}
	result.key = item["leaseKey"].S
	result.owner = getItemString(item, "leaseOwner")
	result.checkpoint = getItemString(item, "leaseCheckpoint")
	counter := getItemLong(item, "leaseCounter")
	if counter == nil {
		log.Print("No counter value found")
		return nil
	}
	result.counter = *counter
	return &result
}

func getItemString(item e.Item, key string) string {
	attribute, found := item[key]
	if found {
		return attribute.S
	}
	return ""
}

func getItemLong(item e.Item, key string) *int64 {
	attribute, found := item[key]
	if found {
		val, err := strconv.ParseInt(attribute.N, 10, 64)
		if err != nil {
			log.Print(err)
			return nil
		}
		return &val
	}
	return nil
}

func leaseToDynamoItem(lease *Lease) e.Item {
	result := make(e.Item)
	result["leaseKey"] = e.AttributeValue{S: lease.key}
	result["leaseOwner"] = e.AttributeValue{S: lease.owner}
	if lease.checkpoint != "" {
		result["leaseCheckpoint"] = e.AttributeValue{S: lease.checkpoint}
	}
	result["leaseCounter"] = e.AttributeValue{N: strconv.FormatInt(lease.counter, 10)}
	return result
}
