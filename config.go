package gokc

import (
	"encoding/json"
	"fmt"
	conf "github.com/smugmug/godynamo/conf"
	"io/ioutil"
	"log"
	"net"
	"net/url"
	"os"
	"time"
)

type AwsAuth struct {
	AccessKey string
	SecretKey string
}

type LeaseTableConfig struct {
	Auth                      AwsAuth
	Create                    bool
	Name                      string
	ReadCapacity              uint64
	WriteCapacity             uint64
	CreateTimeoutSeconds      int
	CreatePollIntervalSeconds int
}

type KinesisConfig struct {
	Auth                      AwsAuth
	Region                    string
	StreamName                string
	MaxRecordsPerFetch        int
	GetRecordsIntervalSeconds int
	ShardSyncIntervalSeconds  int
}

type Config struct {
	AppName               string
	LeaseTable            LeaseTableConfig
	LeaseDurationSeconds  int
	Kinesis               KinesisConfig
	NumProcessorsPerShard int
	WorkerId              string
}

var _config = Config{
	AppName: "gokc", // You should DEFINITELY set this.

	Kinesis: KinesisConfig{
		Auth:                      AwsAuth{},
		StreamName:                "entities",
		MaxRecordsPerFetch:        10000,
		GetRecordsIntervalSeconds: 1,
		ShardSyncIntervalSeconds:  60,
	},

	LeaseDurationSeconds: 30,

	LeaseTable: LeaseTableConfig{
		Auth:                      AwsAuth{},
		Create:                    true,
		Name:                      "", // This is set automatically.
		ReadCapacity:              2,
		WriteCapacity:             2,
		CreateTimeoutSeconds:      60,
		CreatePollIntervalSeconds: 10,
	},

	NumProcessorsPerShard: 2,

	WorkerId: "", // This will be set automatically.
}

func getDynamoUrl(dConf conf.AWS_Conf) *url.URL {
	host := conf.Vals.Network.DynamoDB.Host
	scheme := conf.Vals.Network.DynamoDB.Scheme
	port := conf.Vals.Network.DynamoDB.Port
	url, err := url.Parse(scheme + "://" + host + ":" + port)
	if err != nil {
		panic("confload.init: read err: conf.Vals.Network.DynamoDB.URL malformed")
	}
	return url
}

func (config *Config) Init() {
	if config.LeaseTable.Name == "" {
		config.LeaseTable.Name = getTableName(config.Kinesis.StreamName, config.AppName)
	}
	if config.WorkerId == "" {
		config.WorkerId = getWorkerId(config.AppName)
	}
	InitDynamoConfig(config)
}

func getWorkerId(appName string) string {
	name, err := os.Hostname()
	var addrs []string
	if err == nil {
		addrs, err = net.LookupHost(name)
	}
	if err != nil {
		panic(err)
	}
	pid := os.Getpid()
	return fmt.Sprintf("%s::%s::%d::%d", appName, addrs[0], pid, time.Now().Unix())
}

func getTableName(streamName, appName string) string {
	return fmt.Sprintf("k-%s-%s", streamName, appName)
}

func InitDynamoConfig(config *Config) {
	conf.Vals.ConfLock.Lock()
	conf.Vals.Auth.AccessKey = config.LeaseTable.Auth.AccessKey
	conf.Vals.Auth.Secret = config.LeaseTable.Auth.SecretKey
	// Dynamo

	// TODO(Cameron): Generate from lease table config, or switch to a library that makes connection
	// params easier to specify.
	conf.Vals.Network.DynamoDB.Host = "dynamodb.us-west-2.amazonaws.com"
	conf.Vals.Network.DynamoDB.Zone = "us-west-2"
	conf.Vals.Network.DynamoDB.Scheme = "http"
	conf.Vals.Network.DynamoDB.Port = "80"
	conf.Vals.Network.DynamoDB.URL = "http://dynamodb.us-west-2.amazonaws.com:80"
	conf.Vals.Network.DynamoDB.KeepAlive = true

	conf.Vals.ConfLock.Unlock()

}

func MustParseConfig(path string) Config {
	data, err := ioutil.ReadFile(path)
	if err != nil {
		log.Fatalf("Error reading config file: %s. %s", path, err)
	}
	config := Config{}
	err = json.Unmarshal(data, &config)
	if err != nil {
		log.Fatalf("Error parsing config file: %s. %s", path, err)
	}
	return config
}
