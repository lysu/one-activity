package storage

import "time"

// Storage uses to support save and lookup saga log.
type Storage interface {

	// AppendLog appends log data into log under given logID
	AppendLog(logID string, data string) error

	// Lookup uses to lookup all log under given logID
	Lookup(logID string) ([]string, error)

	// Close use to close storage and release resources
	Close() error

	// LogIDs returns exists logID
	LogIDs() ([]string, error)

	// Cleanup cleans up all log data in logID
	Cleanup(logID string) error

	// LastLog fetch last log entry with given logID
	LastLog(logID string) (string, error)
}

type StorageProvider func(cfg StorageConfig) Storage

type StorageConfig struct {
	Kafka struct {
		ZkAddrs, BrokerAddrs []string
		Partitions, Replicas int
		ReturnDuration       time.Duration
	}
	RMDB struct {
		DBDialect, DBUrl string
	}
}
