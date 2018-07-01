package rmdb


import (
	"github.com/juju/errors"
	"github.com/lysu/go-saga"
	"github.com/lysu/go-saga/storage"
	"sync"
	"github.com/jinzhu/gorm"
	"time"
)

var storageInstance storage.Storage
var rmdbInit sync.Once

func init() {
	saga.StorageProvider = func(cfg storage.StorageConfig) storage.Storage {
		rmdbInit.Do(func() {
			var err error
			storageInstance, err = newRmdbStorage(cfg)
			if err != nil {
				panic(err)
			}
		})
		return storageInstance
	}
}

type rmdbStorage struct {
	db *gorm.DB
}

type TXLog struct {
	gorm.Model
	LogID string  `gorm:"index:idx_logid"`
	Data string
	CreateTime time.Time
}

// newRmdbStorage creates log storage base on rmdb.
func newRmdbStorage(cfg storage.StorageConfig) (storage.Storage, error) {
	db, err := gorm.Open(cfg.RMDB.DBDialect, cfg.RMDB.DBUrl)
	if err != nil {
		return nil, err
	}
	// Migrate the schema
	db.AutoMigrate(&TXLog{})
	return &rmdbStorage{
		db: db,
	}, nil
}

// AppendLog appends log into queue under given logID.
func (s *rmdbStorage) AppendLog(logID string, data string) error {
	txlog := &TXLog{
		LogID: logID,
		Data: data,
		CreateTime: time.Now(),
	}
	if err := s.db.Create(txlog).Error; err != nil {
		return err
	}
	return nil
}

// Lookup lookups log under given logID.
func (s *rmdbStorage) Lookup(logID string) ([]string, error) {
	datas := make([]string, 0)
	rows, err := s.db.Table("tx_logs").Where("log_id = ?", logID).Select("data").Rows()
	defer rows.Close()
	if err != nil {
		return []string{}, err
	}
	for rows.Next() {
		var data string
		rows.Scan(&data)
		datas = append(datas, data)
	}
	return datas, nil
}

// Close uses to close storage and release resources.
func (s *rmdbStorage) Close() error {
	return s.db.Close()
}

// LogIDs uses to take all TXLog ID av in current storage
func (s *rmdbStorage) LogIDs() ([]string, error) {
	logIDs := make([]string, 0)
	rows, err := s.db.Table("tx_logs").Select("DISTRINCT(log_id)").Rows()
	defer rows.Close()
	if err != nil {
		return []string{}, err
	}
	for rows.Next() {
		var logID string
		rows.Scan(&logID)
		logIDs = append(logIDs, logID)
	}
	return logIDs, nil
}

func (s *rmdbStorage) Cleanup(logID string) error {
	if err := s.db.Where("log_id = ?", logID).Delete(TXLog{}).Error; err != nil {
		return err
	}
	return nil
}

func (s *rmdbStorage) LastLog(logID string) (string, error) {
	log := TXLog{}
	if err := s.db.Where("log_id = ?", logID).Order("create_time desc").First(&log).Error; err != nil {
		return "", err
	} else {
		if len(log.LogID) == 0 {
			return "", errors.Errorf("tx_logs %s not found", logID)
		}
		return log.Data, nil
	}
}
