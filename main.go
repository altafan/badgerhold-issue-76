package main

import (
	"fmt"
	"time"

	"github.com/google/uuid"
	log "github.com/sirupsen/logrus"

	"github.com/dgraph-io/badger/v2"
	"github.com/dgraph-io/badger/v2/options"
	"github.com/timshannon/badgerhold/v2"
)

type Key struct {
	TxID string
	VOut uint32
}

type Unspent struct {
	TxID            string
	VOut            uint32
	Value           uint64
	AssetHash       string
	ValueCommitment string
	AssetCommitment string
	ValueBlinder    []byte
	AssetBlinder    []byte
	ScriptPubKey    []byte
	Nonce           []byte
	RangeProof      []byte
	SurjectionProof []byte
	Address         string
	Spent           bool
	Locked          bool
	LockedBy        *uuid.UUID
	Confirmed       bool
}

var (
	dbDir = "unspents"
	key   = Key{
		TxID: "dee3099862699e2055ed6a92b727a762ab921c196e23fa520e81d50fab4b932c",
		VOut: 8,
	}
)

func main() {
	logger := log.New()
	logger.Level = log.DebugLevel

	db, err := createDb(dbDir, logger)
	if err != nil {
		log.Fatal("error while opening db:", err)
	}
	defer closeDb(db)

	elem, err := find(db, key)
	if err != nil {
		log.Fatal("error while finding elem:", err)
	}
	fmt.Println("-----------------------------")
	fmt.Println("found with Find:", elem != nil)
	fmt.Println("-----------------------------")

	elem, err = get(db, key)
	if err != nil {
		log.Fatal("error while getting elem:", err)
	}
	fmt.Println("found with Get:", elem != nil)
	fmt.Println("-----------------------------")
}

// Create/open the DB
func createDb(dbDir string, logger badger.Logger) (*badgerhold.Store, error) {
	opts := badger.DefaultOptions(dbDir)
	opts.Logger = logger
	opts.ValueLogLoadingMode = options.FileIO
	opts.Compression = options.ZSTD

	db, err := badgerhold.Open(badgerhold.Options{
		Encoder:          badgerhold.DefaultEncode,
		Decoder:          badgerhold.DefaultDecode,
		SequenceBandwith: 100,
		Options:          opts,
	})
	if err != nil {
		return nil, err
	}

	ticker := time.NewTicker(30 * time.Minute)
	go func() {
		for {
			<-ticker.C
			if err := db.Badger().RunValueLogGC(0.5); err != nil && err != badger.ErrNoRewrite {
				log.Println(err)
			}
		}
	}()

	return db, nil
}

// Close the DB
func closeDb(db *badgerhold.Store) error {
	return db.Close()
}

func get(db *badgerhold.Store, key Key) (*Unspent, error) {
	var elem Unspent
	if err := db.Get(key, &elem); err != nil {
		if err == badgerhold.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	return &elem, nil
}

func find(db *badgerhold.Store, key Key) (*Unspent, error) {
	query := badgerhold.Where("TxID").Eq(key.TxID).And("VOut").Eq(key.VOut)
	var elems []Unspent
	if err := db.Find(&elems, query); err != nil {
		if err == badgerhold.ErrNotFound {
			return nil, nil
		}
		return nil, err
	}
	if elems == nil {
		return nil, nil
	}
	elem := elems[0]
	return &elem, nil
}
