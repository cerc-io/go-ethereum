package file

import (
	"testing"

	"github.com/jmoiron/sqlx"
)

// TearDownDB is used to tear down the watcher dbs after tests
func TearDownDB(t *testing.T, db *sqlx.DB) {
	tx, err := db.Begin()
	if err != nil {
		t.Fatal(err)
	}

	_, err = tx.Exec(`DELETE FROM eth.header_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.uncle_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.transaction_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.receipt_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.state_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.storage_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.state_accounts`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.access_list_elements`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth.log_cids`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM blocks`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM nodes`)
	if err != nil {
		t.Fatal(err)
	}
	_, err = tx.Exec(`DELETE FROM eth_meta.watched_addresses`)
	if err != nil {
		t.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		t.Fatal(err)
	}
}
