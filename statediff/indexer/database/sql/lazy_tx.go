package sql

import (
	"context"
	"github.com/ethereum/go-ethereum/log"
	"reflect"
)

type DelayedTx struct {
	cache []interface{}
	db    Database
}
type cachedStmt struct {
	sql  string
	args []interface{}
}

type copyFrom struct {
	tableName   []string
	columnNames []string
	rows        [][]interface{}
}

func NewDelayedTx(db Database) *DelayedTx {
	return &DelayedTx{db: db}
}

func (tx *DelayedTx) QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow {
	return tx.db.QueryRow(ctx, sql, args...)
}

func (tx *DelayedTx) CopyFrom(ctx context.Context, tableName []string, columnNames []string, rows [][]interface{}) (int64, error) {
	appendedToExisting := false
	if len(tx.cache) > 0 {
		prevCopy, ok := tx.cache[len(tx.cache)-1].(copyFrom)
		if ok && reflect.DeepEqual(prevCopy.tableName, tableName) && reflect.DeepEqual(prevCopy.columnNames, columnNames) {
			log.Info("statediff lazy_tx : Appending rows to COPY", "table", tableName,
				"current", len(prevCopy.rows), "append", len(rows))
			prevCopy.rows = append(prevCopy.rows, rows...)
			appendedToExisting = true
		}
	}

	if !appendedToExisting {
		tx.cache = append(tx.cache, copyFrom{tableName, columnNames, rows})
	}

	return 0, nil
}

func (tx *DelayedTx) Exec(ctx context.Context, sql string, args ...interface{}) (Result, error) {
	tx.cache = append(tx.cache, cachedStmt{sql, args})
	return nil, nil
}

func (tx *DelayedTx) Commit(ctx context.Context) error {

	base, err := tx.db.Begin(ctx)
	if err != nil {
		return err
	}
	defer func() {
		if p := recover(); p != nil {
			rollback(ctx, base)
			panic(p)
		} else if err != nil {
			rollback(ctx, base)
		}
	}()
	for _, item := range tx.cache {
		switch item.(type) {
		case copyFrom:
			copy := item.(copyFrom)
			log.Info("statediff lazy_tx : COPY", "table", copy.tableName, "rows", len(copy.rows))
			_, err := base.CopyFrom(ctx, copy.tableName, copy.columnNames, copy.rows)
			if err != nil {
				return err
			}
		case cachedStmt:
			stmt := item.(cachedStmt)
			_, err := base.Exec(ctx, stmt.sql, stmt.args...)
			if err != nil {
				return err
			}
		}
	}
	tx.cache = nil
	return base.Commit(ctx)
}

func (tx *DelayedTx) Rollback(ctx context.Context) error {
	tx.cache = nil
	return nil
}
