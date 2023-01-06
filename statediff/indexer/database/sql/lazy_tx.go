package sql

import (
	"context"
)

type DelayedTx struct {
	cache []cachedStmt
	db    Database
}
type cachedStmt struct {
	sql  string
	args []interface{}
}

func NewDelayedTx(db Database) *DelayedTx {
	return &DelayedTx{db: db}
}

func (tx *DelayedTx) QueryRow(ctx context.Context, sql string, args ...interface{}) ScannableRow {
	return tx.db.QueryRow(ctx, sql, args...)
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
	for _, stmt := range tx.cache {
		_, err := base.Exec(ctx, stmt.sql, stmt.args...)
		if err != nil {
			return err
		}
	}
	tx.cache = nil
	return base.Commit(ctx)
}

func (tx *DelayedTx) Rollback(ctx context.Context) error {
	tx.cache = nil
	return nil
}
