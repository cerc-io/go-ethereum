-- +goose Up
CREATE TABLE eth.receipt_cids (
  id                    SERIAL PRIMARY KEY,
  tx_id                 INTEGER NOT NULL REFERENCES eth.transaction_cids (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  cid                   TEXT NOT NULL,
  mh_key                TEXT NOT NULL REFERENCES public.blocks (key) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
  contract              VARCHAR(66),
  contract_hash         VARCHAR(66),
  post_state            VARCHAR(66),
  post_status           INTEGER,
  log_root              VARCHAR(66),
  UNIQUE (tx_id)
);

-- +goose Down
DROP TABLE eth.receipt_cids;
