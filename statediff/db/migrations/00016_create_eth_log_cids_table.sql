-- +goose Up
CREATE TABLE eth.log_cids (
    id                  SERIAL PRIMARY KEY,
    leaf_cid            TEXT NOT NULL,
    leaf_mh_key         TEXT NOT NULL REFERENCES public.blocks (key) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    receipt_id          INTEGER NOT NULL REFERENCES eth.receipt_cids (id) ON DELETE CASCADE DEFERRABLE INITIALLY DEFERRED,
    address             VARCHAR(66),
    log_data            BYTEA,
    index               INTEGER NOT NULL,
    topic0              VARCHAR(66),
    topic1              VARCHAR(66),
    topic2              VARCHAR(66),
    topic3              VARCHAR(66),
    UNIQUE (receipt_id, index)
);

CREATE INDEX log_mh_index ON eth.log_cids USING btree (leaf_mh_key);

CREATE INDEX log_cid_index ON  eth.log_cids USING btree (leaf_cid);

CREATE INDEX log_rct_id_index ON eth.log_cids USING btree (receipt_id);
--
-- Name: log_topic0_index; Type: INDEX; Schema: eth; Owner: -
--

CREATE INDEX log_topic0_index ON eth.log_cids USING btree (topic0);


--
-- Name: log_topic1_index; Type: INDEX; Schema: eth; Owner: -
--

CREATE INDEX log_topic1_index ON eth.log_cids USING btree (topic1);


--
-- Name: log_topic2_index; Type: INDEX; Schema: eth; Owner: -
--

CREATE INDEX log_topic2_index ON eth.log_cids USING btree (topic2);


--
-- Name: log_topic3_index; Type: INDEX; Schema: eth; Owner: -
--

CREATE INDEX log_topic3_index ON eth.log_cids USING btree (topic3);


-- +goose Down
-- log indexes
DROP INDEX eth.log_mh_index;
DROP INDEX eth.log_cid_index;
DROP INDEX eth.log_rct_id_index;
DROP INDEX eth.log_topic0_index;
DROP INDEX eth.log_topic1_index;
DROP INDEX eth.log_topic2_index;
DROP INDEX eth.log_topic3_index;

DROP TABLE eth.log_cids;
