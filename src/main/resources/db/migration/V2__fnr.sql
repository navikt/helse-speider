ALTER TABLE melding ADD COLUMN fnr VARCHAR(32) NOT NULL DEFAULT '';
CREATE INDEX melding_opprettet_idx ON melding USING BTREE(opprettet);
DELETE FROM melding WHERE fnr='';
ALTER TABLE melding ALTER COLUMN fnr DROP DEFAULT;

