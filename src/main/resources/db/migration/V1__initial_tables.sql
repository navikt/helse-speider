CREATE TABLE melding
(
    id        BIGSERIAL PRIMARY KEY,
    data      JSON     NOT NULL,
    opprettet TIMESTAMP NOT NULL
);
