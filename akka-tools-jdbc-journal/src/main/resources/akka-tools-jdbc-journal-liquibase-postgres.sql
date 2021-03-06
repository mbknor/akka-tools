--changeset mokj:Create-akka-tools-jdbc-journal-tables dbms:all
CREATE TABLE t_journal (
  typePath                                VARCHAR(255),
  id                                      VARCHAR(255),
  sequenceNr                              INT,
  journalIndex                            INT,
  persistentRepr                          BYTEA,
  payload_write_only                      TEXT,
  updated                                 TIMESTAMP,

  PRIMARY KEY(typePath, id, sequenceNr)
);

CREATE SEQUENCE s_journalIndex_seq START WITH 1;

-- Create index to make it fast to query using only typePath and s_journal_global_seq
CREATE UNIQUE INDEX IX_journalIndex ON t_journal(typePath, journalIndex);


CREATE TABLE t_snapshot (
  persistenceId                           VARCHAR(255),
  sequenceNr                              INT,
  timestamp                               NUMERIC,
  snapshot                                BYTEA,
  snapshotClassname                       VARCHAR(255),
  updated                                 TIMESTAMP,
  serializerId                            INT,

  PRIMARY KEY(persistenceId, sequenceNr, timestamp)
);

CREATE TABLE t_cluster_nodes (
    nodeName                              VARCHAR(255),
    lastSeen                              TIMESTAMP,
    joined                                INT,
    PRIMARY KEY(nodeName)
);
