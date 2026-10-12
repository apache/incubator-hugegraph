# Store legacy GraphID preflight

Some older Store writes can have the reserved `FFFE` graph prefix without a
persisted graph-to-ID mapping. Allocating a new graph ID does not rewrite those
rows, so their physical presence does not imply that normal graph reads can see
them. This condition is specific to affected old write paths; it does not mean
that every 1.7 database needs migration.

Store checks existing graph-data column families before allocating a missing
graph ID, starting a partition's Raft state machine, or activating a snapshot.
If it finds the reserved prefix, it rejects that operation and reports the
partition and column family. Metadata-only column families are excluded. The
checks do not assign an owner to legacy rows or change the persisted ID format.

The partition preflight resolves existing DB versions without creating a DB,
allocating a store location or pruning older versions. It runs before partition
task/FSM construction and Raft replay; other existing Store bootstrap steps can
already have opened databases. It is not a promise that the entire startup
process leaves every file untouched.

A rejected snapshot retains the active DB and its index, the incoming snapshot,
and the staged copy for diagnosis. Successful snapshots still use the existing
activation path. An incomplete selected DB version with an older valid DB is
rejected rather than silently skipping its preflight.

After rejection, preserve the original database, metadata and snapshot evidence
and investigate the affected write/upgrade history before restarting writes or
replay. Do not delete mappings or assign all reserved-prefix rows to one graph:
different graphs may share that prefix. This change provides detection and
refusal, not automatic migration, repair or proof of a complete 1.7 upgrade.
