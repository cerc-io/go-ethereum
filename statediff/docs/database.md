# Overview

This document will go through some notes on the database component of the statediff service.

# Components

- Indexer: The indexer creates IPLD and DB models to insert to the Postgres DB. It performs the insert utilizing and atomic function.
- Builder: The builder constructs the statediff object that needs to be inserted.
- Known Gaps: Captures any gaps that might have occured and either writes them to the DB, local sql file, to prometeus, or a local error.

# Making Code Changes

## Adding a New Function to the Indexer

If you want to implement a new feature for adding data to the database. Keep the following in mind:

1. You need to handle `sql`, `file`, and `dump`.
   1. `sql` - Contains the code needed to write directly to the `sql` db.
   2. `file` - Contains all the code required to write the SQL statements to a file.
   3. `dump` - Contains all the code for outputting events to the console.
2. You will have to add it to the `interfaces.StateDiffIndexer` interface.
