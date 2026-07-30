# Architecture Overview

## Purpose

The AVEVA RocksDB Plugin provides filesystem and caching extensions so RocksDB can run in stateless cloud deployments backed by Azure Blob Storage.

## Components

1. `src/Azure/`
   - Azure Page Blob filesystem implementation.
   - Azure SDK integration, credential handling, and status translation.
2. `src/Core/`
   - Shared filesystem abstractions and utility logic.
   - Secondary cache, file cache, and related indexing helpers.
3. `include/AVEVA/RocksDB/Plugin/`
   - Public API surface for plugin consumers.
4. `tests/`
   - Unit and integration tests validating plugin behavior.

## Key Design Expectations

1. Public headers remain stable and avoid leaking implementation details.
2. Azure SDK failures are translated into meaningful RocksDB `Status` values.
3. Cache and filesystem components are thread-safe and deterministic.
4. Changes to Azure or cache behavior are covered by tests.
