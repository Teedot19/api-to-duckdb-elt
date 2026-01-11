# api-to-duckdb-elt

Opinionated ELT pipeline for ingesting external API data into DuckDB with schema
validation, idempotent loads, and orchestration.

## Table of contents

- [Overview](#overview)
- [Pipeline flow](#pipeline-flow)
- [Key characteristics](#key-characteristics)
- [Storage design](#storage-design)
- [Design decisions](#design-decisions)
- [Non-goals](#non-goals)

## Overview

This repository contains a minimal, production-style ELT pipeline that moves data
from a third-party REST API into DuckDB with strong correctness guarantees and
low operational overhead. The emphasis is on pipeline design and reliability —
not domain-specific analytics.

## Pipeline flow

1. REST API
2. Runtime schema validation (dataclasses)
3. Type normalization (pandas)
4. Idempotent load (DuckDB)
5. Orchestration (Dagster)
6. Optional BI consumption (Google Sheets / Looker Studio)

## Key characteristics

- Daily batch ingestion (low volume, relaxed latency)
- Runtime schema validation at the ingestion boundary
- Explicit type normalization and lightweight transformations
- Idempotent loading to support safe re-runs and backfills
- Asset-based orchestration with Dagster
- DuckDB as a zero-infra analytical store

## Storage design

Data is organised into logical layers inside DuckDB:

- **Raw** — data closely aligned to the source representation
- **Processed** — validated and type-normalised tables
- **Reporting** — tables structured for downstream consumption

These layers are logical separations for clarity and operational practices,
not enforced by tooling.

## Design decisions

- Prefer batch ELT over streaming to minimise system complexity
- Validate at the ingestion boundary to prevent invalid data from entering
	storage
- Use DuckDB for analytical performance without operational overhead
- Use Dagster for explicit dependencies and safe re-execution

## Non-goals

- Complex analytics or domain-specific insight generation
- Real-time or high-throughput ingestion
- Replacing warehouse-managed transformation frameworks

