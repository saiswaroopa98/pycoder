# Data Sync Ingestion Challenge

This project implements a Dockerized data ingestion pipeline that fetches events from the DataSync Analytics API and stores them in PostgreSQL.
The ingestion is paginated, resumable, and designed to handle large datasets reliably.

---

## Features
- Cursor-based pagination
- Resumable ingestion using stored cursor
- PostgreSQL persistence
- Docker & Docker Compose setup
- Idempotent inserts
- Optional export of event IDs

---

## Prerequisites
- Docker & Docker Compose
- Node.js 20+
- A valid DataSync API key 
