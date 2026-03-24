# Migration App

A Java tool to migrate schema and data between Oracle and PostgreSQL.

Current capabilities:
- Oracle -> PostgreSQL migration
- PostgreSQL -> Oracle migration
- 3-phase flow: create tables, transfer data, add foreign keys
- Optional metadata extraction test output (json/text)
- Retry and Resume for transient migration failures
- Swing UI mode for interactive migration

## 1. Requirements

- Java 17+
- Maven 3.9+
- Docker + Docker Compose (optional but recommended for local test)

## 2. Quick start with Docker

Start local databases:

```bash
docker compose up -d
```

Default containers from docker-compose:
- Oracle: migration-oracle (port 1521)
- PostgreSQL: migration-postgres (port 5432)

Stop containers:

```bash
docker compose down
```

## 3. Environment variables

Use values from .env.example as baseline.

Core connection settings:
- ORACLE_HOST (default: localhost)
- ORACLE_PORT (default: 1521)
- ORACLE_SERVICE (default: ORCL)
- ORACLE_USER (default: scott)
- ORACLE_USER_PASSWORD (default: tiger)
- POSTGRES_HOST (default: localhost)
- POSTGRES_PORT (default: 5432)
- POSTGRES_DB (default: migration_db)
- POSTGRES_USER (default: postgres)
- POSTGRES_PASSWORD (default: admin123)

Migration flow settings:
- MIGRATION_DIRECTION: ORACLE_TO_POSTGRES or POSTGRES_TO_ORACLE
- APP_TIMEZONE (recommended: UTC)
- MIGRATION_BATCH_SIZE (default: 1000)
- MIGRATION_MAX_TABLES (default: 0, means all tables)
- MIGRATION_INCLUDE_TABLES (optional CSV, example: users,orders)
- MIGRATION_EXCLUDE_TABLES (optional CSV, example: audit_log,temp_table)
- MIGRATION_SKIP_EXISTING_TABLES (default: true)
- MIGRATION_SKIP_EXISTING_FKS (default: true)
- MIGRATION_RECREATE_EXISTING_TABLES (default: false)
- SOURCE_SCHEMA (direction-specific source schema)
- TARGET_SCHEMA (direction-specific target schema)

Metadata test settings:
- ENABLE_METADATA_TEST (default: false)
- METADATA_OUTPUT_FORMAT: json or text
- METADATA_MAX_TABLES (default: 5)

## 4. Retry and Resume

New Retry/Resume settings:
- MIGRATION_ENABLE_RETRY (default: true)
- MIGRATION_MAX_ATTEMPTS (default: 3)
- MIGRATION_RETRY_DELAY_MS (default: 2000)
- MIGRATION_RETRY_BACKOFF_MULTIPLIER (default: 2.0)
- MIGRATION_ENABLE_RESUME (default: true)
- MIGRATION_RESUME_STATE_FILE (default: .migration-resume.properties)
- MIGRATION_RESET_RESUME_STATE (default: false)

How it works:
- Retry is applied at two layers:
	- batch-level in DataTransferService for executeBatch() (exponential backoff)
	- table-level in migration flow for transient reconnect/restart cases
- On transient errors (connection/socket/timeout/deadlock-like SQLState), migration retries with backoff.
- Resume stores per-table state with:
	- offset: last committed row count
	- done flag: table completed
- CLI and UI can both resume from offset and skip done tables.

Important notes:
- Offset resume requires deterministic order, so the app uses ORDER BY primary key when PK exists.
- With no primary key, offset resume is not safe and the app falls back to starting table from beginning.
- If MIGRATION_RECREATE_EXISTING_TABLES=true, existing target tables are dropped and resume state is cleared.

## 5. Run the app

### 5.1 Run CLI migration

Build:

```bash
mvn clean package
```

Run:

```bash
mvn -Dexec.mainClass=org.example.MigrationApp exec:java
```

Run with explicit direction:

```bash
mvn -Dexec.mainClass=org.example.MigrationApp -Dexec.args="--direction=POSTGRES_TO_ORACLE" exec:java
```

Optional metadata override flags:
- --enable-metadata-test
- --disable-metadata-test
- --metadata-test=true|false

### 5.2 Run Swing UI

```bash
mvn -Dexec.mainClass=org.example.MigrationAppUI exec:java
```

In UI:
- Configure source and target connections
- Choose mode: full copy / structure only / data only
- Configure truncate/copy-new-only/limit options
- Optional table filter:
	- include CSV: only migrate listed tables
	- exclude CSV: migrate all except listed tables
- Start migration and monitor progress/logs

## 6. Suggested first run profile

For safer first run in local environment:
- APP_TIMEZONE=UTC
- MIGRATION_DIRECTION=ORACLE_TO_POSTGRES
- MIGRATION_ENABLE_RETRY=true
- MIGRATION_MAX_ATTEMPTS=4
- MIGRATION_RETRY_DELAY_MS=2000
- MIGRATION_RETRY_BACKOFF_MULTIPLIER=2
- MIGRATION_ENABLE_RESUME=true

## 7. Troubleshooting

1. Cannot connect to DB
- Verify container health and exposed ports
- Check ORACLE_SERVICE and credentials
- Keep APP_TIMEZONE=UTC to avoid PostgreSQL timezone alias issues

2. Migration stops due to transient DB/network issue
- Keep retry enabled
- Increase MIGRATION_MAX_ATTEMPTS and MIGRATION_RETRY_DELAY_MS

3. Rerun reprocesses too much data
- Enable resume
- Check checkpoint file path from MIGRATION_RESUME_STATE_FILE
- Ensure target tables have primary keys for best duplicate-safe behavior

4. Need a clean rerun
- Set MIGRATION_RESET_RESUME_STATE=true, or remove checkpoint file manually
