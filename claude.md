# Codebase Context Guide

## System Overview

This is a WhatsApp ETL system that extracts data from WhatsApp Web using Selenium, transforms it for two business domains (students and sales), and loads results into MongoDB and Google Sheets.

**Core Purpose:** Automate tracking of student practice submissions and sales leads from WhatsApp group chats.

**Architecture:** Dual-pipeline ETL with parallel student and sales flows.

**Execution Model:** Runs in infinite loop inside Docker container (2-hour intervals).

## Project Structure

```
mk2/
├── src/
│   ├── etl/
│   │   ├── db/mongodb/          # MongoDB singleton connection and handlers
│   │   ├── students_etl/        # Student pipeline (transform, load_mongo_stats, load_sheets_updates)
│   │   ├── sales_etl/           # Sales pipeline (transform, load)
│   │   ├── etl.py              # Orchestrator - calls both pipelines
│   │   └── extract.py          # Selenium WhatsApp scraper
│   └── sheets_connect.py        # Google Sheets API wrapper
├── main.py                      # Entry point
├── whatsapp_session/           # Selenium persistent session (mount as volume)
├── secrets/                    # Google API credentials (mount as volume)
└── .env                        # Runtime configuration
```

**Organization Principle:** Code is grouped by business domain (students vs sales), not by technical layer.

## Technology Stack

- **Python 3.x** with Selenium WebDriver (Chrome)
- **MongoDB** (pymongo 4.15.4) - 3 databases: `students_db`, `sales_db`, `logger_db`
- **Google Sheets API** (gspread 6.2.1) - data source and output destination
- **Docker** with `selenium/standalone-chrome:latest` base image
- **Testing:** pytest available but no active test suite

## Critical Data Models

### MongoDB Collections

**students_db.student_stats:**
```javascript
{
  uniq_id: "MD5(phone_number_name)",  // Unique index
  phone_number: "972 55-660-2298",
  name: "Student Name",
  current_lesson: "7",
  total_messages: 12,  // DEPRECATED - kept for backward compatibility, not updated
  last_message_timedate: "14:30, 09.12.2025",
  last_practice_timedate: "18:51, 12.12.2025",
  lessons: [
    {
      lesson: "7",
      teacher: "Teacher Name",
      practice_count: 5,
      first_practice: "10:30, 01.12.2025",
      last_practice: "18:51, 12.12.2025",
      paid: false,  // Payment status for this specific class
      message_count: 3  // Message count for this specific class
    }
  ],
  created_at: "HH:MM, DD.MM.YYYY",
  updated_at: "HH:MM, DD.MM.YYYY"
}
```

**sales_db.last_run_timestamp:**
```javascript
{
  identifier: "sales_leads_etl",
  last_run_timestamp: "2025-12-09T18:51:42.998Z",
  updated_at: "2025-12-09T18:51:42.998Z"
}
```

**logger_db.logger_stats:**
```javascript
{
  source: "sales_etl",
  log_level: "info",
  timestamp: datetime,
  new_leads: 5,
  total_run_time: 2.34,
  success: true,
  error_message: null,
  metadata: {...}
}
```

**Indexes:** All collections have indexes on query fields. Check `_setup_collections()` in [mongo_handler.py](src/etl/db/mongodb/mongo_handler.py) before adding queries.

## ETL Data Flow

### Execution Sequence

1. **Extract** ([extract.py](src/etl/extract.py)):
   - Selenium opens WhatsApp Web with persistent session
   - Navigates to `STUDENTS_GROUP` and `SALES_TEAM_GROUP`
   - Reads last N messages (`MESSAGE_COUNT` from .env)
   - Returns: `{students: [{sender, timestamp, text}], sales: [...]}`

2. **Students Pipeline**:
   - **Transform** ([students_etl/transform.py](src/etl/students_etl/transform.py)):
     - Fetches student roster from Google Sheets
     - Filters by keywords (`PRACTICE_WORDS`, `MESSAGE_WORDS`)
     - Normalizes phone numbers
     - Enriches with student metadata
   - **Load MongoDB** ([students_etl/load_mongo_stats.py](src/etl/students_etl/load_mongo_stats.py)):
     - Updates `student_stats` collection via upsert
     - Increments practice counts per lesson
     - Auto-advances lessons
     - Deduplicates by timestamp comparison
   - **Load Sheets** ([students_etl/load_sheets_updates.py](src/etl/students_etl/load_sheets_updates.py)):
     - Batch updates `last_practice` column
     - Formats dates as DD/MM/YYYY

3. **Sales Pipeline**:
   - **Transform** ([sales_etl/transform.py](src/etl/sales_etl/transform.py)):
     - Filters messages newer than `last_run_timestamp`
     - Regex extracts: `מקור:`, `שם:`, `טלפון:`, `מייל:`
     - Updates `last_run_timestamp`
   - **Load** ([sales_etl/load.py](src/etl/sales_etl/load.py)):
     - Appends to Google Sheets
     - Logs to `logger_stats`

## Key Architectural Decisions

### Design Patterns in Use

- **Singleton:** MongoDB connection ([mongo_finder.py](src/etl/db/mongodb/mongo_finder.py))
- **Repository:** Database operations abstracted in [mongo_handler.py](src/etl/db/mongodb/mongo_handler.py)
- **Strategy:** Separate ETL strategies per domain
- **Factory:** Environment-based connection initialization

### Critical Conventions

**Timestamp Format:**
- Always use custom format: `"HH:MM, DD.MM.YYYY"` (string, not datetime)
- Helper functions in [mongo_handler.py](src/etl/db/mongodb/mongo_handler.py): `get_current_timestamp()`, `parse_timestamp()`, `format_timestamp()`
- Sales pipeline uses ISO format for `last_run_timestamp`

**Phone Number Normalization:**
- Remove Unicode artifacts, spaces, dashes
- Strip leading `+`
- Stored as strings like `"972 55-660-2298"`
- Normalization logic in [transform.py](src/etl/students_etl/transform.py)

**Deduplication Strategy:**
- **Students:** Compare incoming timestamp vs stored `last_practice_timedate`/`last_message_timedate`
- **Sales:** Process only messages newer than global `last_run_timestamp`
- **MongoDB:** Upserts use `uniq_id` (MD5 hash) or `identifier` as unique key

**Upsert Pattern:**
- All MongoDB writes use upsert operations
- Always include `updated_at` timestamp
- Only set `created_at` on insert (`$setOnInsert`)

### Environment Detection

MongoDB host auto-detection in [mongo_finder.py](src/etl/db/mongodb/mongo_finder.py):
- Docker: `mongo`
- WSL: `localhost`
- Local: `localhost`

Check `platform.system()` and hostname resolution before modifying connection logic.

## Configuration Management

**All configuration via .env file:**

**Critical Variables:**
- `STUDENTS_GROUP`, `SALES_TEAM_GROUP`: WhatsApp group names (exact match required)
- `MESSAGE_COUNT`: Messages to read per run (default 50)
- `PRACTICE_WORDS`, `MESSAGE_WORDS`: CSV keyword lists for filtering
- `SHEET_ID`, `SALES_SHEET_ID`: Google Sheets identifiers
- `CREDENTIALS_FILE`: Path to service account JSON

**Database Variables:**
- `MONGO_HOST`: Auto-detected, override only for custom setups
- `STUDENTS_DB`, `SALES_DB`, `LOGGER_DB`: Database names
- Collection names are also configurable

**Pattern:** No hardcoded values. If you need a new configurable parameter, add it to `.env.exemple` and load via `os.getenv()`.

## Code Navigation

### Finding Key Logic

**Student Practice Tracking:**
- Entry: [students_etl/transform.py](src/etl/students_etl/transform.py) `transform_records()`
- Lesson progression: [load_mongo_stats.py](src/etl/students_etl/load_mongo_stats.py) `load_students_to_mongo()`
- Sheet updates: [load_sheets_updates.py](src/etl/students_etl/load_sheets_updates.py) `load_sheets_with_updates()`

**Sales Lead Extraction:**
- Regex patterns: [sales_etl/transform.py](src/etl/sales_etl/transform.py) `extract_lead_data()`
- Sheet appending: [sales_etl/load.py](src/etl/sales_etl/load.py) `append_to_sheets()`

**WhatsApp Scraping:**
- Selenium logic: [extract.py](src/etl/extract.py) `extract_messages()`
- Session persistence: Uses `--user-data-dir=whatsapp_session`

**Database Operations:**
- Connection: [mongo_finder.py](src/etl/db/mongodb/mongo_finder.py) `MongoDBConnection`
- CRUD: [mongo_handler.py](src/etl/db/mongodb/mongo_handler.py) `MongoDBHandler`

**Google Sheets:**
- API wrapper: [sheets_connect.py](src/sheets_connect.py) `GoogleSheets`

### File Modification Guidelines

**Before editing any file:**
1. Check if configuration should be externalized to .env
2. Verify timestamp format compliance
3. Ensure phone number normalization consistency
4. Maintain upsert pattern for MongoDB operations
5. Check if indexes need updating

**When adding new fields:**
1. Update MongoDB indexes in `_setup_collections()`
2. Add to `$setOnInsert` or `$set` operators appropriately
3. Update Google Sheets column mappings if applicable

**When modifying queries:**
- Ensure indexes exist for query fields
- Use `uniq_id` for student lookups
- Use `identifier` for sales timestamp lookups

## Common Patterns

### Naming Conventions

- Functions: `snake_case`
- Classes: `PascalCase`
- Constants: `UPPER_SNAKE_CASE`
- Private methods: `_method_name`

### Error Handling

- Use try-except with traceback printing
- Selenium: Multiple retry attempts with sleep delays
- MongoDB: 5-second timeout configured
- Docker: Infinite retry loop continues on failure

### Logging Pattern

Print statements with visual indicators:
- `✓` Success
- `✗` Error
- `⚠` Warning
- `💾` Database operation
- Separator lines: `"="*60`

### Import Order

1. Standard library
2. Third-party packages
3. Local imports

## Critical Constraints

### Do Not Break

1. **Timestamp Format Consistency:** Mixed string/datetime types will break comparisons
2. **Phone Number Keys:** Normalization must be identical in transform and lookup
3. **Upsert Keys:** Changing `uniq_id` or `identifier` logic creates duplicates
4. **Session Persistence:** `whatsapp_session/` directory must persist across restarts
5. **Keyword Matching:** Case-sensitive Hebrew text matching
6. **Batch Updates:** Google Sheets rate limits require batching
7. **Lesson Field Requirements:** All new lesson objects must include `paid` (bool) and `message_count` (int) fields

### Schema Migration Notes

**Message Counting Refactor:**
- Student-level `total_messages` field is **deprecated** (kept for backward compatibility)
- Message counts are now tracked per-lesson in `lessons[].message_count`
- Run migration script after deployment: `python -m src.etl.students_etl.load_mongo_stats`

**Payment Status:**
- Each lesson has a `paid` boolean field (default: `false`)
- Represents payment status for that specific class
- Must be explicitly set when marking a lesson as paid

### Implicit Dependencies

- **Students Pipeline:** Requires Google Sheets "main" worksheet with columns: `phone_number`, `name`, `current_lesson`, `teacher`, `last_practice`
- **Sales Pipeline:** Requires Hebrew format: `מקור: X\nשם: Y\nטלפון: Z\nמייל: W`
- **WhatsApp Groups:** Must exist and be accessible in logged-in session
- **MongoDB:** Must be running before container starts

## Deployment Context

**Docker Execution:**
- Base: `selenium/standalone-chrome:latest`
- VNC ports: 5900 (VNC), 7900 (noVNC web interface)
- Selenium Grid: port 4444
- Startup: Waits for Selenium Grid readiness (30s timeout)
- Loop: Every 2 hours, continues on failure

**Volume Requirements:**
- `whatsapp_session/`: Persist login state
- `secrets/`: API credentials
- `.env`: Configuration

**Debugging:**
- Access noVNC at `http://localhost:7900` to watch browser
- MongoDB logs via Docker logs
- ETL logs print to stdout

## Testing Approach

**Current State:** No active test suite.

**Testing Patterns:**
- Manual testing in production
- Print-based debugging throughout
- `if __name__ == '__main__'` test blocks in some modules

**When adding tests:**
- Use pytest with pytest-mock
- Mock external services: Selenium, MongoDB, Google Sheets
- Test timestamp parsing/formatting logic
- Test phone number normalization
- Test deduplication logic

## Additional Optional Features

### Monitoring and Observability
- Structured logging to replace print statements (e.g., Python `logging` module)
- Prometheus metrics endpoint for ETL run statistics
- Alerting on ETL failures or data anomalies
- Dashboard for visualizing `logger_stats` collection

### Data Quality Improvements
- Schema validation for MongoDB documents (e.g., `jsonschema` or Pydantic)
- Data quality checks: phone number format validation, timestamp range validation
- Duplicate detection beyond timestamp comparison
- Hebrew text normalization for consistent keyword matching

### Performance Optimization
- Async MongoDB operations (motor library)
- Parallel processing of student and sales pipelines
- Caching Google Sheets roster to reduce API calls
- Incremental message extraction (track last processed message ID)

### Developer Experience
- Unit and integration test suite
- CI/CD pipeline for Docker builds
- Local development setup without Docker
- Type hints throughout codebase (mypy compliance)
- Pre-commit hooks for code formatting (black, isort)

### Robustness
- Retry logic with exponential backoff for API calls
- Circuit breaker for external service failures
- Health check endpoint for container orchestration
- Graceful shutdown handling for in-progress ETL runs
- Dead letter queue for failed message processing

### Feature Extensions
- Multi-language support beyond Hebrew
- Configurable ETL schedules per pipeline
- Manual trigger endpoint (webhook or CLI command)
- Message archive to MongoDB before processing
- Export functionality for student reports

### Security Hardening
- Secrets management via external vault (not volume mounts)
- MongoDB authentication enforcement
- Google Sheets access scoped to specific worksheets
- WhatsApp session encryption at rest
- Audit logging for data access

### Operational Tools
- CLI for manual ETL runs with date ranges
- Database migration system for schema changes
- Backup automation for MongoDB collections
- Data reconciliation reports (Sheets vs MongoDB)
- Dry-run mode for testing without writes