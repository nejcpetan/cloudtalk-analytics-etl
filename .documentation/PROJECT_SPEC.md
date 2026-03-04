# CloudTalk Analytics ETL Service — Project Specification

## Document Info

| Field | Value |
|-------|-------|
| **Project Name** | CloudTalk Analytics ETL Service |
| **Codename** | `cloudtalk-etl` |
| **Owner** | Big Bang d.o.o. — IT Administration |
| **Version** | 1.0 |
| **Created** | March 2026 |
| **Status** | Ready for Development |

---

## 1. Problem Statement

Big Bang uses CloudTalk as its phone system with ~45 agents. Management needs daily analytics data about agent performance, call volumes, missed calls, and operational efficiency. Currently, this data lives only inside the CloudTalk dashboard with no way to feed it into the company's Qlik Sense BI environment for custom reporting and cross-referencing with other business data.

---

## 2. Solution Overview

Build an automated nightly ETL (Extract, Transform, Load) service that:

1. **Extracts** all available call data, agent data, and group statistics from the CloudTalk REST API (v1.7)
2. **Transforms** the raw data into clean, typed, well-structured rows
3. **Loads** it into a Neon PostgreSQL database that Qlik Sense connects to directly via its native PostgreSQL connector

The service runs as a Docker container deployed via Portainer on an existing Linux server, triggered nightly by an internal cron job.

---

## 3. Stakeholders

| Role | Responsibility |
|------|---------------|
| **Management / Bosses** | Consume data in Qlik Sense, build their own views and dashboards |
| **IT Admin (Nathan)** | Build, deploy, and maintain the ETL service |
| **Qlik Sense Team** | Connect to Neon DB and create BI views/reports |

---

## 4. Scope

### 4.1 In Scope

- Nightly extraction of **all call records** from CloudTalk (the previous day's data)
- Nightly snapshot of **all agents** and their current status
- Nightly snapshot of **group statistics** (answered, unanswered, abandon rates, wait times, durations)
- Storing all data in a Neon PostgreSQL database with clean, well-typed schemas
- Docker containerization with internal cron scheduling
- Deployment via Portainer from a Git repository
- Idempotent runs — re-running for the same date does not duplicate data
- Structured logging and basic health monitoring
- Rate limit compliance (hard cap at 50 requests/minute, well below CloudTalk's 60/min limit)
- Retry logic with exponential backoff for transient API failures
- Environment-variable-based configuration for all secrets and settings

### 4.2 Out of Scope

- Qlik Sense dashboard creation (Qlik team handles this)
- Pre-calculated views, aggregations, or summary tables (Qlik team handles this)
- Real-time or streaming data
- SMS data extraction
- Campaign data extraction
- Conversation Intelligence data (Phase 2 — only 10/45 agents have it enabled)
- Historical backfill (starts from deployment day forward)
- Alerting/notification system (can be added later)

### 4.3 Phase 2 (Future)

- **Conversation Intelligence extraction** — AI summaries, sentiment analysis, talk-listen ratios, topics, transcriptions per call. Requires 6 additional API calls per call, so rate-limit budgeting must be re-evaluated. The service should be designed with a feature flag (`ENABLE_CONVERSATION_INTELLIGENCE=false`) so this can be turned on without code changes.
- **Alerting** — Slack/email notifications on ETL failures
- **Historical backfill** — A CLI command to backfill a date range on demand

---

## 5. Data Sources (CloudTalk API v1.7)

Base URL: `https://my.cloudtalk.io/api`
Authentication: HTTP Basic Auth (API Key ID + Secret)
Rate Limit: 60 requests/minute per company (we self-limit to 50/min for safety)

### 5.1 Endpoints Used

| Endpoint | Method | Purpose | Pagination |
|----------|--------|---------|------------|
| `/calls/index.json` | GET | Full call history with filters | Yes (page + limit, max 1000) |
| `/agents/index.json` | GET | All agents with status/details | Yes (page + limit, max 1000) |
| `/statistics/realtime/groups.json` | GET | Group-level stats snapshot | No |

### 5.2 Key Call Record Fields (from `/calls/index.json`)

Each call record contains a `Cdr` object and a `Contact` object:

**Cdr (Call Detail Record):**
- `id` — Unique call ID
- `type` — `incoming`, `outgoing`, `internal`
- `billsec` — Billed seconds
- `talking_time` — Actual talk time in seconds
- `waiting_time` — Time caller waited before answer (seconds)
- `wrapup_time` — Agent wrap-up time after call (seconds)
- `public_external` — External phone number
- `public_internal` — Internal number of the agent
- `recorded` — Boolean, whether call was recorded
- `is_voicemail` — Boolean
- `user_id` — Agent ID who handled the call
- `started_at` — ISO 8601 timestamp
- `answered_at` — ISO 8601 timestamp (null if missed)
- `ended_at` — ISO 8601 timestamp
- `recording_link` — URL to recording

**Contact (associated contact, if any):**
- `id`, `name`, `company`, `tags`

**Filtering:** The API supports `date_from`, `date_to`, `user_id`, `type`, `status` (missed/answered) filters. We will use `date_from` and `date_to` to pull exactly yesterday's data.

### 5.3 Key Agent Fields (from `/agents/index.json`)

- `id`, `firstname`, `lastname`, `email`
- `availability_status` — `online`, `offline`, `busy`, etc.
- `extension` — Internal extension number
- `default_number` — Primary phone number
- `associated_numbers` — All numbers assigned to agent

### 5.4 Key Group Stats Fields (from `/statistics/realtime/groups.json`)

- `name`, `id` — Group identification
- `operators` — Number of agents in group
- `answered` — Calls answered today
- `unanswered` — Calls unanswered today
- `abandon_rate` — Percentage
- `avg_waiting_time`, `max_waiting_time` — In seconds
- `avg_call_duration` — In seconds
- `real_time.waiting_queue` — Current callers in queue
- `real_time.avg_waiting_time`, `real_time.max_waiting_time`
- `real_time.avg_abandonment_time`

---

## 6. Data Destination

### 6.1 Neon PostgreSQL

- **Provider:** Neon (https://neon.tech) — Serverless PostgreSQL
- **Plan:** Free tier or Pro (depends on data volume)
- **Connection:** Standard PostgreSQL connection string via SSL
- **Consumer:** Qlik Sense connects using its native PostgreSQL ODBC connector

### 6.2 Database Tables

The database will contain 3 tables. The Qlik Sense team will create their own views, so we optimize for **data completeness and clean typing** rather than star-schema or pre-aggregation.

**Table: `calls`**
Stores every individual call record. One row per call. This is the primary fact table.

**Table: `agents`**
Stores a daily snapshot of all agents. One row per agent per sync date. This acts as a slowly-changing dimension — the Qlik team can always see who was active on any given day.

**Table: `group_stats_daily`**
Stores a daily snapshot of group-level statistics. One row per group per sync date.

Full schemas are defined in the Technical Specification document.

---

## 7. Non-Functional Requirements

### 7.1 Reliability

- Retry all API calls up to 5 times with exponential backoff (2s, 4s, 8s, 16s, 32s) plus jitter
- Handle HTTP 429 (rate limited) by reading `X-CloudTalkAPI-ResetTime` header and waiting
- Handle HTTP 5xx with retries; fail gracefully on HTTP 4xx (log and skip)
- Idempotent writes using `ON CONFLICT` / upsert patterns — safe to re-run

### 7.2 Rate Limiting

- CloudTalk allows 60 requests/minute
- Self-imposed limit: **50 requests/minute** (83% of capacity, leaving headroom)
- Implementation: Token bucket or leaky bucket rate limiter applied at the HTTP transport layer
- Estimated daily usage at 45 agents:
  - Agents endpoint: ~1 request (all fit in one page)
  - Groups endpoint: 1 request
  - Calls endpoint: Depends on daily volume. At 500 calls/day with limit=1000, that's 1 page. At 2000 calls/day, that's 2 pages
  - **Total: ~3-5 requests on a normal day** — well within limits
  - Phase 2 with Conversation Intelligence: 6 calls per call record × daily volume. At 500 calls = 3,000 requests = ~60 minutes at 50 req/min. This is why it's Phase 2.

### 7.3 Security

- All secrets (API keys, database connection string) stored as environment variables, never in code
- CloudTalk API uses HTTPS only
- Neon connection uses SSL (`sslmode=require`)
- Docker container runs as non-root user
- No sensitive data logged (mask API keys, connection strings in logs)

### 7.4 Observability

- Structured JSON logging with timestamps
- Log levels: DEBUG, INFO, WARNING, ERROR
- Key events logged: ETL start, API calls made, records fetched, records written, ETL complete, errors
- Exit codes: 0 for success, 1 for failure — compatible with monitoring tools
- Run summary logged at end: total calls synced, agents synced, groups synced, duration, errors

### 7.5 Performance

- Nightly batch job — no latency requirements
- Target: Complete full ETL cycle in under 5 minutes on a normal day
- Database writes use batch inserts (not row-by-row)

---

## 8. Deployment Architecture

```
┌─────────────────────────────────────────────────────┐
│                  Linux Server                        │
│                  (Portainer)                          │
│                                                      │
│  ┌───────────────────────────────────────────────┐   │
│  │         Docker Container                       │   │
│  │         cloudtalk-etl                          │   │
│  │                                                │   │
│  │  ┌─────────┐    ┌──────────┐                  │   │
│  │  │  cron   │───▶│  Python  │                  │   │
│  │  │(2:00 AM)│    │  ETL     │                  │   │
│  │  └─────────┘    │  Script  │                  │   │
│  │                  └────┬─────┘                  │   │
│  │                       │                        │   │
│  └───────────────────────┼────────────────────────┘   │
│                          │                            │
└──────────────────────────┼────────────────────────────┘
                           │
              ┌────────────┼────────────┐
              │            │            │
              ▼            ▼            ▼
     ┌──────────────┐  ┌────────┐  ┌────────────┐
     │  CloudTalk   │  │  Neon  │  │ Qlik Sense │
     │  REST API    │  │ Postgres│──│ (reads DB) │
     │  (source)    │  │ (dest) │  └────────────┘
     └──────────────┘  └────────┘
```

### 8.1 Deployment Flow

1. Developer pushes code to Git repository
2. Portainer pulls the image (or builds from Dockerfile via Portainer's git-based stack deploy)
3. Container starts with environment variables configured in Portainer
4. Internal cron runs the ETL script at the configured schedule (default: 2:00 AM CET)
5. Container stays running between executions (cron daemon keeps it alive)

---

## 9. Development Phases

### Phase 1: Foundation (MVP)

**Goal:** Get data flowing from CloudTalk into Neon so the Qlik team can start building.

1. **Step 1.1 — Project Setup**
   - Initialize Python project with `pyproject.toml`
   - Set up dependency management
   - Create project structure
   - Create `.env.example` with all required environment variables
   - Create basic `Dockerfile`

2. **Step 1.2 — CloudTalk API Client**
   - Implement authenticated HTTP client with rate limiting (50 req/min)
   - Implement retry logic with exponential backoff + jitter
   - Implement pagination handler for list endpoints
   - Handle 429 responses using CloudTalk's rate limit headers
   - Write unit tests with mocked responses

3. **Step 1.3 — Database Layer**
   - Create Neon database and configure connection
   - Implement database connection with SSL
   - Create migration/schema initialization script
   - Implement upsert functions for each table (calls, agents, group_stats_daily)
   - Write integration tests

4. **Step 1.4 — ETL Pipeline**
   - Implement main ETL orchestrator
   - Extract: Pull yesterday's calls, all agents, group stats
   - Transform: Clean/validate data, handle nulls, type casting
   - Load: Batch upsert into Neon tables
   - Implement structured logging throughout
   - Implement run summary reporting

5. **Step 1.5 — Containerization & Deployment**
   - Finalize Dockerfile with cron setup
   - Create `docker-compose.yml` for local development
   - Create Portainer stack configuration
   - Test end-to-end in Docker
   - Deploy to production server via Portainer

### Phase 2: Conversation Intelligence (Future)

1. Add feature flag `ENABLE_CONVERSATION_INTELLIGENCE`
2. For each call ID from the daily sync, fetch: summary, sentiment, talk-listen ratio, topics, transcription, smart-notes
3. Create `call_intelligence` table
4. Adjust rate limit budget — this phase will significantly increase API calls
5. Consider splitting into two cron jobs: one for core data (fast), one for CI data (slow)

---

## 10. Success Criteria

- [ ] ETL runs every night without manual intervention
- [ ] All call records from the previous day appear in the `calls` table within 1 hour of the scheduled run
- [ ] Agent roster is refreshed daily
- [ ] Group statistics are captured daily
- [ ] Qlik Sense team can connect to Neon and see all tables
- [ ] Re-running the ETL for the same day does not create duplicate records
- [ ] The service handles CloudTalk API outages gracefully (retries, then fails with clear error)
- [ ] Logs provide clear visibility into what happened during each run

---

## 11. Risks & Mitigations

| Risk | Impact | Mitigation |
|------|--------|------------|
| CloudTalk API rate limit hit | ETL fails or slows down | Self-limit to 50/min, respect 429 headers, retry with backoff |
| CloudTalk API changes/downtime | ETL fails | Retry logic, structured error logging, manual re-run capability |
| Neon DB connection issues | Data not written | Retry DB connections, connection pooling, clear error logs |
| Data volume spike (e.g., 5000 calls/day) | Longer ETL run, more API pages | Pagination handling scales automatically; monitor run duration |
| Neon free tier limits exceeded | DB unavailable | Monitor usage; upgrade to Pro tier when needed (~$19/mo) |
| Duplicate data on re-runs | Incorrect analytics | Upsert (ON CONFLICT) on all writes |

---

## 12. Environment Variables

| Variable | Description | Example |
|----------|-------------|---------|
| `CLOUDTALK_API_KEY_ID` | CloudTalk API access key ID | `ABCDEFGHIJ` |
| `CLOUDTALK_API_KEY_SECRET` | CloudTalk API access key secret | `X05Dg4c331...` |
| `DATABASE_URL` | Neon PostgreSQL connection string | `postgresql://user:pass@ep-cool-name.eu-central-1.aws.neon.tech/cloudtalk_analytics?sslmode=require` |
| `CRON_SCHEDULE` | Cron expression for ETL schedule | `0 2 * * *` (2:00 AM daily) |
| `LOG_LEVEL` | Logging verbosity | `INFO` |
| `RATE_LIMIT_RPM` | Max requests per minute to CloudTalk | `50` |
| `ENABLE_CONVERSATION_INTELLIGENCE` | Feature flag for Phase 2 | `false` |
| `ETL_DATE_OVERRIDE` | Optional: Override sync date (YYYY-MM-DD) for manual re-runs | _(empty = yesterday)_ |
| `TZ` | Container timezone | `Europe/Ljubljana` |
