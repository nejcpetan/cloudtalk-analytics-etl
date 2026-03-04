# CloudTalk ETL — AI Vibe Coding Guide

## How to Build This 100% with AI

### What You Need Before Starting

1. **A coding AI tool** — any of these work:
   - **Claude Code** (terminal) — best for this kind of project, it can run and test as it goes
   - **Cursor** — great if you want to see the code in an editor
   - **Windsurf** — similar to Cursor
   - **Claude.ai with computer use** — works but slower iteration

2. **On your machine:**
   - Docker Desktop (or Docker on your Linux server)
   - Git installed
   - A terminal

3. **Accounts ready:**
   - Neon account (https://neon.tech — free tier is fine to start)
   - CloudTalk API keys (from CloudTalk Dashboard → Account → Settings → API Keys)
   - GitHub/GitLab repo created (empty, we'll push to it)

4. **The two spec documents** — `PROJECT_SPEC.md` and `TECHNICAL_SPEC.md` from our chat

---

### The Vibe Coding Workflow

The trick to vibe coding a project like this is: **don't give the AI the whole project at once.** Break it into phases. Each prompt below builds on the previous one. After each phase, you test it, verify it works, and THEN move to the next prompt.

**Your workflow for each phase:**

```
1. Paste the prompt into your AI tool
2. Let it generate code
3. Read through it quickly (you don't need to understand every line,
   but look for obvious "that doesn't look right" moments)
4. Run it / test it
5. If something breaks, paste the error back to the AI and say "fix this"
6. Once it works, commit to git
7. Move to next phase
```

**Pro tip:** If using Claude Code, just `cd` into your project folder and paste the prompt. It will create files, run them, and fix errors automatically.

---

## PROMPT 1: Project Scaffolding

> Copy everything below the line and paste it into your AI coder.

---

```
I'm building a Python ETL service called `cloudtalk-etl`. I need you to create the project scaffolding from scratch.

Here are the two specification documents for this project:

[PASTE THE FULL CONTENTS OF PROJECT_SPEC.md HERE]

[PASTE THE FULL CONTENTS OF TECHNICAL_SPEC.md HERE]

For now, I ONLY want Phase 1, Step 1.1 — the project scaffolding. Do NOT build any business logic yet.

Create:
1. The full directory structure as defined in the technical spec (Section 2)
2. `pyproject.toml` with all dependencies pinned as specified (Section 11)
3. `.env.example` as specified (Section 12)
4. `.gitignore` for Python/Docker projects
5. `src/cloudtalk_etl/__init__.py` (empty)
6. `src/cloudtalk_etl/config.py` — Pydantic Settings class as specified (Section 3)
7. `src/cloudtalk_etl/logging.py` — structlog JSON setup as specified (Section 8)
8. `src/cloudtalk_etl/__main__.py` — minimal entry point that just loads config and prints "CloudTalk ETL ready"
9. A basic `README.md` with project name and setup instructions

After creating the files, install the dependencies and verify the config loads by running:
`python -m cloudtalk_etl`

It should print "CloudTalk ETL ready" without errors.
```

---

**After this prompt:** You should have a clean project structure. Test it:
```bash
# Create a .env file from the example
cp .env.example .env
# Edit .env with your real values (or dummy values for now)

# Install and run
pip install -e ".[dev]"
python -m cloudtalk_etl
# Should print "CloudTalk ETL ready"
```

Commit: `git init && git add . && git commit -m "scaffolding: project structure and config"`

---

## PROMPT 2: Rate Limiter + API Client

```
Continue building the cloudtalk-etl project. The scaffolding is done.

Now implement Phase 1, Step 1.2 — the CloudTalk API client with rate limiting.

Refer to the TECHNICAL_SPEC.md sections 4.1, 4.2 for the exact implementation.

Build these files:
1. `src/cloudtalk_etl/api/__init__.py`
2. `src/cloudtalk_etl/api/rate_limiter.py` — TokenBucketRateLimiter as specified
3. `src/cloudtalk_etl/api/client.py` — CloudTalkClient with:
   - HTTP Basic Auth using credentials from config
   - Rate limiting at 50 req/min (configurable)
   - Retry logic using tenacity: 5 attempts, exponential backoff with jitter
   - Proper handling of HTTP 429 (read X-CloudTalkAPI-ResetTime header)
   - Proper handling of HTTP 5xx (retry) vs 4xx (don't retry, log and raise)
   - Pagination helper method (get_all_pages)
   - Methods: get_calls(), get_agents(), get_group_stats()

4. Write tests:
   - `tests/test_rate_limiter.py` — test that the rate limiter actually throttles
   - `tests/test_api_client.py` — test with pytest-httpx mocks:
     - Test successful request
     - Test pagination across multiple pages
     - Test 429 handling triggers retry
     - Test 500 handling triggers retry
     - Test 400 does NOT retry

Run the tests and make sure they all pass.

Important implementation notes:
- CloudTalk API base URL: https://my.cloudtalk.io/api
- Auth is HTTP Basic Auth (key_id:key_secret)
- Response envelope: all data is under responseData
- Pagination: responseData contains pageCount, pageNumber, limit, data[]
- The /statistics/realtime/groups.json endpoint does NOT paginate
- Rate limit of 50/min is BELOW CloudTalk's actual 60/min limit — this is intentional safety margin
- Use httpx synchronous client (not async) — this is a batch job, no need for async
```

---

**After this prompt:** Run the tests:
```bash
python -m pytest tests/ -v
# All tests should pass
```

Commit: `git add . && git commit -m "feat: API client with rate limiting and retry logic"`

---

## PROMPT 3: Database Layer

```
Continue building the cloudtalk-etl project. The API client is done and tested.

Now implement Phase 1, Step 1.3 — the database layer.

Refer to TECHNICAL_SPEC.md sections 5.1, 5.2, 5.3 for the exact implementation.

Build these files:
1. `src/cloudtalk_etl/db/__init__.py`
2. `src/cloudtalk_etl/db/connection.py` — get_connection() using psycopg 3
3. `src/cloudtalk_etl/db/schema.py` — ensure_schema() function that:
   - Creates all tables IF NOT EXISTS (calls, agents, group_stats_daily, call_intelligence)
   - Creates all indexes IF NOT EXISTS
   - Uses the EXACT SQL from the technical spec Section 5.2
   - Is idempotent (safe to run multiple times)
4. `src/cloudtalk_etl/db/repositories.py` — upsert functions:
   - upsert_calls(conn, calls: list[dict]) -> int
   - upsert_agents(conn, agents: list[dict], sync_date: date) -> int
   - upsert_group_stats(conn, stats: list[dict], sync_date: date) -> int
   - ALL must use ON CONFLICT ... DO UPDATE for idempotency
   - ALL must use batch operations (executemany), not row-by-row
5. `scripts/init_db.py` — standalone script to initialize the database schema

Critical implementation notes:
- Use psycopg 3 (import psycopg, NOT psycopg2)
- Use named parameters %(name)s style, NOT positional %s
- Connection string includes sslmode=require for Neon
- Use autocommit=False, commit after each batch
- The agents table has a composite primary key (id, sync_date)
- The associated_numbers field is a PostgreSQL TEXT[] array
- Phone numbers must be TEXT type, not VARCHAR

Write tests:
- `tests/test_repositories.py` — test upsert functions with mock data
  - Test inserting new records
  - Test that re-inserting the same records (upsert) doesn't duplicate
  - Test that updated records are properly overwritten

If a real database connection is available (check DATABASE_URL in .env), 
run the schema initialization and verify the tables are created.
```

---

**After this prompt:** 
1. Make sure you have a Neon database created and the connection string in `.env`
2. Run: `python scripts/init_db.py` — should create all tables
3. Check Neon dashboard — you should see the tables
4. Run tests: `python -m pytest tests/ -v`

Commit: `git add . && git commit -m "feat: database layer with schema and upsert repos"`

---

## PROMPT 4: ETL Pipeline

```
Continue building the cloudtalk-etl project. The API client and database layer are done.

Now implement Phase 1, Step 1.4 — the ETL pipeline (extract, transform, load).

Refer to TECHNICAL_SPEC.md sections 6.1, 6.2, 6.3, 6.4 for the exact implementation.

Build these files:
1. `src/cloudtalk_etl/etl/__init__.py`
2. `src/cloudtalk_etl/etl/extract.py` — extraction functions:
   - extract_calls(client, sync_date) — pulls all calls for that date using date_from/date_to filters
   - extract_agents(client) — pulls all agents
   - extract_group_stats(client) — pulls group statistics
   - All use structured logging
3. `src/cloudtalk_etl/etl/transform.py` — transformation functions:
   - transform_calls(raw_calls, sync_date) — flattens Cdr + Contact, derives call_status and call_date
   - transform_agents(raw_agents, sync_date) — flattens Agent, creates fullname
   - transform_group_stats(raw_stats, sync_date) — flattens group + real_time
   - Include helper functions: safe_int(), safe_float(), parse_timestamp()
   - Handle all edge cases: None values, empty strings, "0" as string for booleans
4. `src/cloudtalk_etl/etl/load.py` — load functions with batch processing (BATCH_SIZE=500)
5. `src/cloudtalk_etl/main.py` — the main ETL orchestrator:
   - Determines sync_date (yesterday by default, or ETL_DATE_OVERRIDE)
   - Initializes all components (rate limiter, API client, DB connection)
   - Runs Extract → Transform → Load for all three data types
   - Logs a run summary at the end
   - Exits with code 1 on any failure
   - Properly closes connections in a finally block
6. Update `src/cloudtalk_etl/__main__.py` to call the real run_etl() function

Write tests:
- `tests/test_transform.py`:
  - Test transform_calls with a complete record
  - Test transform_calls with missing Contact
  - Test transform_calls derives "missed" when answered_at is null
  - Test transform_calls derives "answered" when answered_at exists
  - Test safe_int handles None, empty string, garbage, valid int
  - Test safe_float handles same edge cases
  - Test parse_timestamp handles valid ISO, null, empty string, garbage
  - Test transform_agents creates fullname from first + last
  - Test transform_group_stats handles missing real_time object

Run all tests. Then do a DRY RUN against the real CloudTalk API:
1. Make sure .env has real CLOUDTALK_API_KEY_ID and CLOUDTALK_API_KEY_SECRET
2. Make sure .env has real DATABASE_URL  
3. Run: python -m cloudtalk_etl
4. Check the logs — it should extract, transform, and load yesterday's data
5. Check Neon — the tables should have rows

IMPORTANT: If the dry run fails, debug it. The most common issues will be:
- Timestamp parsing (CloudTalk may return formats slightly different from examples)
- Field types (some fields that look like integers come as strings)
- Null handling in transforms
```

---

**After this prompt:** This is the big one. After this works, your ETL actually runs.
```bash
python -m cloudtalk_etl
# Watch the JSON logs — should see extract/transform/load for calls, agents, groups
# Check Neon dashboard — should see data in all 3 tables
```

Commit: `git add . && git commit -m "feat: complete ETL pipeline — extract, transform, load"`

---

## PROMPT 5: Docker + Deployment

```
Continue building the cloudtalk-etl project. The ETL pipeline works locally.

Now implement Phase 1, Step 1.5 — containerization and deployment.

Refer to TECHNICAL_SPEC.md Section 7 for the exact implementation.

Build these files:
1. `Dockerfile` — as specified:
   - Base: python:3.13-slim-bookworm
   - Install supercronic for Docker-friendly cron
   - Non-root user (etl:etl)
   - Layer caching (deps first, then source)
   - Entrypoint is the shell script
2. `scripts/entrypoint.sh` — as specified:
   - Reads CRON_SCHEDULE env var (default: "0 2 * * *")
   - Generates crontab file from env var
   - If first arg is "run", execute ETL immediately and exit
   - Otherwise start supercronic in foreground
   - Make sure the script has proper error handling (set -e)
3. `docker-compose.yml` for local development:
   - Reads from .env file
   - Sets TZ=Europe/Ljubljana
   - Has a comment showing how to override for immediate run
4. `docker-compose.prod.yml` for production:
   - Same but with restart: unless-stopped
   - No build context (assumes pre-built image or Portainer git deploy)

After creating the files:
1. Build the Docker image: docker build -t cloudtalk-etl .
2. Test immediate run: docker run --env-file .env cloudtalk-etl run
3. Verify it completes the ETL successfully inside the container
4. Test cron mode: docker run --env-file .env -e CRON_SCHEDULE="* * * * *" cloudtalk-etl
   (This sets cron to every minute for testing — watch logs, it should trigger within 60 seconds)
5. Stop the container after verifying cron works

Fix any issues with paths, permissions, or missing dependencies inside the container.

IMPORTANT: 
- supercronic binary URL for linux-amd64: 
  https://github.com/aptible/supercronic/releases/download/v0.2.33/supercronic-linux-amd64
- The entrypoint.sh MUST have LF line endings (not CRLF) or it will fail in Linux
- The cron job command should be: python -m cloudtalk_etl (runs from /app as working dir)
- Make sure PYTHONPATH includes /app/src or install the package properly in the Dockerfile
```

---

**After this prompt:**
```bash
# Build
docker build -t cloudtalk-etl .

# Test immediate run
docker run --rm --env-file .env cloudtalk-etl run

# Test cron (every minute for testing, Ctrl+C after you see it trigger)
docker run --rm --env-file .env -e CRON_SCHEDULE="* * * * *" cloudtalk-etl
```

Commit: `git add . && git commit -m "feat: Docker containerization with supercronic cron"`

Push: `git push origin main`

Then deploy in Portainer:
1. Stacks → Add Stack → Repository
2. Point to your git repo
3. Add all env vars from `.env.example`
4. Deploy!

---

## Troubleshooting Prompts

These are prompts you can use when things go wrong:

### "It's not connecting to Neon"
```
The cloudtalk-etl service is failing to connect to the Neon PostgreSQL database.
Here is the error: [PASTE ERROR]
The DATABASE_URL is in the format: postgresql://user:pass@ep-xxx.region.aws.neon.tech/dbname?sslmode=require
Debug and fix the connection issue.
```

### "CloudTalk API returns unexpected data"
```
The CloudTalk API is returning data in a format the transforms don't expect.
Here is the raw API response: [PASTE THE JSON]
Here is the error: [PASTE ERROR]
Update the transform layer to handle this format correctly.
```

### "Docker build fails"
```
The Docker build is failing with this error: [PASTE ERROR]
Here is my current Dockerfile: [PASTE OR REFERENCE]
Fix the Dockerfile.
```

### "Duplicate data after re-run"
```
Running the ETL twice for the same date creates duplicate rows in the database.
The upsert ON CONFLICT clause isn't working correctly.
Here is the repository code: [PASTE]
Here is the table schema: [PASTE]
Fix the upsert to be truly idempotent.
```

---

## What "Vibe Coding" Means Here

You're not writing code. You're:

1. **Architecting** — we already did this together (the two spec docs)
2. **Directing** — you give the AI specific prompts for what to build
3. **Testing** — you run the code and paste errors back
4. **Deploying** — you push to git and click deploy in Portainer

The AI writes 100% of the code. Your job is quality control and steering.

**Time estimate:** With a good AI coder, this whole project is maybe 2-4 hours of your time across a day. The AI does the work of what would take a developer 2-3 days.

---

## After Deployment Checklist

- [ ] ETL runs at 2:00 AM CET every night
- [ ] Check Portainer logs the morning after first run
- [ ] Verify data in Neon (connect with any SQL client or Neon's SQL editor)
- [ ] Give the Qlik Sense team the Neon connection details
- [ ] Monitor for a week to make sure it's stable
- [ ] Set a calendar reminder to check logs weekly for the first month
