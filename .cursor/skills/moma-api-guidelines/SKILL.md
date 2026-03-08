---
name: moma-api-guidelines
description: Guidelines and constraints for interacting with the Moma API (魔码云服API). Use this skill whenever you are about to write code to fetch data from Moma API, test new Moma API endpoints, or run Python scripts that interact with the Moma API.
---

# Moma API Guidelines

**Official API Documentation**: [https://momaapi.com/docs-shares.html](https://momaapi.com/docs-shares.html)

This skill provides strict rules and best practices for interacting with the Moma API (魔码云服API) to ensure quota limits are respected and data fetching is efficient.

## Core Rules

1. **Strict Rate Limits**: 
   - Total allowed API calls: **200,000 requests**.
   - Per-minute limit: **300 requests/minute**.
   - *Action*: Always implement global throttling (e.g., `time.sleep()`) and HTTP 429 backoff retries in your API client to strictly stay under 300 requests/minute.

2. **Minimize API Calls via Date Ranges**:
   - If an API endpoint supports start and end dates (e.g., `st` and `et` parameters), ALWAYS use them to fetch data in bulk over a time period rather than making individual daily requests.

3. **Test Before Batching**:
   - When using a *new* or *untested* API endpoint for the first time, NEVER write a loop to batch download immediately.
   - *Action*: First, write a minimal script to make a **single request** (or very few requests). Run it, inspect the JSON response structure, verify it works, and ONLY THEN proceed to implement the full batch download logic.

4. **Incremental Updates**:
   - All data fetching logic must support **incremental updates**.
   - Do not drop and recreate tables or fetch full historical data every time. Use `on_conflict_do_update` or `on_conflict_do_nothing` (upsert logic) and only fetch data from the last synced date to the current date.

5. **Environment Activation**:
   - Before executing any Python code or scripts, ALWAYS ensure you have activated the correct Python virtual environment for the project.
