# Google Maps Business Scraper

This Go CLI scrapes Google Maps for any business category and stores the results (plus derived contact info) in MySQL. The workflow is split into three explicit steps so you can run them independently or on a schedule:

1. **Step 1 (`-step1`)** – reads every city in the `locations` table, geocodes the center, pans radially (~4 km) to gather nearby results at zoom level 15, and upserts the businesses into the `businesses` table. Each run resets the `processed` flag so every city is covered.
2. **Step 2 (`-step2`)** – reopens every stored Google Maps URL (no new scraping) and captures the website, phone, and any email that Google exposes.
3. **Step 3 (`-step3`)** – visits each stored website, follows contact/team-style links, hunts for Swiss `*.ch` emails, validates them via MX records, and updates the `email` column. Workers run concurrently, so the scan can revisit flaky sites in later runs.

## Prerequisites

- Go 1.21+ installed locally; configure GoLand (or your IDE) with that GOROOT.
- Google Chrome or Chromium installed (Chromedp drives it headlessly).
- MySQL/MariaDB instance reachable via the credentials in `.env`.
- Outbound network access to Google Maps, OpenStreetMap’s Nominatim API, and each target business website.

## Installation

1. Clone/copy the project into a directory (e.g., `~/Farm/go-scraper-gmaps`).
2. Install Go dependencies:
   ```bash
   go mod tidy
   ```
3. Copy `.env.example` to `.env` and fill in your settings:
   ```env
   DB_HOST=127.0.0.1:3306
   DB_USER=gmaps
   DB_PASSWORD=mapmap
   DB_NAME=gmaps
   BUSINESS_CATEGORY="Radiology Clinics"
   HEADLESS=true
   RANDOM_DELAY_MIN_MS=1500
   RANDOM_DELAY_MAX_MS=4000
   DETAIL_BATCH_LIMIT=0     # 0 = process entire backlog
   EMAIL_BATCH_LIMIT=0
   WEBSITE_FETCH_TIMEOUT_MS=15000
   WEBSITE_MAX_PAGES=6
   WEBSITE_CONCURRENCY=5
   GEOCODER_BASE_URL=https://nominatim.openstreetmap.org/search
   GEOCODER_USER_AGENT=your-app-name/1.0 (contact@example.com)
   GEOCODER_EMAIL=contact@example.com
   GEOCODER_DELAY_MS=2000
   ```
4. Populate the `locations` table (at minimum `city`; optional `latitude`/`longitude`). Example SQL:
   ```sql
   INSERT INTO locations (city, latitude, longitude)
   VALUES ('Zürich', 47.3769, 8.5417),
          ('Bern', 46.9480, 7.4474),
          ('Basel', 47.5596, 7.5886);
   ```

## Usage

Run steps individually; each command can optionally target specific cities via `-city "City1,City2"`:

```bash
# Step 1: scrape Google Maps and store businesses
go run main.go -step1

# Step 2: reopen stored Maps URLs to capture website/phone/email
go run main.go -step2

# Step 3: crawl websites for Swiss *.ch emails
go run main.go -step3

# Limit any step to selected cities
go run main.go -step1 -city "Zürich,Bern"
```

### Step summaries
- Each step prints a summary when it finishes (e.g., cities processed, businesses inserted/updated, emails found).
- If you see warnings (DNS failure, TLS timeout, 403, etc.), the run continues; rerun the step later to retry flaky sites.

## Customization

- **Delays & retries**: `RANDOM_DELAY_*` controls Chromedp throttling; website/email retries are hardcoded to 3 attempts.
- **Geocoding**: Provide a descriptive `GEOCODER_USER_AGENT` + contact email per Nominatim’s policy. Coordinates are cached in `locations` after the first lookup.
- **Concurrency**: Increase `WEBSITE_CONCURRENCY` to crawl more sites at once; lower it if you hit bandwidth or rate limits.
- **Category-specific runs**: Change `BUSINESS_CATEGORY` in `.env` and rerun Step 1. Existing data stays in the database unless you manually delete it.

## Database schema (created automatically)

- `locations(city, processed, latitude, longitude, created_at)`
- `businesses(city, category, name, address, rating, reviews, url, email, website, telephone, scraped_at)`

Use phpMyAdmin or `mysql` CLI to inspect/update the data:
```sql
SELECT processed, city FROM locations;
SELECT city, category, name, website, telephone, email FROM businesses LIMIT 20;
```

## Common issues

- **“GOROOT not defined”** – install Go 1.21+ and point GoLand/IDE to that SDK.
- **No data in DB** – confirm `.env` credentials match the schema you’re viewing in phpMyAdmin (`DB_USER`, `DB_NAME`, etc.).
- **Missing cities** – ensure the `locations` table has the exact city names you want to scrape; Step 1 relies entirely on that table.
- **Slow runs** – reduce `RANDOM_DELAY_*`, drop some of the pan bearings, or limit to fewer cities via `-city` to iterate faster.

## License

MIT License (adjust as needed for your project).
