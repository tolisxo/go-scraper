package main

import (
    "bytes"
    "context"
    "database/sql"
    "encoding/json"
    "flag"
    "fmt"
    "io"
    "log"
    "math"
    "math/rand"
    "net/http"
    "net/url"
    "os"
    "regexp"
    "sort"
    "strconv"
    "strings"
    "sync"
    "sync/atomic"
    "time"

    "github.com/PuerkitoBio/goquery"
    "github.com/chromedp/chromedp"
    _ "github.com/go-sql-driver/mysql"
    "github.com/joho/godotenv"
    "github.com/miekg/dns"
)

const (
    searchBaseURL           = "https://www.google.com/maps/search/"
    scrollIterations        = 5
    zoomLevel               = 15
    panRadiusMeters         = 4000
    earthRadiusMeters       = 6378137.0
    nominatimURL            = "https://nominatim.openstreetmap.org/search"
    websiteDefaultTimeout   = 15 * time.Second
    websiteMaxPagesDefault  = 6
    maxWebsiteResponseBytes = 2 * 1024 * 1024
    detailFetchMaxRetries   = 3
    emailFetchMaxRetries    = 3
)

var (
    nonDigitRegex = regexp.MustCompile(`\D`)
    emailRegex    = regexp.MustCompile(`(?i)[a-z0-9._%+\-]+@[a-z0-9.-]+\.ch`)
    keywordAnchors = []string{
        "kontakt",
        "über uns",
        "uber uns",
        "contact",
        "kommunikation",
        "communication",
        "our doctors",
        "our doctor",
        "unsere ärzte",
        "team",
        "arzt",
        "ärzte",
        "doctors",
        "impressum",
    }
)

type config struct {
    Headless           bool
    DBHost             string
    DBUser             string
    DBPass             string
    DBName             string
    Category           string
    DelayMin           time.Duration
    DelayMax           time.Duration
    GeocoderBaseURL    string
    GeocoderUserAgent  string
    GeocoderEmail      string
    GeocoderDelay      time.Duration
    DetailBatchLimit   int
    EmailBatchLimit    int
    WebsiteTimeout     time.Duration
    WebsiteMaxPages    int
    WebsiteConcurrency int
}

type coordinate struct {
    Lat float64
    Lng float64
}

type location struct {
    City      string
    Lat       float64
    Lng       float64
    HasCoords bool
}

type business struct {
    Name     string
    Category string
    Address  string
    Rating   float64
    Reviews  int
    URL      string
    Email    string
    Website  string
    Phone    string
}

type rawBusiness struct {
    Name        string `json:"name"`
    Category    string `json:"category"`
    Address     string `json:"address"`
    RatingText  string `json:"rating"`
    ReviewsText string `json:"reviews"`
    URL         string `json:"url"`
    Email       string `json:"email"`
    Website     string `json:"website"`
    Phone       string `json:"phone"`
}

type contactInfo struct {
    Website string `json:"website"`
    Email   string `json:"email"`
    Phone   string `json:"phone"`
}

type businessDetailTarget struct {
    ID       int64
    Name     string
    URL      string
    City     string
    Category string
}

type businessEmailTarget struct {
    ID   int64
    Name string
    URL  string
}

func main() {
    step1 := flag.Bool("step1", false, "Scrape Google Maps for every city")
    step2 := flag.Bool("step2", false, "Enrich stored businesses with website/phone data")
    step3 := flag.Bool("step3", false, "Visit business websites and hunt for Swiss emails")
    cityFilterRaw := flag.String("city", "", "Comma-separated list of cities to process (case-insensitive); leave empty for all")
    flag.Parse()

    cityFilter := parseCityFilter(*cityFilterRaw)

    if !*step1 && !*step2 && !*step3 {
        log.Println("No step selected. Use -step1, -step2 or -step3.")
        return
    }

    if *step1 {
        if err := runScraper(cityFilter); err != nil {
            log.Fatalf("step1 failed: %v", err)
        }
    }
    if *step2 {
        if err := runDetailEnrichment(cityFilter); err != nil {
            log.Fatalf("step2 failed: %v", err)
        }
    }
    if *step3 {
        if err := runWebsiteEmailExtraction(cityFilter); err != nil {
            log.Fatalf("step3 failed: %v", err)
        }
    }
}

func runScraper(cityFilter map[string]struct{}) error {
    _ = godotenv.Load()
    cfg, err := loadConfig()
    if err != nil {
        return err
    }

    db, err := sql.Open("mysql", cfg.dsn())
    if err != nil {
        return err
    }
    defer db.Close()

    ctx := context.Background()
    if err := db.PingContext(ctx); err != nil {
        return err
    }

    if err := ensureLocationsTable(ctx, db); err != nil {
        return err
    }
    if err := ensureBusinessesTable(ctx, db); err != nil {
        return err
    }
    if err := resetLocationProcessedFlags(ctx, db, cityFilter); err != nil {
        return err
    }

    locations, err := fetchLocations(ctx, db, cityFilter)
    if err != nil {
        return err
    }
    if len(locations) == 0 {
        fmt.Println("No cities found in locations table.")
        return nil
    }

    geocodeCache := make(map[string]coordinate)
    citiesWithResults := 0
    totalBusinesses := 0

    for idx, loc := range locations {
        query := fmt.Sprintf("%s in %s", cfg.Category, loc.City)
        log.Printf("[%d/%d] Scraping %q\n", idx+1, len(locations), query)

        center, err := resolveLocationCoordinate(ctx, cfg, db, geocodeCache, loc)
        if err != nil {
            log.Printf("   unable to geocode %s: %v\n", loc.City, err)
            continue
        }

        positions := generatePanPositions(center, panRadiusMeters)
        aggregated := make(map[string]business)

        for sweepIdx, pos := range positions {
            searchURL := buildSearchURL(query, pos, zoomLevel)
            log.Printf("   sweep %d/%d centered at %.5f, %.5f\n", sweepIdx+1, len(positions), pos.Lat, pos.Lng)

            results, err := scrapeGoogleMaps(cfg, searchURL)
            if err != nil {
                log.Printf("      error scraping sweep %d: %v\n", sweepIdx+1, err)
                continue
            }

            mergeBusinessSet(aggregated, results, cfg.Category)
            time.Sleep(cfg.randomDelay())
        }

        finalResults := flattenBusinessSet(aggregated)
        finalResults = filterExistingBusinesses(ctx, db, loc.City, finalResults)

        if len(finalResults) == 0 {
            fmt.Printf("No business cards found for %q after filtering.\n\n", query)
            continue
        }

        citiesWithResults++
        totalBusinesses += len(finalResults)

        printBusinesses(finalResults)
        if err := storeBusinesses(ctx, db, loc.City, cfg.Category, finalResults); err != nil {
            log.Printf("   error storing businesses for %q: %v\n", query, err)
        }
        if err := markLocationProcessed(ctx, db, loc.City); err != nil {
            log.Printf("   error marking %s processed: %v\n", loc.City, err)
        }
        time.Sleep(cfg.randomDelay())
    }

    log.Printf("Step1 summary: total cities=%d, cities with results=%d, businesses upserted=%d", len(locations), citiesWithResults, totalBusinesses)
    return nil
}

func runDetailEnrichment(cityFilter map[string]struct{}) error {
    _ = godotenv.Load()
    cfg, err := loadConfig()
    if err != nil {
        return err
    }

    db, err := sql.Open("mysql", cfg.dsn())
    if err != nil {
        return err
    }
    defer db.Close()

    ctx := context.Background()
    if err := db.PingContext(ctx); err != nil {
        return err
    }

    targets, err := fetchBusinessesNeedingDetails(ctx, db, cfg.DetailBatchLimit, cityFilter)
    if err != nil {
        return err
    }
    if len(targets) == 0 {
        fmt.Println("No businesses need website/telephone enrichment.")
        return nil
    }

    opts := append(chromedp.DefaultExecAllocatorOptions[:],
        chromedp.Flag("headless", cfg.Headless),
        chromedp.Flag("disable-gpu", true),
        chromedp.Flag("no-sandbox", true),
        chromedp.Flag("disable-dev-shm-usage", true),
        chromedp.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    )
    updatedCount := 0

    allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
    defer cancel()

    for idx, target := range targets {
        if strings.TrimSpace(target.URL) == "" {
            continue
        }

        website, email, phone, err := fetchContactInfoWithRetry(cfg, allocCtx, target.URL, detailFetchMaxRetries)
        if err != nil {
            log.Printf("[%d/%d] unable to fetch details for %s: %v\n", idx+1, len(targets), target.Name, err)
            continue
        }

        info := contactInfo{Website: website, Email: email, Phone: phone}
        if err := updateBusinessContacts(ctx, db, target.ID, info); err != nil {
            log.Printf("   error updating %s: %v\n", target.Name, err)
        } else if info.Website != "" || info.Email != "" || info.Phone != "" {
            updatedCount++
        }
        time.Sleep(cfg.randomDelay())
    }

    log.Printf("Step2 summary: targets=%d, updated=%d", len(targets), updatedCount)
    return nil
}

func runWebsiteEmailExtraction(cityFilter map[string]struct{}) error {
    _ = godotenv.Load()
    cfg, err := loadConfig()
    if err != nil {
        return err
    }

    db, err := sql.Open("mysql", cfg.dsn())
    if err != nil {
        return err
    }
    defer db.Close()

    ctx := context.Background()
    if err := db.PingContext(ctx); err != nil {
        return err
    }

    targets, err := fetchBusinessesNeedingEmails(ctx, db, cfg.EmailBatchLimit, cityFilter)
    if err != nil {
        return err
    }
    if len(targets) == 0 {
        fmt.Println("No businesses require website email extraction.")
        return nil
    }

    workerCount := cfg.WebsiteConcurrency
    if workerCount <= 0 {
        workerCount = 5
    }

    tasks := make(chan businessEmailTarget)
    var wg sync.WaitGroup
    var updatedCount int64

    for i := 0; i < workerCount; i++ {
        wg.Add(1)
        go func(workerID int) {
            defer wg.Done()
            for target := range tasks {
                if strings.TrimSpace(target.URL) == "" {
                    continue
                }

                email, err := huntWebsiteForEmailWithRetry(cfg, target.URL, emailFetchMaxRetries)
                if err != nil {
                    log.Printf("[worker %d] unable to extract email for %s: %v", workerID+1, target.Name, err)
                    continue
                }
                if email == "" {
                    continue
                }

                info := contactInfo{Email: email}
                if err := updateBusinessEmail(ctx, db, target.ID, info); err != nil {
                    log.Printf("[worker %d] error updating email for %s: %v", workerID+1, target.Name, err)
                } else {
                    atomic.AddInt64(&updatedCount, 1)
                }

                if delay := cfg.randomDelay(); delay > 0 {
                    time.Sleep(delay)
                }
            }
        }(i)
    }

    for _, target := range targets {
        tasks <- target
    }
    close(tasks)
    wg.Wait()

    log.Printf("Step3 summary: targets=%d, emails updated=%d", len(targets), updatedCount)
    return nil
}

func loadConfig() (config, error) {
    headless, err := parseHeadless(os.Getenv("HEADLESS"))
    if err != nil {
        return config{}, err
    }

    delayMin := parseDurationEnv("RANDOM_DELAY_MIN_MS", 1500)
    delayMax := parseDurationEnv("RANDOM_DELAY_MAX_MS", 4000)
    if delayMax < delayMin {
        delayMin, delayMax = delayMax, delayMin
    }

    geocoderDelay := parseDurationEnv("GEOCODER_DELAY_MS", 2000)
    geocoderBase := valueOrDefault(os.Getenv("GEOCODER_BASE_URL"), nominatimURL)
    geocoderUA := strings.TrimSpace(os.Getenv("GEOCODER_USER_AGENT"))
    if geocoderUA == "" {
        geocoderUA = "go-scraper-gmaps/1.0"
    }
    geocoderEmail := strings.TrimSpace(os.Getenv("GEOCODER_EMAIL"))

    cfg := config{
        Headless:           headless,
        DBHost:             valueOrDefault(os.Getenv("DB_HOST"), "127.0.0.1:3306"),
        DBUser:             valueOrDefault(os.Getenv("DB_USER"), "gmaps"),
        DBPass:             valueOrDefault(os.Getenv("DB_PASSWORD"), "mapmap"),
        DBName:             valueOrDefault(os.Getenv("DB_NAME"), "gmaps"),
        Category:           strings.TrimSpace(os.Getenv("BUSINESS_CATEGORY")),
        DelayMin:           delayMin,
        DelayMax:           delayMax,
        GeocoderBaseURL:    geocoderBase,
        GeocoderUserAgent:  geocoderUA,
        GeocoderEmail:      geocoderEmail,
        GeocoderDelay:      geocoderDelay,
        DetailBatchLimit:   parseLimitEnv("DETAIL_BATCH_LIMIT", 0),
        EmailBatchLimit:    parseLimitEnv("EMAIL_BATCH_LIMIT", 0),
        WebsiteTimeout:     parseDurationEnv("WEBSITE_FETCH_TIMEOUT_MS", int(websiteDefaultTimeout/time.Millisecond)),
        WebsiteMaxPages:    parseIntEnv("WEBSITE_MAX_PAGES", websiteMaxPagesDefault),
        WebsiteConcurrency: parseIntEnv("WEBSITE_CONCURRENCY", 5),
    }

    if cfg.Category == "" {
        return config{}, fmt.Errorf("BUSINESS_CATEGORY is required")
    }

    return cfg, nil
}

func (c config) dsn() string {
    return fmt.Sprintf("%s:%s@tcp(%s)/%s?parseTime=true&charset=utf8mb4&loc=Local",
        c.DBUser, c.DBPass, c.DBHost, c.DBName,
    )
}

func ensureLocationsTable(ctx context.Context, db *sql.DB) error {
    const ddl = `
CREATE TABLE IF NOT EXISTS locations (
  id INT AUTO_INCREMENT PRIMARY KEY,
  city VARCHAR(255) NOT NULL,
  processed TINYINT(1) NOT NULL DEFAULT 0,
  latitude DECIMAL(10,7) NULL,
  longitude DECIMAL(10,7) NULL,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_city (city)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
    _, err := db.ExecContext(ctx, ddl)
    return err
}

func ensureBusinessesTable(ctx context.Context, db *sql.DB) error {
    const ddl = `
CREATE TABLE IF NOT EXISTS businesses (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  city VARCHAR(255) NOT NULL,
  category VARCHAR(255) NOT NULL,
  name VARCHAR(255) NOT NULL,
  address VARCHAR(255) NULL,
  rating DECIMAL(4,2) NULL,
  reviews INT NULL,
  url TEXT NULL,
  email VARCHAR(255) NULL,
  website VARCHAR(255) NULL,
  telephone VARCHAR(50) NULL,
  scraped_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  UNIQUE KEY uniq_city_category_name (city, category, name)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;`
    _, err := db.ExecContext(ctx, ddl)
    return err
}

func fetchLocations(ctx context.Context, db *sql.DB, cityFilter map[string]struct{}) ([]location, error) {
    rows, err := db.QueryContext(ctx, `SELECT city, latitude, longitude FROM locations ORDER BY id ASC`)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var locations []location
    for rows.Next() {
        var loc location
        var lat, lng sql.NullFloat64
        if err := rows.Scan(&loc.City, &lat, &lng); err != nil {
            return nil, err
        }
        loc.City = strings.TrimSpace(loc.City)
        if lat.Valid && lng.Valid {
            loc.Lat = lat.Float64
            loc.Lng = lng.Float64
            loc.HasCoords = true
        }
        locations = append(locations, loc)
    }

    if err := rows.Err(); err != nil {
        return nil, err
    }

    if len(cityFilter) == 0 {
        return locations, nil
    }

    filtered := make([]location, 0, len(cityFilter))
    for _, loc := range locations {
        if _, ok := cityFilter[strings.ToLower(loc.City)]; ok {
            filtered = append(filtered, loc)
        }
    }
    return filtered, nil
}

func storeBusinesses(ctx context.Context, db *sql.DB, city, category string, results []business) error {
    if len(results) == 0 {
        return nil
    }

    const stmt = `
INSERT INTO businesses (city, category, name, address, rating, reviews, url, email, website, telephone, scraped_at)
VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
ON DUPLICATE KEY UPDATE
  address=VALUES(address),
  rating=VALUES(rating),
  reviews=VALUES(reviews),
  url=VALUES(url),
  email=VALUES(email),
  website=VALUES(website),
  telephone=VALUES(telephone),
  scraped_at=VALUES(scraped_at);`

    prepared, err := db.PrepareContext(ctx, stmt)
    if err != nil {
        return err
    }
    defer prepared.Close()

    now := time.Now()
    for _, b := range results {
        if strings.TrimSpace(b.Name) == "" {
            continue
        }

        if _, err := prepared.ExecContext(ctx,
            city,
            category,
            b.Name,
            nullString(b.Address),
            nullFloat64(b.Rating),
            nullInt(b.Reviews),
            nullString(b.URL),
            nullString(b.Email),
            nullString(b.Website),
            nullString(b.Phone),
            now,
        ); err != nil {
            return err
        }
    }
    return nil
}

func markLocationProcessed(ctx context.Context, db *sql.DB, city string) error {
    _, err := db.ExecContext(ctx, `UPDATE locations SET processed = 1 WHERE city = ?`, city)
    return err
}

func resetLocationProcessedFlags(ctx context.Context, db *sql.DB, cityFilter map[string]struct{}) error {
    if len(cityFilter) == 0 {
        _, err := db.ExecContext(ctx, `UPDATE locations SET processed = 0`)
        return err
    }
    placeholders := make([]string, 0, len(cityFilter))
    args := make([]interface{}, 0, len(cityFilter))
    for city := range cityFilter {
        placeholders = append(placeholders, "?")
        args = append(args, city)
    }
    query := fmt.Sprintf("UPDATE locations SET processed = 0 WHERE LOWER(city) IN (%s)", strings.Join(placeholders, ","))
    _, err := db.ExecContext(ctx, query, args...)
    return err
}

func updateLocationCoordinates(ctx context.Context, db *sql.DB, city string, coord coordinate) error {
    _, err := db.ExecContext(ctx, `UPDATE locations SET latitude = ?, longitude = ? WHERE city = ?`, coord.Lat, coord.Lng, city)
    return err
}

func fetchBusinessesNeedingDetails(ctx context.Context, db *sql.DB, limit int, cityFilter map[string]struct{}) ([]businessDetailTarget, error) {
    base := `
SELECT id, name, url, city, category
FROM businesses
WHERE (url IS NOT NULL AND url <> '')
  AND (
    website IS NULL OR website = ''
    OR telephone IS NULL OR telephone = ''
  )`
    var args []interface{}
    if len(cityFilter) > 0 {
        placeholders := make([]string, 0, len(cityFilter))
        for city := range cityFilter {
            placeholders = append(placeholders, "?")
            args = append(args, city)
        }
        base += fmt.Sprintf(" AND LOWER(city) IN (%s)", strings.Join(placeholders, ","))
    }
    base += " ORDER BY scraped_at DESC"
    if limit > 0 {
        base += fmt.Sprintf(" LIMIT %d", limit)
    }

    rows, err := db.QueryContext(ctx, base, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var targets []businessDetailTarget
    for rows.Next() {
        var t businessDetailTarget
        if err := rows.Scan(&t.ID, &t.Name, &t.URL, &t.City, &t.Category); err != nil {
            return nil, err
        }
        targets = append(targets, t)
    }
    return targets, rows.Err()
}

func updateBusinessContacts(ctx context.Context, db *sql.DB, id int64, info contactInfo) error {
    if info.Website == "" && info.Email == "" && info.Phone == "" {
        return nil
    }

    query := `
UPDATE businesses
SET
  website = CASE WHEN ? <> '' THEN ? ELSE website END,
  email = CASE WHEN ? <> '' THEN ? ELSE email END,
  telephone = CASE WHEN ? <> '' THEN ? ELSE telephone END,
  scraped_at = ?
WHERE id = ?`

    _, err := db.ExecContext(ctx, query,
        info.Website, info.Website,
        info.Email, info.Email,
        info.Phone, info.Phone,
        time.Now(),
        id,
    )
    return err
}

func updateBusinessEmail(ctx context.Context, db *sql.DB, id int64, info contactInfo) error {
    email := strings.TrimSpace(info.Email)
    if email == "" {
        return nil
    }
    _, err := db.ExecContext(ctx, `
UPDATE businesses
SET email = ?, scraped_at = ?
WHERE id = ?`, email, time.Now(), id)
    return err
}

func fetchBusinessesNeedingEmails(ctx context.Context, db *sql.DB, limit int, cityFilter map[string]struct{}) ([]businessEmailTarget, error) {
    base := `
SELECT id, name, website
FROM businesses
WHERE (website IS NOT NULL AND website <> '')
  AND (email IS NULL OR email = '')`
    var args []interface{}
    if len(cityFilter) > 0 {
        placeholders := make([]string, 0, len(cityFilter))
        for city := range(cityFilter) {
            placeholders = append(placeholders, "?")
            args = append(args, city)
        }
        base += fmt.Sprintf(" AND LOWER(city) IN (%s)", strings.Join(placeholders, ","))
    }
    base += " ORDER BY scraped_at DESC"
    if limit > 0 {
        base += fmt.Sprintf(" LIMIT %d", limit)
    }

    rows, err := db.QueryContext(ctx, base, args...)
    if err != nil {
        return nil, err
    }
    defer rows.Close()

    var targets []businessEmailTarget
    for rows.Next() {
        var t businessEmailTarget
        if err := rows.Scan(&t.ID, &t.Name, &t.URL); err != nil {
            return nil, err
        }
        targets = append(targets, t)
    }
    return targets, rows.Err()
}

func printBusinesses(results []business) {
    for idx, b := range results {
        fmt.Printf("%d. %s\n", idx+1, b.Name)
        if b.Category != "" {
            fmt.Printf("   Category: %s\n", b.Category)
        }
        if b.Address != "" {
            fmt.Printf("   Address: %s\n", b.Address)
        }
        if b.Rating > 0 {
            if b.Reviews > 0 {
                fmt.Printf("   Rating: %.1f (%d reviews)\n", b.Rating, b.Reviews)
            } else {
                fmt.Printf("   Rating: %.1f\n", b.Rating)
            }
        }
        if b.URL != "" {
            fmt.Printf("   Maps URL: %s\n", b.URL)
        }
        if b.Website != "" {
            fmt.Printf("   Website: %s\n", b.Website)
        }
        if b.Phone != "" {
            fmt.Printf("   Phone: %s\n", b.Phone)
        }
        if b.Email != "" {
            fmt.Printf("   Email: %s\n", b.Email)
        }
        fmt.Println()
    }
    fmt.Printf("Total unique businesses: %d\n\n", len(results))
}

func scrapeGoogleMaps(cfg config, searchURL string) ([]business, error) {
    opts := append(chromedp.DefaultExecAllocatorOptions[:],
        chromedp.Flag("headless", cfg.Headless),
        chromedp.Flag("disable-gpu", true),
        chromedp.Flag("no-sandbox", true),
        chromedp.Flag("disable-dev-shm-usage", true),
        chromedp.UserAgent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/122.0.0.0 Safari/537.36"),
    )
    allocCtx, cancel := chromedp.NewExecAllocator(context.Background(), opts...)
    defer cancel()

    ctx, cancel := chromedp.NewContext(allocCtx)
    defer cancel()

    ctx, cancel = context.WithTimeout(ctx, 60*time.Second)
    defer cancel()

    var rawJSON string
    tasks := chromedp.Tasks{
        chromedp.Navigate(searchURL),
        chromedp.Sleep(cfg.randomDelay()),
        chromedp.ActionFunc(func(ctx context.Context) error {
            return chromedp.Evaluate(consentScript, nil).Do(ctx)
        }),
        chromedp.WaitVisible(`div[role="feed"]`, chromedp.ByQuery),
        chromedp.Sleep(cfg.randomDelay()),
        chromedp.ActionFunc(func(ctx context.Context) error {
            for i := 0; i < scrollIterations; i++ {
                if err := chromedp.Evaluate(scrollScript, nil).Do(ctx); err != nil {
                    return err
                }
                time.Sleep(cfg.randomDelay())
            }
            return nil
        }),
        chromedp.Evaluate(scrapeScript, &rawJSON),
    }

    if err := chromedp.Run(ctx, tasks); err != nil {
        return nil, err
    }

    if strings.TrimSpace(rawJSON) == "" {
        return nil, fmt.Errorf("no business data returned")
    }

    var raw []rawBusiness
    if err := json.Unmarshal([]byte(rawJSON), &raw); err != nil {
        return nil, err
    }

    results := normalizeBusinesses(raw)
    results = enrichBusinessContacts(cfg, allocCtx, results)
    return results, nil
}

func buildSearchURL(query string, center coordinate, zoom int) string {
    base := searchBaseURL + url.QueryEscape(query)
    if center == (coordinate{}) {
        return base
    }
    return fmt.Sprintf("%s/@%f,%f,%dz", base, center.Lat, center.Lng, zoom)
}

func normalizeBusinesses(raw []rawBusiness) []business {
    seen := make(map[string]struct{})
    results := make([]business, 0, len(raw))

    for _, item := range raw {
        name := strings.TrimSpace(item.Name)
        if name == "" {
            continue
        }
        b := business{
            Name:     name,
            Category: strings.TrimSpace(item.Category),
            Address:  strings.TrimSpace(item.Address),
            Rating:   parseRating(item.RatingText),
            Reviews:  parseReviews(item.ReviewsText),
            URL:      strings.TrimSpace(item.URL),
            Email:    strings.TrimSpace(item.Email),
            Website:  strings.TrimSpace(item.Website),
            Phone:    strings.TrimSpace(item.Phone),
        }
        key := businessKey(b)
        if key == "" {
            continue
        }
        if _, dup := seen[key]; dup {
            continue
        }
        seen[key] = struct{}{}
        results = append(results, b)
    }

    return results
}

func mergeBusinessSet(target map[string]business, incoming []business, fallbackCategory string) {
    for _, item := range incoming {
        if strings.TrimSpace(item.Name) == "" {
            continue
        }
        if item.Category == "" {
            item.Category = fallbackCategory
        }
        key := businessKey(item)
        if key == "" {
            continue
        }
        if existing, ok := target[key]; ok {
            target[key] = mergeBusiness(existing, item)
            continue
        }
        target[key] = item
    }
}

func mergeBusiness(a, b business) business {
    if a.Address == "" {
        a.Address = b.Address
    }
    if a.Category == "" {
        a.Category = b.Category
    }
    if a.Rating == 0 && b.Rating != 0 {
        a.Rating = b.Rating
    }
    if a.Reviews == 0 && b.Reviews != 0 {
        a.Reviews = b.Reviews
    }
    if a.URL == "" {
        a.URL = b.URL
    }
    if a.Email == "" {
        a.Email = b.Email
    }
    if a.Website == "" {
        a.Website = b.Website
    }
    if a.Phone == "" {
        a.Phone = b.Phone
    }
    return a
}

func flattenBusinessSet(m map[string]business) []business {
    items := make([]business, 0, len(m))
    for _, b := range m {
        items = append(items, b)
    }
    sort.Slice(items, func(i, j int) bool {
        return strings.ToLower(items[i].Name) < strings.ToLower(items[j].Name)
    })
    return items
}

func filterExistingBusinesses(ctx context.Context, db *sql.DB, city string, incoming []business) []business {
    rows, err := db.QueryContext(ctx, `SELECT name, IFNULL(address,''), IFNULL(url,''), IFNULL(category,'') FROM businesses WHERE city = ?`, city)
    if err != nil {
        log.Printf("   warning: unable to load existing businesses for %s: %v\n", city, err)
        return incoming
    }
    defer rows.Close()
    existing := make(map[string]struct{})
    for rows.Next() {
        var name, address, url, category string
        if err := rows.Scan(&name, &address, &url, &category); err != nil {
            continue
        }
        key := normalizeBusinessKey(name, address, url, category)
        if key != "" {
            existing[key] = struct{}{}
        }
    }

    var filtered []business
    for _, b := range incoming {
        key := businessKey(b)
        if key == "" {
            continue
        }
        if _, dup := existing[key]; dup {
            continue
        }
        existing[key] = struct{}{}
        filtered = append(filtered, b)
    }
    return filtered
}

func businessKey(b business) string {
    return normalizeBusinessKey(b.Name, b.Address, b.URL, b.Category)
}

func normalizeBusinessKey(name, address, url, category string) string {
    name = strings.ToLower(strings.TrimSpace(name))
    if name == "" {
        return ""
    }
    address = strings.ToLower(strings.TrimSpace(address))
    if address == "" {
        address = strings.ToLower(strings.TrimSpace(url))
    }
    cat := strings.ToLower(strings.TrimSpace(category))
    return name + "|" + address + "|" + cat
}

func parseRating(value string) float64 {
    value = strings.TrimSpace(value)
    if value == "" {
        return 0
    }
    value = strings.ReplaceAll(value, ",", ".")
    r, err := strconv.ParseFloat(value, 64)
    if err != nil {
        return 0
    }
    return r
}

func parseReviews(value string) int {
    cleaned := nonDigitRegex.ReplaceAllString(value, "")
    if cleaned == "" {
        return 0
    }
    n, err := strconv.Atoi(cleaned)
    if err != nil {
        return 0
    }
    return n
}

func parseHeadless(value string) (bool, error) {
    value = strings.TrimSpace(value)
    if value == "" {
        return true, nil
    }
    b, err := strconv.ParseBool(value)
    if err != nil {
        return false, fmt.Errorf("invalid HEADLESS value: %w", err)
    }
    return b, nil
}

func valueOrDefault(value, fallback string) string {
    if strings.TrimSpace(value) == "" {
        return fallback
    }
    return strings.TrimSpace(value)
}

func parseDurationEnv(key string, defaultMs int) time.Duration {
    value := strings.TrimSpace(os.Getenv(key))
    if value == "" {
        return time.Duration(defaultMs) * time.Millisecond
    }
    ms, err := strconv.Atoi(value)
    if err != nil || ms < 0 {
        return time.Duration(defaultMs) * time.Millisecond
    }
    return time.Duration(ms) * time.Millisecond
}

func parseIntEnv(key string, fallback int) int {
    value := strings.TrimSpace(os.Getenv(key))
    if value == "" {
        return fallback
    }
    n, err := strconv.Atoi(value)
    if err != nil || n <= 0 {
        return fallback
    }
    return n
}

func parseLimitEnv(key string, fallback int) int {
    value := strings.TrimSpace(os.Getenv(key))
    if value == "" {
        return fallback
    }
    n, err := strconv.Atoi(value)
    if err != nil {
        return fallback
    }
    if n < 0 {
        return fallback
    }
    return n
}

func (c config) randomDelay() time.Duration {
    if c.DelayMin <= 0 && c.DelayMax <= 0 {
        return 0
    }
    if c.DelayMax <= c.DelayMin {
        return c.DelayMin
    }
    delta := c.DelayMax - c.DelayMin
    return c.DelayMin + time.Duration(rand.Int63n(int64(delta)))
}

func (c config) geocoderDelayDuration() time.Duration {
    if c.GeocoderDelay <= 0 {
        return 0
    }
    jitter := time.Duration(rand.Int63n(int64(c.GeocoderDelay/2 + 1)))
    return c.GeocoderDelay + jitter
}

func nullString(value string) sql.NullString {
    value = strings.TrimSpace(value)
    if value == "" {
        return sql.NullString{}
    }
    return sql.NullString{String: value, Valid: true}
}

func nullFloat64(value float64) sql.NullFloat64 {
    if value == 0 {
        return sql.NullFloat64{}
    }
    return sql.NullFloat64{Float64: value, Valid: true}
}

func nullInt(value int) sql.NullInt64 {
    if value == 0 {
        return sql.NullInt64{}
    }
    return sql.NullInt64{Int64: int64(value), Valid: true}
}

func parseCityFilter(raw string) map[string]struct{} {
    result := make(map[string]struct{})
    raw = strings.TrimSpace(raw)
    if raw == "" {
        return result
    }
    for _, city := range strings.Split(raw, ",") {
        city = strings.TrimSpace(city)
        if city == "" {
            continue
        }
        result[strings.ToLower(city)] = struct{}{}
    }
    return result
}

func resolveLocationCoordinate(ctx context.Context, cfg config, db *sql.DB, cache map[string]coordinate, loc location) (coordinate, error) {
    lower := strings.ToLower(loc.City)
    if loc.HasCoords {
        coord := coordinate{Lat: loc.Lat, Lng: loc.Lng}
        cache[lower] = coord
        return coord, nil
    }
    if coord, ok := cache[lower]; ok {
        return coord, nil
    }
    coord, err := geocodeCity(ctx, cfg, loc.City)
    if err != nil {
        return coordinate{}, err
    }
    cache[lower] = coord
    if err := updateLocationCoordinates(ctx, db, loc.City, coord); err != nil {
        log.Printf("   warning: unable to persist coordinates for %s: %v\n", loc.City, err)
    }
    return coord, nil
}

func geocodeCity(ctx context.Context, cfg config, city string) (coordinate, error) {
    if delay := cfg.geocoderDelayDuration(); delay > 0 {
        time.Sleep(delay)
    }

    params := url.Values{}
    params.Set("format", "json")
    params.Set("limit", "1")
    params.Set("q", city)

    base := strings.TrimRight(cfg.GeocoderBaseURL, "?&")
    if base == "" {
        base = nominatimURL
    }
    endpoint := fmt.Sprintf("%s?%s", base, params.Encode())

    req, err := http.NewRequestWithContext(ctx, http.MethodGet, endpoint, nil)
    if err != nil {
        return coordinate{}, err
    }
    req.Header.Set("User-Agent", cfg.GeocoderUserAgent)
    req.Header.Set("Accept-Language", "en")
    if cfg.GeocoderEmail != "" {
        req.Header.Set("From", cfg.GeocoderEmail)
    }

    resp, err := http.DefaultClient.Do(req)
    if err != nil {
        return coordinate{}, err
    }
    defer resp.Body.Close()

    if resp.StatusCode != http.StatusOK {
        if resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode == http.StatusForbidden {
            return coordinate{}, fmt.Errorf("geocoder responded with status %d (check GEOCODER_* settings)", resp.StatusCode)
        }
        return coordinate{}, fmt.Errorf("geocoder responded with status %d", resp.StatusCode)
    }

    var payload []struct {
        Lat string `json:"lat"`
        Lon string `json:"lon"`
    }
    if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
        return coordinate{}, err
    }
    if len(payload) == 0 {
        return coordinate{}, fmt.Errorf("no geocoding results for %s", city)
    }
    lat, err := strconv.ParseFloat(payload[0].Lat, 64)
    if err != nil {
        return coordinate{}, err
    }
    lng, err := strconv.ParseFloat(payload[0].Lon, 64)
    if err != nil {
        return coordinate{}, err
    }
    return coordinate{Lat: lat, Lng: lng}, nil
}

func generatePanPositions(center coordinate, radius float64) []coordinate {
    positions := []coordinate{center}
    bearings := []float64{0, 45, 90, 135, 180, 225, 270, 315}
    for _, bearing := range bearings {
        positions = append(positions, offsetCoordinate(center, radius, bearing))
    }
    return positions
}

func offsetCoordinate(origin coordinate, distanceMeters, bearingDeg float64) coordinate {
    if distanceMeters == 0 {
        return origin
    }
    bearingRad := degreesToRadians(bearingDeg)
    latRad := degreesToRadians(origin.Lat)
    lngRad := degreesToRadians(origin.Lng)
    angDist := distanceMeters / earthRadiusMeters

    lat2 := math.Asin(math.Sin(latRad)*math.Cos(angDist) + math.Cos(latRad)*math.Sin(angDist)*math.Cos(bearingRad))
    lng2 := lngRad + math.Atan2(math.Sin(bearingRad)*math.Sin(angDist)*math.Cos(latRad), math.Cos(angDist)-math.Sin(latRad)*math.Sin(lat2))

    return coordinate{Lat: radiansToDegrees(lat2), Lng: normalizeLongitude(radiansToDegrees(lng2))}
}

func degreesToRadians(value float64) float64 { return value * math.Pi / 180 }
func radiansToDegrees(value float64) float64 { return value * 180 / math.Pi }

func normalizeLongitude(lng float64) float64 {
    for lng > 180 {
        lng -= 360
    }
    for lng < -180 {
        lng += 360
    }
    return lng
}

func enrichBusinessContacts(cfg config, allocCtx context.Context, businesses []business) []business {
    for idx := range businesses {
        if strings.TrimSpace(businesses[idx].URL) == "" {
            continue
        }

        website, email, phone, err := fetchContactInfo(cfg, allocCtx, businesses[idx].URL)
        if err != nil {
            log.Printf("   unable to fetch contact info for %s: %v\n", businesses[idx].Name, err)
            continue
        }

        if website != "" {
            businesses[idx].Website = normalizeWebsiteURL(website)
        }
        if email != "" {
            businesses[idx].Email = normalizeEmail(email)
        }
        if phone != "" {
            businesses[idx].Phone = normalizePhone(phone)
        }

        time.Sleep(cfg.randomDelay())
    }

    return businesses
}

func fetchContactInfoWithRetry(cfg config, allocCtx context.Context, placeURL string, retries int) (string, string, string, error) {
    if retries < 1 {
        retries = 1
    }
    var website, email, phone string
    var err error
    for attempt := 0; attempt < retries; attempt++ {
        website, email, phone, err = fetchContactInfo(cfg, allocCtx, placeURL)
        if err == nil {
            return website, email, phone, nil
        }
        time.Sleep(time.Second * time.Duration(attempt+1))
    }
    return "", "", "", err
}

func fetchContactInfo(cfg config, allocCtx context.Context, placeURL string) (string, string, string, error) {
    if strings.TrimSpace(placeURL) == "" {
        return "", "", "", nil
    }

    ctx, cancel := chromedp.NewContext(allocCtx)
    defer cancel()

    ctx, timeoutCancel := context.WithTimeout(ctx, 25*time.Second)
    defer timeoutCancel()

    var payload string
    tasks := chromedp.Tasks{
        chromedp.Navigate(placeURL),
        chromedp.Sleep(cfg.randomDelay()),
        chromedp.ActionFunc(func(ctx context.Context) error {
            return chromedp.Evaluate(consentScript, nil).Do(ctx)
        }),
        chromedp.WaitVisible(`h1.DUwDvf`, chromedp.ByQuery),
        chromedp.Sleep(cfg.randomDelay()),
        chromedp.Evaluate(contactScript, &payload),
    }

    if err := chromedp.Run(ctx, tasks); err != nil {
        return "", "", "", err
    }

    if strings.TrimSpace(payload) == "" {
        return "", "", "", nil
    }

    var info contactInfo
    if err := json.Unmarshal([]byte(payload), &info); err != nil {
        return "", "", "", err
    }

    return info.Website, info.Email, info.Phone, nil
}

func huntWebsiteForEmailWithRetry(cfg config, rawURL string, retries int) (string, error) {
    if retries < 1 {
        retries = 1
    }
    var email string
    var err error
    for attempt := 0; attempt < retries; attempt++ {
        email, err = huntWebsiteForEmail(cfg, rawURL)
        if err == nil {
            return email, nil
        }
        time.Sleep(time.Second * time.Duration(attempt+1))
    }
    return "", err
}

func huntWebsiteForEmail(cfg config, rawURL string) (string, error) {
    startURL := ensureURLHasScheme(normalizeWebsiteURL(rawURL))
    base, err := url.Parse(startURL)
    if err != nil || base.Host == "" {
        return "", fmt.Errorf("invalid website URL %q", rawURL)
    }

    queue := []string{startURL}
    visited := make(map[string]struct{})
    pages := 0
    maxPages := cfg.WebsiteMaxPages
    if maxPages <= 0 {
        maxPages = websiteMaxPagesDefault
    }

    for len(queue) > 0 && pages < maxPages {
        current := queue[0]
        queue = queue[1:]
        if _, seen := visited[current]; seen {
            continue
        }
        visited[current] = struct{}{}

        email, links, err := parsePageForEmail(cfg, current, base)
        if err != nil {
            log.Printf("   warning: %v\n", err)
            continue
        }
        pages++
        if email != "" {
            return email, nil
        }
        queue = append(queue, links...)
    }

    return "", nil
}

func parsePageForEmail(cfg config, pageURL string, root *url.URL) (string, []string, error) {
    doc, rawHTML, err := fetchHTMLDocument(cfg, pageURL)
    if err != nil {
        return "", nil, err
    }

    if email := extractEmailFromHTML(rawHTML); email != "" {
        return email, nil, nil
    }
    if email := extractEmailFromDocument(doc); email != "" {
        return email, nil, nil
    }

    links := collectCandidateLinks(doc, pageURL, root)
    return "", links, nil
}

func fetchHTMLDocument(cfg config, target string) (*goquery.Document, string, error) {
    req, err := http.NewRequest(http.MethodGet, target, nil)
    if err != nil {
        return nil, "", err
    }
    req.Header.Set("User-Agent", cfg.GeocoderUserAgent)
    req.Header.Set("Accept", "text/html,application/xhtml+xml")
    req.Header.Set("Accept-Language", "en")

    timeout := cfg.WebsiteTimeout
    if timeout <= 0 {
        timeout = websiteDefaultTimeout
    }
    client := &http.Client{Timeout: timeout}

    resp, err := client.Do(req)
    if err != nil {
        return nil, "", err
    }
    defer resp.Body.Close()

    if resp.StatusCode >= 400 {
        return nil, "", fmt.Errorf("website %s responded with status %d", target, resp.StatusCode)
    }

    limited := io.LimitReader(resp.Body, maxWebsiteResponseBytes)
    body, err := io.ReadAll(limited)
    if err != nil {
        return nil, "", err
    }

    doc, err := goquery.NewDocumentFromReader(bytes.NewReader(body))
    if err != nil {
        return nil, "", err
    }

    return doc, string(body), nil
}

func extractEmailFromHTML(html string) string {
    emails := emailRegex.FindAllString(html, -1)
    for _, e := range emails {
        if sanitized := sanitizeEmail(e); sanitized != "" {
            return sanitized
        }
    }
    return ""
}

func extractEmailFromDocument(doc *goquery.Document) string {
    email := ""
    doc.Find("a[href]").EachWithBreak(func(i int, sel *goquery.Selection) bool {
        href, exists := sel.Attr("href")
        if !exists {
            return true
        }
        href = strings.TrimSpace(href)
        if strings.HasPrefix(strings.ToLower(href), "mailto:") {
            em := strings.TrimPrefix(href, "mailto:")
            if idx := strings.Index(em, "?"); idx != -1 {
                em = em[:idx]
            }
            em = sanitizeEmail(em)
            if em != "" {
                email = em
                return false
            }
        }
        return true
    })
    return email
}

func collectCandidateLinks(doc *goquery.Document, pageURL string, root *url.URL) []string {
    var links []string
    base, _ := url.Parse(pageURL)
    if base == nil {
        base = root
    }

    doc.Find("a[href]").Each(func(i int, sel *goquery.Selection) {
        href, _ := sel.Attr("href")
        href = strings.TrimSpace(href)
        if href == "" || strings.HasPrefix(strings.ToLower(href), "mailto:") {
            return
        }
        abs := resolveLink(base, href)
        if abs == "" {
            return
        }
        parsed, err := url.Parse(abs)
        if err != nil {
            return
        }
        if !sameHost(root, parsed) {
            return
        }
        text := strings.ToLower(strings.TrimSpace(sel.Text()))
        if shouldPrioritizeLink(text, strings.ToLower(abs)) {
            links = append(links, abs)
        }
    })

    if len(links) == 0 {
        fallback := []string{}
        doc.Find("a[href]").EachWithBreak(func(i int, sel *goquery.Selection) bool {
            href, _ := sel.Attr("href")
            href = strings.TrimSpace(href)
            if href == "" || strings.HasPrefix(strings.ToLower(href), "mailto:") {
                return true
            }
            abs := resolveLink(base, href)
            if abs == "" {
                return true
            }
            parsed, err := url.Parse(abs)
            if err != nil || !sameHost(root, parsed) {
                return true
            }
            fallback = append(fallback, abs)
            return len(fallback) < 2
        })
        links = append(links, fallback...)
    }

    return links
}

func resolveLink(base *url.URL, href string) string {
    if base == nil {
        return href
    }
    parsed, err := base.Parse(href)
    if err != nil {
        return ""
    }
    parsed.Fragment = ""
    return parsed.String()
}

func shouldPrioritizeLink(text, href string) bool {
    combined := strings.ToLower(text + " " + href)
    for _, keyword := range keywordAnchors {
        if strings.Contains(combined, keyword) {
            return true
        }
    }
    return false
}

func sameHost(a, b *url.URL) bool {
    if a == nil || b == nil {
        return false
    }
    return strings.EqualFold(a.Hostname(), b.Hostname())
}

func sanitizeEmail(raw string) string {
    clean := strings.TrimSpace(raw)
    clean = strings.Trim(clean, "<>()[]{}.,;:\"'`“”’“”")
    clean = strings.ReplaceAll(clean, " ", "")
    if strings.Contains(clean, " ") {
        clean = strings.Split(clean, " ")[0]
    }
    clean = strings.Trim(clean, "<>()[]{}.,;:\"'`“”’“”")
    clean = strings.TrimPrefix(clean, "mailto:")
    if idx := strings.Index(clean, "?"); idx != -1 {
        clean = clean[:idx]
    }
    clean = strings.TrimSpace(clean)
    if decoded, err := url.QueryUnescape(clean); err == nil {
        clean = decoded
    }
    clean = strings.ReplaceAll(clean, "%20", "")
    match := emailRegex.FindString(clean)
    if match == "" {
        return ""
    }
    match = strings.ToLower(match)
    if !hasMXRecords(match) {
        return ""
    }
    return match
}

func normalizeWebsiteURL(raw string) string {
    raw = strings.TrimSpace(raw)
    if raw == "" {
        return ""
    }
    if strings.HasPrefix(raw, "https://www.google.com/url?") {
        if parsed, err := url.Parse(raw); err == nil {
            if target := parsed.Query().Get("q"); target != "" {
                raw = target
            }
        }
    }
    return raw
}

func normalizeEmail(raw string) string {
    raw = sanitizeEmail(raw)
    return raw
}

func normalizePhone(raw string) string {
    raw = strings.TrimSpace(raw)
    if raw == "" {
        return ""
    }
    lower := strings.ToLower(raw)
    if strings.HasPrefix(lower, "tel:") {
        raw = raw[4:]
    }
    return strings.TrimSpace(raw)
}

func ensureURLHasScheme(raw string) string {
    if strings.HasPrefix(strings.ToLower(raw), "http") {
        return raw
    }
    return "https://" + strings.TrimLeft(raw, "/")
}

func hasMXRecords(email string) bool {
    parts := strings.Split(email, "@")
    if len(parts) != 2 {
        return false
    }
    domain := strings.TrimSpace(parts[1])
    if domain == "" {
        return false
    }

    msg := new(dns.Msg)
    msg.SetQuestion(dns.Fqdn(domain), dns.TypeMX)
    msg.RecursionDesired = true

    client := new(dns.Client)
    servers := []string{"8.8.8.8:53", "1.1.1.1:53"}
    for _, server := range servers {
        if resp, _, err := client.Exchange(msg, server); err == nil {
            if resp != nil && resp.Rcode == dns.RcodeSuccess && len(resp.Answer) > 0 {
                return true
            }
        }
    }
    return false
}

const consentScript = `(function () {
  const selectors = [
    'button[aria-label="Accept all"]',
    'button[aria-label="I agree"]',
    'button[aria-label="Alles akzeptieren"]',
    'button.VfPpkd-LgbsSe-OWXEXe-k8QpJ'
  ];
  for (const sel of selectors) {
    const btn = document.querySelector(sel);
    if (btn) {
      btn.click();
      return true;
    }
  }
  return false;
})();`

const scrollScript = `(function () {
  const feed = document.querySelector('div[role="feed"]');
  if (feed) {
    feed.scrollBy(0, feed.offsetHeight);
  }
})();`

const scrapeScript = `(function () {
  const cards = Array.from(document.querySelectorAll('div.Nv2PK'));
  const pickText = (root, selector) => {
    if (!root) {
      return '';
    }
    const node = root.querySelector(selector);
    return node ? node.textContent.trim() : '';
  };
  return JSON.stringify(cards.map(card => {
    const link = card.querySelector('a.hfpxzc');
    const infoLines = card.querySelectorAll('.W4Efsd span');
    const addressNode = card.querySelector('.W4Efsd span:last-child');
    return {
      name: pickText(card, '.qBF1Pd'),
      category: infoLines.length ? infoLines[0].textContent.trim() : '',
      address: addressNode ? addressNode.textContent.trim() : '',
      rating: pickText(card, '.MW4etd'),
      reviews: pickText(card, '.UY7F9'),
      url: link ? link.href : '',
      email: '',
      website: '',
      phone: ''
    };
  }));
})();`

const contactScript = `(function () {
  const websiteSelectors = [
    'a[data-item-id="authority"]',
    'a[data-item-id="website"]',
    'a[aria-label="Website"]',
    'a[aria-label="Website:"]',
    'a[href^="https://www.google.com/url?"][aria-label*="Website"]'
  ];
  let website = '';
  for (const sel of websiteSelectors) {
    const node = document.querySelector(sel);
    if (node) {
      website = node.href || node.getAttribute('href') || '';
      break;
    }
  }
  const emailNode = document.querySelector('a[href^="mailto:"], button[data-item-id="mail"], div[data-item-id="email"], span[data-item-id="email"]');
  let email = '';
  if (emailNode) {
    if (emailNode.href) {
      email = emailNode.href;
    } else if (emailNode.getAttribute && emailNode.getAttribute('href')) {
      email = emailNode.getAttribute('href');
    } else {
      email = emailNode.textContent ? emailNode.textContent.trim() : '';
    }
  }
  const mailLink = document.querySelector('a[href^="mailto:"]');
  if (!email && mailLink) {
    email = mailLink.href || '';
  }
  const phoneSelectors = [
    'button[data-item-id^="phone:tel"]',
    'a[href^="tel:"]',
    'div[data-item-id^="phone"] span',
    'button[aria-label^="Phone:"] span',
    'div[aria-label^="Phone:"] span'
  ];
  let phone = '';
  for (const sel of phoneSelectors) {
    const node = document.querySelector(sel);
    if (node) {
      phone = node.href || node.textContent || '';
      if (phone) break;
    }
  }
  const telLink = document.querySelector('a[href^="tel:"]');
  if (!phone && telLink) {
    phone = telLink.href || '';
  }
  return JSON.stringify({ website: website || '', email: email || '', phone: phone || '' });
})();`
