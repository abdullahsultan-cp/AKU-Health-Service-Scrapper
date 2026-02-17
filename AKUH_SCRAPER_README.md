# AKUH Department/Service Page Scraper

A comprehensive Python scraper for extracting structured data from Aga Khan University Hospital Pakistan's department and service pages.

## Features

- **6 Page Type Classification**: Automatically identifies page structure (standard, simple, parent_overview, multi_specialty, structured, service_complex)
- **Comprehensive Data Extraction**:
  - Page titles and breadcrumbs
  - Body content with word count
  - Faculty links with specialty information
  - Appointment request sections
  - Subsection links (for parent pages)
  - External links classification
  - Subheading detection
  - Collapsible section detection

- **Multiple Output Formats**:
  - JSON (full detailed data)
  - CSV (summary table)

- **Error Handling**: Graceful handling of network errors, 404s, and timeouts with detailed logging

## Installation

```bash
pip install requests beautifulsoup4
```

## Usage

### 1. Prepare URLs

Create a `health-services-links.txt` file with one URL per line:

```
https://hospitals.aku.edu/pakistan/Health-Services/department-of-anaesthesiology/Pages/default.aspx
https://hospitals.aku.edu/pakistan/Health-Services/department-of-dentistry/Pages/default.aspx
# Add more URLs...
```

### 2. Run the Scraper

```bash
python akuh_scraper.py
```

### 3. Output Files

- **akuh_scraping_results.json**: Full detailed data for all pages
- **akuh_scraping_summary.csv**: Quick reference table with key metrics

## Output Schema

### JSON Structure

```json
{
  "scrape_metadata": {
    "date": "ISO timestamp",
    "total_pages": 41,
    "pages_scraped": 40,
    "pages_failed": 1,
    "failed_urls": ["..."]
  },
  "pages": [
    {
      "url": "string",
      "page_title": "string",
      "breadcrumb": "string",
      "has_h1_title": boolean,
      "body_content": {
        "main_paragraphs": "string",
        "word_count": integer,
        "has_subheadings": boolean,
        "subheading_tags": ["h2", "h3", "h4"],
        "has_bullet_lists": boolean,
        "has_collapsible_sections": boolean
      },
      "subsection_links": {
        "present": boolean,
        "count": integer,
        "links": [{"text": "string", "url": "string"}]
      },
      "faculty_links": {
        "count": integer,
        "pattern": "single | multiple | none",
        "links": [
          {
            "text": "string",
            "url": "string",
            "specialty": "string"
          }
        ]
      },
      "appointment_section": {
        "present": boolean,
        "components": {
          "heading": "string",
          "click_here_link": {
            "present": boolean,
            "text": "string",
            "url": "string"
          },
          "phone_number": "string",
          "family_hifazat": {
            "main_link_present": boolean,
            "google_play_button": boolean,
            "app_store_button": boolean
          }
        }
      },
      "external_links": [
        {
          "text": "string",
          "url": "string",
          "type": "external | internal | document"
        }
      ],
      "page_type_classification": "standard | simple | parent_overview | multi_specialty | structured | service_complex"
    }
  ]
}
```

### CSV Columns

- `url`: Page URL
- `page_title`: Extracted page title
- `has_h1`: Whether page has H1 tag
- `page_type`: Classification (standard, simple, parent_overview, etc.)
- `body_word_count`: Word count of main content
- `faculty_link_count`: Number of faculty links
- `has_appointment`: Whether appointment section exists
- `has_click_button`: Whether "Click here" button present
- `has_apps`: Whether Family Hifazat app mentioned
- `subsection_count`: Number of subsection links

## Page Type Classification

| Type | Description | Characteristics |
|------|-------------|-----------------|
| **standard** | Most common department pages | H1 title, body content, single faculty link, appointment section |
| **simple** | Department without appointment | H1 title, body content, faculty link, NO appointment |
| **parent_overview** | Department overview with subdepartments | H1 title, subsection links to child departments |
| **multi_specialty** | Multiple specialty faculty links | H1 title, 3+ faculty links inline |
| **structured** | Service pages with H2 subheadings | H1 title, H2/H3 subheadings, no faculty links |
| **service_complex** | Complex services (Pathology, Pharmacy) | No H1 or collapsible sections, multiple faculty links |

## Configuration

Edit these constants in `akuh_scraper.py`:

```python
RATE_LIMIT_DELAY = 2  # Seconds between requests
REQUEST_TIMEOUT = 10  # Request timeout in seconds
USER_AGENT = "Mozilla/5.0..."  # Custom user agent
```

## Error Handling

The scraper handles:
- Network timeouts
- 404 Not Found errors
- 401 Unauthorized errors
- Connection errors
- Invalid HTML

Failed URLs are logged in the metadata and can be retried later.

## Performance

- Typical scrape time: ~2 seconds per page (with rate limiting)
- For 41 pages: ~90 seconds total
- Memory usage: Minimal (processes one page at a time)

## Quality Checks

The scraper automatically:
- Removes zero-width spaces (​)
- Normalizes whitespace
- Cleans HTML tags
- Validates URLs
- Deduplicates links
- Filters navigation elements

## Troubleshooting

### No results extracted
- Check that URLs are accessible
- Verify HTML structure matches expected patterns
- Check browser console for JavaScript-rendered content

### Too many external links
- Adjust the `extract_external_links()` function to filter more strictly
- Consider excluding specific link patterns

### Appointment section not detected
- Verify page contains "Request an Appointment" text
- Check phone number format matches regex pattern

## Future Enhancements

- [ ] JavaScript rendering support (Selenium/Playwright)
- [ ] Proxy rotation for rate limiting
- [ ] Database export (SQLite, PostgreSQL)
- [ ] Incremental scraping (only new/changed pages)
- [ ] Image extraction and download
- [ ] Content change detection
