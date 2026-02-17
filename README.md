# AKUH Health Services Scraper & Uploader

A production-ready Python scraper and uploader for extracting structured data from Aga Khan University Hospital Pakistan's department and service pages, then uploading to Storyblok CMS with proper nested block structure.

## Overview

This toolkit automatically:
1. **Scrapes** AKUH department pages and saves individual JSON files
2. **Uploads** data to Storyblok CMS with nested Grid Layout blocks including appointment section

## Quick Start

### 1. Prerequisites

```bash
pip install requests beautifulsoup4 python-dotenv
```

### 2. Setup Credentials

Copy `.env.example` to `.env` and add your Storyblok credentials:

```bash
cp .env.example .env
```

Edit `.env`:
```
STORYBLOK_TOKEN=your_personal_access_token
STORYBLOK_SPACE_ID=123456
```

### 3. Add URLs

Open `links.txt` and add your URLs (one per line):

```
https://hospitals.aku.edu/pakistan/Health-Services/department-of-anaesthesiology/Pages/default.aspx
https://hospitals.aku.edu/pakistan/Health-Services/department-of-dentistry/Pages/default.aspx
```

### 4. Run Complete Pipeline

```bash
# Scrape and upload in one command
python akuh_scrape_and_upload.py --publish
```

Or run separately:

```bash
# Scrape only
python akuh_scrape_and_upload.py --scrape-only

# Upload only (from existing output folder)
python akuh_scrape_and_upload.py --upload-only output_2026-02-16_131021 --publish
```

## Features

### Scraper Features

✅ **Data Extraction**
- Page title and breadcrumb
- Full body content (main_paragraphs)
- Appointment section detection (has_appointment_section flag)
- Faculty links with specialty information
- Page type classification (6 types)
- Hero image extraction

✅ **Output**
- Individual JSON files per page
- Timestamped output folders
- Metadata tracking (success/failure)
- CSV summary for quick reference

### Uploader Features

✅ **Storyblok Integration**
- Creates stories in: `Automation > health-services` folder
- Content type: `Health And Service`
- Proper nested block structure
- Image upload to Storyblok assets
- Duplicate slug handling
- Validation (title and content required)

✅ **Block Structure**
```
Grid Layout (2 columns, gap 10)
├── Left Column: Paragraph (main content)
└── Right Column: Stack Layout
    ├── Paragraph (H6: "Request an Appointment:" + details)
    ├── App Store (Apple)
    ├── App Store (Google Play)
    └── App Store (Custom)
```

## Storyblok Block Structure

The uploader creates the following nested structure:

```json
{
  "component": "Health And Service",
  "title": "Department of Anaesthesiology",
  "blocks": [
    {
      "component": "grid",
      "layout_type": "grid",
      "columns": 2,
      "gap": 10,
      "children": [
        {
          "component": "paragraph",
          "text": {
            "type": "doc",
            "content": [
              {
                "type": "paragraph",
                "content": [
                  {
                    "type": "text",
                    "text": "Main department description..."
                  }
                ]
              }
            ]
          }
        },
        {
          "component": "grid",
          "layout_type": "stack",
          "children": [
            {
              "component": "paragraph",
              "text": {
                "type": "doc",
                "content": [
                  {
                    "type": "heading",
                    "attrs": {"level": 6},
                    "content": [{"type": "text", "text": "Request an Appointment:"}]
                  },
                  {
                    "type": "paragraph",
                    "content": [{"type": "text", "text": "Click here to request..."}]
                  }
                ]
              }
            },
            {
              "component": "app_store",
              "type": "apple",
              "link": {
                "url": "https://apps.apple.com/pk/app/family-hifazat/id1373736569"
              }
            },
            {
              "component": "app_store",
              "type": "google",
              "link": {
                "url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat"
              }
            },
            {
              "component": "app_store",
              "type": "custom",
              "link": {
                "url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat"
              }
            }
          ]
        }
      ]
    }
  ]
}
```

## Output Formats

### Scraper Output

Creates timestamped folder with individual files:

```
output_2026-02-16_131021/
├── 1_Department_of_Anaesthesiology.json
├── 2_Department_of_Dentistry.json
├── 3_Department_of_Cardiology.json
├── metadata.json
└── summary.csv
```

### JSON Structure (Individual Files)

Each file contains:

```json
{
  "url": "https://...",
  "page_title": "Department of Anaesthesiology",
  "breadcrumb": "The Aga Khan University Hospital, Pakistan > Health Services",
  "has_h1_title": true,
  "body_content": {
    "main_paragraphs": "Full content...",
    "has_appointment_section": true,
    "word_count": 273,
    "has_subheadings": true,
    "subheading_tags": ["h4"],
    "has_bullet_lists": true,
    "has_collapsible_sections": false
  },
  "faculty_links": {
    "count": 2,
    "pattern": "multiple",
    "links": [...]
  },
  "appointment_section": {
    "present": true,
    "components": {...}
  },
  "page_type_classification": "standard"
}
```

### CSV Summary

Quick reference table:

```csv
file_number,url,page_title,has_h1,page_type,body_word_count,faculty_link_count,has_appointment
1,https://...,Department of Anaesthesiology,true,standard,273,2,true
```

## Page Type Classification

The scraper automatically classifies pages into 6 types:

| Type | Description | Characteristics |
|------|-------------|-----------------|
| **standard** | Most common department pages | H1 + body + faculty link + appointment section |
| **simple** | Department without appointment | H1 + body + faculty link, NO appointment |
| **parent_overview** | Department overview pages | Contains subsection links to child departments |
| **multi_specialty** | Multiple specialty departments | 3+ faculty links inline |
| **structured** | Service pages with subheadings | H2/H3 subheadings, no faculty links |
| **service_complex** | Complex service pages | No H1 or collapsible sections |

## Configuration

Edit these constants in `akuh_scrape_and_upload.py` to customize:

```python
# Storyblok config
CONTENT_TYPE = "health_and_service"
FIELD_TITLE = "title"
FIELD_IMAGE = "image"
CONTENT_PATH = ["Automation", "health-services"]

# Scraper config
RATE_LIMIT_DELAY = 2  # Seconds between requests
REQUEST_TIMEOUT = 10  # Request timeout
```

## Workflow

### Complete Pipeline

```bash
# 1. Add URLs to links.txt
echo "https://hospitals.aku.edu/pakistan/Health-Services/department-of-anaesthesiology/Pages/default.aspx" > links.txt

# 2. Run scraper and uploader
python akuh_scrape_and_upload.py --publish

# 3. Check results
cat output_*/metadata.json
```

### Retry Failed Uploads

```bash
# Check which files failed
cat output_2026-02-16_131021/metadata.json | grep failed_urls

# Re-run upload only
python akuh_scrape_and_upload.py --upload-only output_2026-02-16_131021 --publish
```

## Validation Rules

The uploader validates:

✅ **Directory Structure**
- Ensures `Automation > health-services` folder exists
- Creates folders if missing

✅ **Content Validation**
- `page_title` must not be empty
- `main_paragraphs` must not be empty
- Skips invalid entries with error logging

✅ **Duplicate Handling**
- Automatic slug conflict resolution
- Retries with random suffix if slug exists

## Troubleshooting

### Scraper Issues

**Network timeout**
```
✗ Error: Connection to hospitals.aku.edu timed out
```
- Check internet connection
- Verify AKUH website is accessible
- Try again later (server may be busy)

**No results extracted**
- Verify URLs are correct
- Check page structure hasn't changed
- Review HTML in browser

### Uploader Issues

**"STORYBLOK_TOKEN not found"**
- Create `.env` file with credentials
- Copy from `.env.example`
- Verify token is valid in Storyblok

**"Folder not found"**
- Use correct output folder name
- Example: `python akuh_scrape_and_upload.py --upload-only output_2026-02-16_131021`

**"page_title is empty"**
- Check JSON file has valid title
- Re-scrape the page
- Verify page structure

**Story creation fails**
- Check Storyblok component schema matches
- Verify `Health And Service` content type exists
- Check `grid`, `paragraph`, `app_store` components exist

## Advanced Usage

### Upload Specific Files

```bash
# Upload only one folder
python akuh_scrape_and_upload.py --upload-only output_2026-02-16_131021

# Upload as drafts (not published)
python akuh_scrape_and_upload.py --upload-only output_2026-02-16_131021
```

### Custom Asset Folder

```bash
# Upload images to specific Storyblok folder
python akuh_scrape_and_upload.py --upload-only output_folder --asset-folder-id 789
```

### Batch Processing

```bash
# Process multiple URLs
cat urls.txt | while read url; do
    echo "$url" > links.txt
    python akuh_scrape_and_upload.py --publish
    sleep 5
done
```

## Performance

- **Per-page scrape time**: ~2-3 seconds (with rate limiting)
- **Per-page upload time**: ~1-2 seconds
- **For 41 pages**: ~3-5 minutes total
- **Memory usage**: Minimal (processes one page at a time)

## Quality Assurance

The toolkit includes built-in QA:

✅ All URLs processed (success or failure logged)
✅ No empty page titles
✅ Faculty links properly extracted
✅ Text properly cleaned (no special characters)
✅ Page type classification validated
✅ JSON output is valid and parseable
✅ Storyblok block structure validated
✅ Upload success/failure logged

## Files

- `akuh_scrape_and_upload.py` - Main combined script
- `links.txt` - URLs to scrape (one per line)
- `.env` - Storyblok credentials (create from .env.example)
- `output_*/` - Timestamped output folders
- `README.md` - This file

## Support

For issues:
1. Check troubleshooting section
2. Verify URLs and credentials
3. Check `metadata.json` for error details
4. Review Storyblok component schema

## License

This scraper is provided as-is for educational and research purposes.

## Disclaimer

- Respect AKUH's website terms of service
- Use appropriate rate limiting
- Don't overload the server
- Verify data accuracy before use
- Check copyright and usage rights

---

**Last Updated**: February 16, 2026
**Version**: 2.0
**Status**: Production Ready
