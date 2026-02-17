#!/usr/bin/env python
# akuh_scraper.py
# Comprehensive AKUH department/service page scraper with 6 page type classification

import requests
from bs4 import BeautifulSoup
import json
import time
import re
import sys
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlparse

# Configuration
RATE_LIMIT_DELAY = 2  # seconds between requests
REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"

# Sections to exclude from body content
EXCLUDED_SECTIONS = [
    "Resources and Information",
    "Quick Links",
    "Website Policies",
    "© The Aga Khan University Hospital",
]


def sanitize_filename(filename: str) -> str:
    """Convert title to safe filename."""
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = filename.replace(' ', '_').strip('. ')
    return filename[:100] if filename else "page"


def clean_text(text: str) -> str:
    """Clean text: remove zero-width spaces, normalize whitespace."""
    text = text.replace('\u200b', '')  # Remove zero-width space
    text = text.replace('\xa0', ' ')   # Replace non-breaking space
    text = re.sub(r'\s+', ' ', text)   # Normalize whitespace
    return text.strip()


def extract_title(soup: BeautifulSoup) -> str:
    """Extract page title from H1 or fallback to first major heading."""
    h1 = soup.find('h1')
    if h1:
        return clean_text(h1.get_text())
    
    # Fallback: look for first h2 or major text
    h2 = soup.find('h2')
    if h2:
        return clean_text(h2.get_text())
    
    return "Untitled"


def extract_breadcrumb(soup: BeautifulSoup) -> str:
    """Extract breadcrumb navigation path."""
    # Look for breadcrumb container
    breadcrumb_elem = soup.find('div', class_=lambda x: x and 'breadcrumb' in x.lower())
    if not breadcrumb_elem:
        breadcrumb_elem = soup.find('nav', class_=lambda x: x and 'breadcrumb' in x.lower())
    
    if breadcrumb_elem:
        links = breadcrumb_elem.find_all('a')
        if links:
            breadcrumb_parts = [clean_text(link.get_text()) for link in links]
            return ' > '.join(breadcrumb_parts)
    
    return ""


def extract_body_content(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract main body content paragraphs and structure."""
    # Find main content area - try multiple selectors
    content_div = None
    
    # Try specific content containers
    for selector in ['div.ContentMain', 'div.MainContentZone', 'div[role="main"]', 'article', 'main']:
        content_div = soup.select_one(selector)
        if content_div:
            break
    
    # Fallback: find the largest text container that's not navigation
    if not content_div:
        for div in soup.find_all('div', class_=lambda x: x and any(c in str(x).lower() for c in ['content', 'main', 'body', 'article'])):
            if div.find('p') or div.find('h1') or div.find('h2'):
                content_div = div
                break
    
    if not content_div:
        content_div = soup.body if soup.body else soup
    
    # Extract all paragraphs
    paragraphs = []
    for p in content_div.find_all('p', recursive=True):
        text = clean_text(p.get_text())
        if text and len(text) > 10:
            # Skip excluded sections
            if not any(excluded in text for excluded in EXCLUDED_SECTIONS):
                paragraphs.append(text)
    
    main_text = '\n\n'.join(paragraphs)
    
    # Check for subheadings within content
    subheadings = []
    for tag in ['h2', 'h3', 'h4', 'h5', 'h6']:
        if content_div.find(tag):
            subheadings.append(tag)
    
    # Check for bullet lists
    has_bullet_lists = bool(content_div.find('ul') or content_div.find('ol'))
    
    # Check for collapsible sections (H4 with collapse IDs)
    has_collapsible = bool(content_div.find('h4', id=lambda x: x and 'collapse' in x.lower()))
    
    return {
        'main_paragraphs': main_text,
        'word_count': len(main_text.split()),
        'has_subheadings': bool(subheadings),
        'subheading_tags': subheadings,
        'has_bullet_lists': has_bullet_lists,
        'has_collapsible_sections': has_collapsible,
    }


def extract_faculty_links(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract faculty links with specialty information."""
    faculty_links = []
    
    # Pattern 1: H4 with link
    for h4 in soup.find_all('h4'):
        link = h4.find('a', href=True)
        if link and ('/findadoctor.aspx' in link.get('href', '')):
            text = clean_text(link.get_text())
            url = link.get('href', '')
            specialty = extract_specialty_from_url(url)
            faculty_links.append({
                'text': text,
                'url': url,
                'specialty': specialty,
            })
    
    # Pattern 2: Inline links with "Meet our" or "Find a Doctor"
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = clean_text(link.get_text())
        
        if '/findadoctor.aspx' in href and link not in [l.get('_elem') for l in faculty_links]:
            if 'Meet our' in text or 'Find a Doctor' in text or 'faculty' in text.lower():
                specialty = extract_specialty_from_url(href)
                faculty_links.append({
                    'text': text,
                    'url': href,
                    'specialty': specialty,
                })
    
    # Remove duplicates
    seen = set()
    unique_links = []
    for link in faculty_links:
        key = (link['url'], link['text'])
        if key not in seen:
            seen.add(key)
            unique_links.append(link)
    
    pattern = 'none'
    if len(unique_links) == 1:
        pattern = 'single'
    elif len(unique_links) > 1:
        pattern = 'multiple'
    
    return {
        'count': len(unique_links),
        'pattern': pattern,
        'links': unique_links,
    }


def extract_specialty_from_url(url: str) -> str:
    """Extract specialty name from findadoctor.aspx URL parameter."""
    match = re.search(r'[?&]Spec=([^&]+)', url)
    if match:
        return match.group(1)
    return ""


def extract_appointment_section(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract appointment request section."""
    appointment = {
        'present': False,
        'components': {
            'heading': '',
            'click_here_link': {
                'present': False,
                'text': '',
                'url': '',
            },
            'phone_number': '',
            'family_hifazat': {
                'main_link_present': False,
                'google_play_button': False,
                'app_store_button': False,
            },
        },
    }
    
    # Look for "Request an Appointment" text
    for p in soup.find_all('p'):
        text = clean_text(p.get_text())
        if 'Request an Appointment' in text or 'request an appointment' in text.lower():
            appointment['present'] = True
            appointment['components']['heading'] = 'Request an Appointment'
            
            # Extract phone number
            phone_match = re.search(r'\(\d{3}\)\d{3}\d{6}', text)
            if phone_match:
                appointment['components']['phone_number'] = phone_match.group(0)
            
            # Look for click link
            click_link = p.find('a', href=True)
            if click_link:
                appointment['components']['click_here_link']['present'] = True
                appointment['components']['click_here_link']['text'] = clean_text(click_link.get_text())
                appointment['components']['click_here_link']['url'] = click_link.get('href', '')
            
            # Check for app buttons/links
            if 'Family Hifazat' in text or 'family hifazat' in text.lower():
                appointment['components']['family_hifazat']['main_link_present'] = True
            
            # Look for app store images/buttons
            for img in p.find_all('img'):
                src = img.get('src', '').lower()
                if 'google play' in src or 'playstore' in src:
                    appointment['components']['family_hifazat']['google_play_button'] = True
                if 'app store' in src or 'appstore' in src:
                    appointment['components']['family_hifazat']['app_store_button'] = True
            
            break
    
    return appointment


def extract_subsection_links(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract subsection links (for parent/overview pages)."""
    subsection_links = []
    
    # Find main content area first
    content_div = None
    for selector in ['div.ContentMain', 'div.MainContentZone', 'div[role="main"]', 'article', 'main']:
        content_div = soup.select_one(selector)
        if content_div:
            break
    
    if not content_div:
        content_div = soup.body if soup.body else soup
    
    # Look for bullet lists with links ONLY in main content
    for ul in content_div.find_all('ul', recursive=True):
        # Skip navigation menus
        if ul.find_parent(class_=lambda x: x and any(nav in str(x).lower() for nav in ['nav', 'menu', 'sidebar', 'header', 'footer'])):
            continue
        
        for li in ul.find_all('li', recursive=False):  # Direct children only
            link = li.find('a', href=True)
            if link:
                text = clean_text(link.get_text())
                url = link.get('href', '')
                
                # Filter: meaningful department/service links
                if text and len(text) > 2 and not text.startswith('#'):
                    subsection_links.append({
                        'text': text,
                        'url': url,
                    })
    
    return {
        'present': len(subsection_links) > 0,
        'count': len(subsection_links),
        'links': subsection_links,
    }


def extract_external_links(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Extract external and document links."""
    external_links = []
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = clean_text(link.get_text())
        
        if not text or not href:
            continue
        
        # Skip internal navigation and faculty links
        if '/findadoctor.aspx' in href or 'breadcrumb' in str(link.parent.get('class', '')):
            continue
        
        # Classify link type
        link_type = 'internal'
        if href.startswith('http') and 'aku.edu' not in href:
            link_type = 'external'
        elif href.endswith(('.pdf', '.doc', '.docx', '.xlsx')):
            link_type = 'document'
        
        external_links.append({
            'text': text,
            'url': href,
            'type': link_type,
        })
    
    return external_links


def classify_page_type(data: Dict[str, Any]) -> str:
    """Classify page into one of 6 types based on structure."""
    if data['subsection_links']['present']:
        return 'parent_overview'
    
    if not data['has_h1_title']:
        return 'service_complex'
    
    if data['body_content']['has_collapsible_sections']:
        return 'service_complex'
    
    if data['faculty_links']['count'] > 3:
        return 'multi_specialty'
    
    if data['faculty_links']['count'] == 1 and not data['appointment_section']['present']:
        return 'simple'
    
    if data['body_content']['has_subheadings'] and data['faculty_links']['count'] == 0:
        return 'structured'
    
    return 'standard'


def scrape_page(url: str) -> Optional[Dict[str, Any]]:
    """Scrape a single page and extract all data."""
    try:
        print(f"  Fetching: {url}")
        response = requests.get(url, headers={'User-Agent': USER_AGENT}, timeout=REQUEST_TIMEOUT)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Extract all components
        h1 = soup.find('h1')
        page_title = extract_title(soup)
        body_content = extract_body_content(soup)
        faculty_links = extract_faculty_links(soup)
        appointment_section = extract_appointment_section(soup)
        subsection_links = extract_subsection_links(soup)
        external_links = extract_external_links(soup)
        breadcrumb = extract_breadcrumb(soup)
        
        data = {
            'url': url,
            'page_title': page_title,
            'breadcrumb': breadcrumb,
            'has_h1_title': bool(h1),
            'body_content': body_content,
            'subsection_links': subsection_links,
            'faculty_links': faculty_links,
            'appointment_section': appointment_section,
            'external_links': external_links,
        }
        
        # Classify page type
        data['page_type_classification'] = classify_page_type(data)
        
        print(f"    ✓ Type: {data['page_type_classification']}, Faculty: {faculty_links['count']}, Appointment: {appointment_section['present']}")
        return data
        
    except requests.RequestException as e:
        print(f"  ✗ Error: {e}")
        return None
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        return None


def main():
    """Main execution."""
    # Read URLs from file
    links_file = Path('links.txt')
    if not links_file.exists():
        print(f"Error: {links_file} not found")
        sys.exit(1)
    
    urls = []
    with open(links_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    
    print(f"Found {len(urls)} URLs to scrape\n")
    
    # Create output folder with timestamp
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    output_folder = Path(f'output_{timestamp}')
    output_folder.mkdir(exist_ok=True)
    
    print(f"Output folder: {output_folder}\n")
    
    # Scrape all pages
    results = []
    failed_urls = []
    page_counter = 1
    
    for idx, url in enumerate(urls, 1):
        print(f"[{idx}/{len(urls)}] Scraping...")
        page_data = scrape_page(url)
        
        if page_data:
            results.append(page_data)
            
            # Save individual JSON file
            title = page_data['page_title']
            safe_filename = sanitize_filename(title)
            json_file = output_folder / f"{page_counter}_{safe_filename}.json"
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(page_data, f, indent=2, ensure_ascii=False)
            
            print(f"    ✓ Saved: {json_file.name}")
            page_counter += 1
        else:
            failed_urls.append(url)
        
        time.sleep(RATE_LIMIT_DELAY)
    
    # Create metadata file
    metadata = {
        'scrape_metadata': {
            'date': datetime.now().isoformat(),
            'total_pages': len(urls),
            'pages_scraped': len(results),
            'pages_failed': len(failed_urls),
            'failed_urls': failed_urls,
            'output_folder': str(output_folder),
        },
        'summary': {
            'total_files': len(results),
            'file_pattern': '{number}_{title}.json',
        }
    }
    
    metadata_file = output_folder / 'metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    # Create CSV summary
    csv_file = output_folder / 'summary.csv'
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        f.write('file_number,url,page_title,has_h1,page_type,body_word_count,faculty_link_count,has_appointment,has_click_button,has_apps,subsection_count\n')
        for idx, page in enumerate(results, 1):
            url = page['url']
            title = page['page_title'].replace('"', '""')
            has_h1 = str(page['has_h1_title']).lower()
            ptype = page['page_type_classification']
            word_count = page['body_content']['word_count']
            faculty_count = page['faculty_links']['count']
            has_appt = str(page['appointment_section']['present']).lower()
            has_click = str(page['appointment_section']['components']['click_here_link']['present']).lower()
            has_apps = str(page['appointment_section']['components']['family_hifazat']['main_link_present']).lower()
            subsection_count = page['subsection_links']['count']
            
            f.write(f'{idx},"{url}","{title}",{has_h1},{ptype},{word_count},{faculty_count},{has_appt},{has_click},{has_apps},{subsection_count}\n')
    
    print(f"\n✓ Scraping complete!")
    print(f"  Successful: {len(results)}/{len(urls)}")
    print(f"  Failed: {len(failed_urls)}")
    print(f"  Output folder: {output_folder}")
    print(f"  Individual files: {len(results)} JSON files")
    print(f"  Metadata: {metadata_file.name}")
    print(f"  Summary: {csv_file.name}")
    
    # Print summary by page type
    print("\nPage Type Distribution:")
    type_counts = {}
    for page in results:
        ptype = page['page_type_classification']
        type_counts[ptype] = type_counts.get(ptype, 0) + 1
    
    for ptype, count in sorted(type_counts.items()):
        print(f"  {ptype}: {count}")


if __name__ == '__main__':
    main()
