#!/usr/bin/env python
# akuh_scrape_and_upload.py
# Combined AKUH scraper and Storyblok uploader

import argparse
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any
from urllib.parse import urljoin, urlsplit, quote

import requests
from bs4 import BeautifulSoup
from requests.exceptions import HTTPError, SSLError, Timeout, ConnectionError

try:
    from dotenv import load_dotenv
except Exception:
    load_dotenv = None


# ----------------------------
# Env
# ----------------------------
def _load_env_file(path: Path) -> None:
    if not path.exists():
        return
    try:
        with open(path, "r", encoding="utf-8", errors="ignore") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, value = line.partition("=")
                key = key.strip()
                value = value.strip().strip('"').strip("'")
                if key and value:
                    os.environ.setdefault(key, value)
    except Exception:
        pass


# ----------------------------
# Storyblok config
# ----------------------------
CONTENT_TYPE = "health_and_service"
FIELD_TITLE = "title"
FIELD_DESCRIPTION = "description"
FIELD_IMAGE = "image"
CONTENT_PATH = ["Automation", "health-services"]

# Scraper config
RATE_LIMIT_DELAY = 2
REQUEST_TIMEOUT = 10
USER_AGENT = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) Chrome/120.0.0.0 Safari/537.36"

EXCLUDED_SECTIONS = [
    "Resources and Information",
    "Quick Links",
    "Website Policies",
    "© The Aga Khan University Hospital",
]


# ----------------------------
# Logging
# ----------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("akuh")
    logger.setLevel(logging.INFO)
    logger.handlers.clear()
    fmt = logging.Formatter("%(asctime)s | %(levelname)s | %(message)s")
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(fmt)
    logger.addHandler(sh)
    return logger


# ----------------------------
# Helpers
# ----------------------------
def sanitize_filename(filename: str) -> str:
    """Convert title to safe filename."""
    filename = re.sub(r'[<>:"/\\|?*]', '', filename)
    filename = filename.replace(' ', '_').strip('. ')
    return filename[:100] if filename else "page"


def clean_text(text: str) -> str:
    """Clean text: remove zero-width spaces, normalize whitespace."""
    text = text.replace('\u200b', '')
    text = text.replace('\xa0', ' ')
    text = re.sub(r'\s+', ' ', text)
    return text.strip()


def slugify(s: str, max_len: int = 90) -> str:
    """Convert string to URL-safe slug."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s, flags=re.UNICODE).strip("-")
    if not s:
        s = f"service-{int(time.time())}"
    return s[:max_len].rstrip("-")


# ----------------------------
# Scraper Functions
# ----------------------------
def extract_title(soup: BeautifulSoup) -> str:
    """Extract page title from H1 or fallback."""
    h1 = soup.find('h1')
    if h1:
        return clean_text(h1.get_text())
    h2 = soup.find('h2')
    if h2:
        return clean_text(h2.get_text())
    return "Untitled"


def extract_breadcrumb(soup: BeautifulSoup) -> str:
    """Extract breadcrumb navigation path."""
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
    """Extract main body content paragraphs and structure, check for appointment section."""
    content_div = None
    
    for selector in ['div.ContentMain', 'div.MainContentZone', 'div[role="main"]', 'article', 'main']:
        content_div = soup.select_one(selector)
        if content_div:
            break
    
    if not content_div:
        for div in soup.find_all('div', class_=lambda x: x and any(c in str(x).lower() for c in ['content', 'main', 'body', 'article'])):
            if div.find('p') or div.find('h1') or div.find('h2'):
                content_div = div
                break
    
    if not content_div:
        content_div = soup.body if soup.body else soup
    
    paragraphs = []
    has_appointment_section = False
    
    for p in content_div.find_all('p', recursive=True):
        text = clean_text(p.get_text())
        if text and len(text) > 10:
            if not any(excluded in text for excluded in EXCLUDED_SECTIONS):
                paragraphs.append(text)
    
    # Check for appointment section by looking for <strong>Request an Appointment:</strong> pattern
    for strong in content_div.find_all('strong'):
        strong_text = clean_text(strong.get_text())
        if 'Request an Appointment' in strong_text:
            # Check if the next content has phone number and/or Family Hifazat
            parent = strong.find_parent(['div', 'p'])
            if parent:
                parent_text = clean_text(parent.get_text())
                # Check for phone number and/or Family Hifazat mention
                if ('(021)111911911' in parent_text or '(021) 111911911' in parent_text) or 'Family Hifazat' in parent_text:
                    has_appointment_section = True
                    break
    
    # Fallback: Check in all paragraphs for appointment pattern (more flexible)
    if not has_appointment_section:
        for p in paragraphs:
            # Check if paragraph contains both "Request an Appointment" AND appointment details
            if 'Request an Appointment' in p:
                # Check for any appointment-related content
                if ('(021)111911911' in p or 'Family Hifazat' in p or 'Click here' in p or 'call to book' in p):
                    has_appointment_section = True
                    break
    
    main_text = '\n\n'.join(paragraphs)
    
    # Final check: if not found yet, check in combined main_text
    if not has_appointment_section:
        # Simple check: if both "Request an Appointment" and phone/app are in the text
        if 'Request an Appointment' in main_text:
            if '(021)111911911' in main_text or 'Family Hifazat' in main_text:
                has_appointment_section = True
    
    subheadings = []
    for tag in ['h2', 'h3', 'h4', 'h5', 'h6']:
        if content_div.find(tag):
            subheadings.append(tag)
    
    has_bullet_lists = bool(content_div.find('ul') or content_div.find('ol'))
    has_collapsible = bool(content_div.find('h4', id=lambda x: x and 'collapse' in x.lower()))
    
    return {
        'main_paragraphs': main_text,
        'has_appointment_section': has_appointment_section,
        'word_count': len(main_text.split()),
        'has_subheadings': bool(subheadings),
        'subheading_tags': subheadings,
        'has_bullet_lists': has_bullet_lists,
        'has_collapsible_sections': has_collapsible,
    }


def extract_faculty_links(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract faculty links with specialty information."""
    faculty_links = []
    
    for h4 in soup.find_all('h4'):
        link = h4.find('a', href=True)
        if link and ('/findadoctor.aspx' in link.get('href', '')):
            text = clean_text(link.get_text())
            url = link.get('href', '')
            specialty = extract_specialty_from_url(url)
            faculty_links.append({'text': text, 'url': url, 'specialty': specialty})
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = clean_text(link.get_text())
        
        if '/findadoctor.aspx' in href and link not in [l.get('_elem') for l in faculty_links]:
            if 'Meet our' in text or 'Find a Doctor' in text or 'faculty' in text.lower():
                specialty = extract_specialty_from_url(href)
                faculty_links.append({'text': text, 'url': href, 'specialty': specialty})
    
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
    
    return {'count': len(unique_links), 'pattern': pattern, 'links': unique_links}


def extract_specialty_from_url(url: str) -> str:
    """Extract specialty name from URL parameter."""
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
            'click_here_link': {'present': False, 'text': '', 'url': ''},
            'phone_number': '',
            'family_hifazat': {'main_link_present': False, 'google_play_button': False, 'app_store_button': False},
        },
    }
    
    for p in soup.find_all('p'):
        text = clean_text(p.get_text())
        if 'Request an Appointment' in text or 'request an appointment' in text.lower():
            appointment['present'] = True
            appointment['components']['heading'] = 'Request an Appointment'
            
            phone_match = re.search(r'\(\d{3}\)\d{3}\d{6}', text)
            if phone_match:
                appointment['components']['phone_number'] = phone_match.group(0)
            
            click_link = p.find('a', href=True)
            if click_link:
                appointment['components']['click_here_link']['present'] = True
                appointment['components']['click_here_link']['text'] = clean_text(click_link.get_text())
                appointment['components']['click_here_link']['url'] = click_link.get('href', '')
            
            if 'Family Hifazat' in text or 'family hifazat' in text.lower():
                appointment['components']['family_hifazat']['main_link_present'] = True
            
            for img in p.find_all('img'):
                src = img.get('src', '').lower()
                if 'google play' in src or 'playstore' in src:
                    appointment['components']['family_hifazat']['google_play_button'] = True
                if 'app store' in src or 'appstore' in src:
                    appointment['components']['family_hifazat']['app_store_button'] = True
            
            break
    
    return appointment


def extract_subsection_links(soup: BeautifulSoup) -> Dict[str, Any]:
    """Extract subsection links."""
    subsection_links = []
    
    content_div = None
    for selector in ['div.ContentMain', 'div.MainContentZone', 'div[role="main"]', 'article', 'main']:
        content_div = soup.select_one(selector)
        if content_div:
            break
    
    if not content_div:
        content_div = soup.body if soup.body else soup
    
    for ul in content_div.find_all('ul', recursive=True):
        if ul.find_parent(class_=lambda x: x and any(nav in str(x).lower() for nav in ['nav', 'menu', 'sidebar', 'header', 'footer'])):
            continue
        
        for li in ul.find_all('li', recursive=False):
            link = li.find('a', href=True)
            if link:
                text = clean_text(link.get_text())
                url = link.get('href', '')
                
                if text and len(text) > 2 and not text.startswith('#'):
                    subsection_links.append({'text': text, 'url': url})
    
    return {'present': len(subsection_links) > 0, 'count': len(subsection_links), 'links': subsection_links}


def extract_external_links(soup: BeautifulSoup) -> List[Dict[str, str]]:
    """Extract external and document links."""
    external_links = []
    
    for link in soup.find_all('a', href=True):
        href = link.get('href', '')
        text = clean_text(link.get_text())
        
        if not text or not href:
            continue
        
        if '/findadoctor.aspx' in href or 'breadcrumb' in str(link.parent.get('class', '')):
            continue
        
        link_type = 'internal'
        if href.startswith('http') and 'aku.edu' not in href:
            link_type = 'external'
        elif href.endswith(('.pdf', '.doc', '.docx', '.xlsx')):
            link_type = 'document'
        
        external_links.append({'text': text, 'url': href, 'type': link_type})
    
    return external_links


def classify_page_type(data: Dict[str, Any]) -> str:
    """Classify page into one of 6 types."""
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
        
        data['page_type_classification'] = classify_page_type(data)
        
        print(f"    ✓ Type: {data['page_type_classification']}, Faculty: {faculty_links['count']}, Appointment: {appointment_section['present']}")
        return data
        
    except requests.RequestException as e:
        print(f"  ✗ Error: {e}")
        return None
    except Exception as e:
        print(f"  ✗ Unexpected error: {e}")
        return None


# ----------------------------
# Storyblok Client
# ----------------------------
class StoryblokClient:
    def __init__(self, token: str, space_id: int, logger: logging.Logger):
        self.token = token
        self.space_id = space_id
        self.logger = logger
        self.base = "https://mapi.storyblok.com/v1"
        self.s = requests.Session()
        self.s.headers.update({
            "Authorization": token,
            "Content-Type": "application/json",
            "Accept": "application/json"
        })

    def _req(self, method: str, path: str, *, params=None, json_body=None, timeout=90, retries=4) -> dict:
        """Make API request with retry logic."""
        url = f"{self.base}{path}"
        last_err = None
        
        for attempt in range(1, retries + 1):
            try:
                r = self.s.request(method, url, params=params, data=json.dumps(json_body) if json_body else None, timeout=timeout)
                if r.status_code >= 400:
                    raise requests.HTTPError(f"{r.status_code} {r.text[:2000]}", response=r)
                return r.json()
            except Exception as e:
                last_err = e
                if attempt < retries:
                    time.sleep(1.2 * attempt)
                    continue
                raise last_err

    def create_signed_asset(self, filename: str, asset_folder_id: Optional[int] = None) -> dict:
        """Create signed asset for upload."""
        body = {"filename": filename}
        if asset_folder_id:
            body["asset_folder_id"] = int(asset_folder_id)
        return self._req("POST", f"/spaces/{self.space_id}/assets", json_body=body, timeout=90, retries=4)

    def upload_asset_from_bytes(self, signed_payload: dict, file_bytes: bytes, filename: str, mime: str) -> None:
        """Upload asset bytes to S3."""
        fields = signed_payload.get("fields") or {}
        post_url = signed_payload.get("post_url")
        
        if not post_url or not fields:
            raise RuntimeError("Signed upload payload missing fields/post_url")
        
        r = requests.post(post_url, data=fields, files={"file": (filename, file_bytes, mime)}, timeout=180)
        r.raise_for_status()

    @staticmethod
    def _ext_from_mime(mime: str) -> str:
        """Get file extension from MIME type."""
        m = (mime or "").lower()
        if "png" in m:
            return ".png"
        if "jpeg" in m or "jpg" in m:
            return ".jpg"
        if "gif" in m:
            return ".gif"
        if "webp" in m:
            return ".webp"
        return ""

    def list_folders(self) -> list:
        """List all folders in space."""
        out, page = [], 1
        while True:
            data = self._req("GET", f"/spaces/{self.space_id}/stories", params={"folder_only": 1, "per_page": 100, "page": page})
            items = data.get("stories", []) or []
            out.extend(items)
            if page * 100 >= int(data.get("total") or 0) or not items:
                break
            page += 1
        return out

    def ensure_content_folder_by_path(self, path_parts: list) -> int:
        """Ensure folder structure exists, create if needed."""
        if not path_parts:
            return 0
        
        folders = self.list_folders()
        parent_id = 0
        
        for name in path_parts:
            found = next((f for f in folders if f.get("is_folder") and f.get("name") == name and int(f.get("parent_id") or 0) == parent_id), None)
            
            if found:
                parent_id = int(found["id"])
                continue
            
            body = {"story": {"name": name, "slug": slugify(name), "is_folder": True, "parent_id": parent_id, "content": {"component": "folder"}}}
            created = self._req("POST", f"/spaces/{self.space_id}/stories", json_body=body)
            folder = created.get("story") or created
            parent_id = int(folder.get("id"))
            folders.append(folder)
        
        return parent_id

    def create_story(self, title: str, slug: str, content: dict, parent_id: int = 0, publish: bool = False) -> dict:
        """Create a story in Storyblok."""
        body = {"story": {"name": title, "slug": slug, "parent_id": int(parent_id), "content": content}}
        return self._req("POST", f"/spaces/{self.space_id}/stories", params={"publish": 1} if publish else None, json_body=body)


# ----------------------------
# Upload Functions
# ----------------------------
def upload_image_to_storyblok(client: StoryblokClient, image_path: str, asset_folder_id: Optional[int] = None, max_retries: int = 3) -> Optional[dict]:
    """Upload image to Storyblok and return asset object."""
    if not image_path:
        return None
    
    path_obj = Path(image_path)
    
    if not path_obj.is_absolute() and not path_obj.exists():
        path_obj = Path(__file__).resolve().parent / image_path
    
    if not path_obj.exists():
        client.logger.warning(f"Image not found: {image_path}")
        return None
    
    for attempt in range(1, max_retries + 1):
        try:
            file_bytes = path_obj.read_bytes()
            mime, _ = mimetypes.guess_type(str(path_obj))
            
            if not mime or not mime.startswith("image/"):
                ext = path_obj.suffix.lower()
                mime = "image/png" if ext == ".png" else "image/jpeg" if ext in (".jpg", ".jpeg") else "image/gif" if ext == ".gif" else "image/webp" if ext == ".webp" else "image/jpeg"
            
            filename = path_obj.name or f"image-{int(time.time())}{client._ext_from_mime(mime)}"
            
            signed = client.create_signed_asset(filename, asset_folder_id)
            payload = signed.get("data") or signed
            
            client.upload_asset_from_bytes(payload, file_bytes, filename, mime)
            
            key = (payload.get("fields") or {}).get("key")
            if not key:
                raise RuntimeError("No asset key")
            
            asset_obj = {"filename": f"https://a.storyblok.com/{key}", "fieldtype": "asset"}
            
            if signed.get("id") or (signed.get("asset") or {}).get("id"):
                asset_obj["id"] = int(signed.get("id") or (signed.get("asset") or {}).get("id"))
            
            client.logger.info(f"Uploaded image: {filename}")
            return asset_obj
            
        except (SSLError, Timeout, ConnectionError) as e:
            if attempt < max_retries:
                time.sleep(1.5 * attempt)
                continue
        except Exception as e:
            client.logger.error(f"Image upload failed: {image_path} | {e}")
            return None
    
    return None


# def create_storyblok_story(client: StoryblokClient, title: str, description: str, image_asset: Optional[dict], parent_id: int = 0, publish: bool = False) -> Optional[dict]:
#     """Create story in Storyblok with proper block structure."""
#     base_slug = slugify(title)
    
#     for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
#         try:
#             # Build the block structure
#             blocks = [
#                 {
#                     "component": "grid",
#                     "layout_type": "grid",
#                     "columns": 2,
#                     "gap": 10,
#                     "_uid": f"grid-{int(time.time())}-{random.randint(1000, 9999)}",
#                     "children": [
#                         # Left column: Main paragraph
#                         {
#                             "component": "paragraph",
#                             "text": {
#                                 "type": "doc",
#                                 "content": [
#                                     {
#                                         "type": "paragraph",
#                                         "content": [
#                                             {
#                                                 "type": "text",
#                                                 "text": description
#                                             }
#                                         ]
#                                     }
#                                 ]
#                             },
#                             "_uid": f"para-{int(time.time())}-{random.randint(1000, 9999)}"
#                         },
#                         # Right column: Stack layout with appointment info
#                         {
#                             "component": "grid",
#                             "layout_type": "stack",
#                             "_uid": f"stack-{int(time.time())}-{random.randint(1000, 9999)}",
#                             "children": [
#                                 # Appointment paragraph with H6 heading
#                                 {
#                                     "component": "paragraph",
#                                     "text": {
#                                         "type": "doc",
#                                         "content": [
#                                             {
#                                                 "type": "heading",
#                                                 "attrs": {"level": 6},
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Request an Appointment:"
#                                                     }
#                                                 ]
#                                             },
#                                             {
#                                                 "type": "paragraph",
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Click here to request an appointment online, call to book an appointment: (021)111911911 or use our Family Hifazat APP to self-book."
#                                                     }
#                                                 ]
#                                             }
#                                         ]
#                                     },
#                                     "_uid": f"appt-para-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Apple App Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "apple",
#                                     "link": {
#                                         "cached_url": "https://apps.apple.com/pk/app/family-hifazat/id1373736569",
#                                         "linktype": "url",
#                                         "url": "https://apps.apple.com/pk/app/family-hifazat/id1373736569"
#                                     },
#                                     "_uid": f"apple-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Google Play Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "google",
#                                     "link": {
#                                         "cached_url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat",
#                                         "linktype": "url",
#                                         "url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat"
#                                     },
#                                     "_uid": f"google-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Custom App Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "custom",
#                                     "link": {
#                                         "cached_url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat",
#                                         "linktype": "url",
#                                         "url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat"
#                                     },
#                                     "_uid": f"custom-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 }
#                             ]
#                         }
#                     ]
#                 }
#             ]
            
#             # Create content with blocks
#             content = {
#                 "component": CONTENT_TYPE,
#                 FIELD_TITLE: title,
#                 "blocks": blocks
#             }
            
#             if image_asset:
#                 content[FIELD_IMAGE] = image_asset
            
#             result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
#             story = result.get("story") or result
            
#             client.logger.info(f"Created story: {story.get('id')} / {story.get('slug')}")
#             return story
            
#         except HTTPError as he:
#             resp = getattr(he, "response", None)
#             if resp and resp.status_code == 422 and ("already taken" in (resp.text or "").lower() or "slug" in (resp.text or "").lower()):
#                 continue
#             client.logger.error(f"Story creation failed: {he}")
#             return None
#         except Exception as e:
#             client.logger.error(f"Story creation failed: {e}")
#             return None
    
#     return None

# def create_storyblok_story(client: StoryblokClient, title: str, description: str, image_asset: Optional[dict], parent_id: int = 0, publish: bool = False) -> Optional[dict]:
#     """Create story in Storyblok with proper block structure matching the component layers."""
#     base_slug = slugify(title)
    
#     for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
#         try:
#             # Build the block structure exactly as shown in component layers
#             blocks = [
#                 {
#                     "component": "grid",
#                     "layout_type": "grid",
#                     "columns": 2,
#                     "gap": 15,  # ✅ FIXED: Changed from 10 to 15
#                     "_uid": f"grid-{int(time.time())}-{random.randint(1000, 9999)}",
#                     "children": [
#                         # Left column: Main paragraph
#                         {
#                             "component": "paragraph",
#                             "text": {
#                                 "type": "doc",
#                                 "content": [
#                                     {
#                                         "type": "paragraph",
#                                         "content": [
#                                             {
#                                                 "type": "text",
#                                                 "text": description
#                                             }
#                                         ]
#                                     }
#                                 ]
#                             },
#                             "_uid": f"para-{int(time.time())}-{random.randint(1000, 9999)}"
#                         },
#                         # Right column: Stack layout with appointment info
#                         {
#                             "component": "grid",
#                             "layout_type": "stack",
#                             "_uid": f"stack-{int(time.time())}-{random.randint(1000, 9999)}",
#                             "children": [
#                                 # Appointment paragraph with H6 heading
#                                 {
#                                     "component": "paragraph",
#                                     "text": {
#                                         "type": "doc",
#                                         "content": [
#                                             {
#                                                 "type": "heading",
#                                                 "attrs": {"level": 6},
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Request an Appointment:"
#                                                     }
#                                                 ]
#                                             },
#                                             {
#                                                 "type": "paragraph",
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Click here to request an appointment online, call to book an appointment: (021)111911911 or use our Family Hifazat APP to self-book."
#                                                     }
#                                                 ]
#                                             }
#                                         ]
#                                     },
#                                     "_uid": f"appt-para-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # ✅ FIXED: Custom App Store FIRST (Family Hifazat website)
#                                 {
#                                     "component": "app_store",
#                                     "type": "custom",
#                                     "link": {
#                                         "cached_url": "https://familyhifazat.aku.edu/User/Login",
#                                         "linktype": "url",
#                                         "url": "https://familyhifazat.aku.edu/User/Login"
#                                     },
#                                   "_uid": f"custom-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Google Play Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "google",
#                                     "link": {
#                                         "cached_url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat",
#                                         "linktype": "url",
#                                         "url": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat"
#                                     },
#                                     "_uid": f"google-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Apple App Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "apple",
#                                     "link": {
#                                         "cached_url": "https://apps.apple.com/pk/app/family-hifazat/id1373736569",
#                                         "linktype": "url",
#                                         "url": "https://apps.apple.com/pk/app/family-hifazat/id1373736569"
#                                     },
#                                     "_uid": f"apple-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 }
#                             ]
#                         }
#                     ]
#                 }
#             ]
            
#             # Create content with blocks
#             content = {
#                 "component": CONTENT_TYPE,
#                 FIELD_TITLE: title,
#                 "blocks": blocks
#             }
            
#             if image_asset:
#                 content[FIELD_IMAGE] = image_asset
            
#             result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
#             story = result.get("story") or result
            
#             client.logger.info(f"Created story: {story.get('id')} / {story.get('slug')}")
#             return story
            
#         except HTTPError as he:
#             resp = getattr(he, "response", None)
#             if resp and resp.status_code == 422 and ("already taken" in (resp.text or "").lower() or "slug" in (resp.text or "").lower()):
#                 continue
#             client.logger.error(f"Story creation failed: {he}")
#             return None
#         except Exception as e:
#             client.logger.error(f"Story creation failed: {e}")
#             return None
    
#     return None

# def create_storyblok_story(client: StoryblokClient, title: str, description: str, image_asset: Optional[dict], parent_id: int = 0, publish: bool = False) -> Optional[dict]:
#     """Create story in Storyblok with proper block structure matching the schema."""
#     base_slug = slugify(title)
    
#     for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
#         try:
#             # Build the block structure exactly matching the schema
#             blocks = [
#                 {
#                     "component": "grid_layout",  # ✅ FIXED: Changed from "grid" to "grid_layout"
#                     "layout_type": "grid",
#                     "columns": 2,
#                     "gap": 15,
#                     "_uid": f"grid-{int(time.time())}-{random.randint(1000, 9999)}",
#                     "children": [
#                         # Left column: Main paragraph
#                         {
#                             "component": "paragraph",
#                             "text": {
#                                 "type": "doc",
#                                 "content": [
#                                     {
#                                         "type": "paragraph",
#                                         "content": [
#                                             {
#                                                 "type": "text",
#                                                 "text": description
#                                             }
#                                         ]
#                                     }
#                                 ]
#                             },
#                             "_uid": f"para-{int(time.time())}-{random.randint(1000, 9999)}"
#                         },
#                         # Right column: Stack layout with appointment info
#                         {
#                             "component": "grid_layout",  # ✅ FIXED: Changed from "grid" to "grid_layout"
#                             "layout_type": "stack",
#                             "_uid": f"stack-{int(time.time())}-{random.randint(1000, 9999)}",
#                             "children": [
#                                 # Appointment paragraph with H6 heading
#                                 {
#                                     "component": "paragraph",
#                                     "text": {
#                                         "type": "doc",
#                                         "content": [
#                                             {
#                                                 "type": "heading",
#                                                 "attrs": {"level": 6},
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Request an Appointment:"
#                                                     }
#                                                 ]
#                                             },
#                                             {
#                                                 "type": "paragraph",
#                                                 "content": [
#                                                     {
#                                                         "type": "text",
#                                                         "text": "Click here to request an appointment online, call to book an appointment: (021)111911911 or use our Family Hifazat APP to self-book."
#                                                     }
#                                                 ]
#                                             }
#                                         ]
#                                     },
#                                     "_uid": f"appt-para-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # ✅ FIXED: Custom App Store FIRST with plain text link
#                                 {
#                                     "component": "app_store",
#                                     "type": "custom",
#                                     "link": "https://familyhifazat.aku.edu/User/Login",  # ✅ FIXED: Plain string, not object
#                                     "_uid": f"custom-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Google Play Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "google",
#                                     "link": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat",  # ✅ FIXED: Plain string
#                                     "_uid": f"google-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 },
#                                 # Apple App Store
#                                 {
#                                     "component": "app_store",
#                                     "type": "apple",
#                                     "link": "https://apps.apple.com/pk/app/family-hifazat/id1373736569",  # ✅ FIXED: Plain string
#                                     "_uid": f"apple-{int(time.time())}-{random.randint(1000, 9999)}"
#                                 }
#                             ]
#                         }
#                     ]
#                 }
#             ]
            
#             # Create content with blocks
#             content = {
#                 "component": CONTENT_TYPE,
#                 FIELD_TITLE: title,
#                 "blocks": blocks
#             }
            
#             if image_asset:
#                 content[FIELD_IMAGE] = image_asset
            
#             result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
#             story = result.get("story") or result
            
#             client.logger.info(f"Created story: {story.get('id')} / {story.get('slug')}")
#             return story
            
#         except HTTPError as he:
#             resp = getattr(he, "response", None)
#             if resp and resp.status_code == 422 and ("already taken" in (resp.text or "").lower() or "slug" in (resp.text or "").lower()):
#                 continue
#             client.logger.error(f"Story creation failed: {he}")
#             return None
#         except Exception as e:
#             client.logger.error(f"Story creation failed: {e}")
#             return None
    
#     return None

def create_storyblok_story(client: StoryblokClient, title: str, description: str, image_asset: Optional[dict], parent_id: int = 0, publish: bool = False) -> Optional[dict]:
    """Create story in Storyblok with proper block structure matching the schema."""
    base_slug = slugify(title)
    
    for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
        try:
            # Build the block structure exactly matching the schema
            blocks = [
                {
                    "component": "grid_layout",  # ✅ FIXED: Changed from "grid" to "grid_layout"
                    "layout_type": "grid",
                    "columns": 2,
                    "gap": 15,
                    "_uid": f"grid-{int(time.time())}-{random.randint(1000, 9999)}",
                    "children": [
                        # Left column: Main paragraph
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
                                                "text": description
                                            }
                                        ]
                                    }
                                ]
                            },
                            "_uid": f"para-{int(time.time())}-{random.randint(1000, 9999)}"
                        },
                        # Right column: Stack layout with appointment info
                        {
                            "component": "grid_layout",  # ✅ FIXED: Changed from "grid" to "grid_layout"
                            "layout_type": "stack",
                            "_uid": f"stack-{int(time.time())}-{random.randint(1000, 9999)}",
                            "children": [
                                # Appointment paragraph with H6 heading
                                {
                                    "component": "paragraph",
                                    "text": {
                                        "type": "doc",
                                        "content": [
                                            {
                                                "type": "heading",
                                                "attrs": {"level": 6},
                                                "content": [
                                                    {
                                                        "type": "text",
                                                        "text": "Request an Appointment:"
                                                    }
                                                ]
                                            },
                                            {
                                                "type": "paragraph",
                                                "content": [
                                                    {
                                                        "type": "text",
                                                        "text": "Click here to request an appointment online, call to book an appointment: (021)111911911 or use our Family Hifazat APP to self-book."
                                                    }
                                                ]
                                            }
                                        ]
                                    },
                                    "_uid": f"appt-para-{int(time.time())}-{random.randint(1000, 9999)}"
                                },
                                # ✅ FIXED: Custom App Store FIRST with plain text link
                                {
                                    "component": "app_store",
                                    "type": "custom",
                                    "link": "https://familyhifazat.aku.edu/User/Login",  # ✅ FIXED: Plain string, not object
                                    "_uid": f"custom-{int(time.time())}-{random.randint(1000, 9999)}"
                                },
                                # Google Play Store
                                {
                                    "component": "app_store",
                                    "type": "google",
                                    "link": "https://play.google.com/store/apps/details?id=edu.aku.family_hifazat",  # ✅ FIXED: Plain string
                                    "_uid": f"google-{int(time.time())}-{random.randint(1000, 9999)}"
                                },
                                # Apple App Store
                                {
                                    "component": "app_store",
                                    "type": "apple",
                                    "link": "https://apps.apple.com/pk/app/family-hifazat/id1373736569",  # ✅ FIXED: Plain string
                                    "_uid": f"apple-{int(time.time())}-{random.randint(1000, 9999)}"
                                }
                            ]
                        }
                    ]
                }
            ]
            
            # Create content with blocks
            content = {
                "component": CONTENT_TYPE,
                FIELD_TITLE: title,
                "blocks": blocks
            }
            
            if image_asset:
                content[FIELD_IMAGE] = image_asset
            
            result = client.create_story(title, slug, content, parent_id=parent_id, publish=publish)
            story = result.get("story") or result
            
            client.logger.info(f"Created story: {story.get('id')} / {story.get('slug')}")
            return story
            
        except HTTPError as he:
            resp = getattr(he, "response", None)
            if resp and resp.status_code == 422 and ("already taken" in (resp.text or "").lower() or "slug" in (resp.text or "").lower()):
                continue
            client.logger.error(f"Story creation failed: {he}")
            return None
        except Exception as e:
            client.logger.error(f"Story creation failed: {e}")
            return None
    
    return None

def run_upload(json_paths: List[Path], logger: logging.Logger, *, publish: bool = False, asset_folder_id: Optional[int] = None) -> None:
    """Upload JSON files to Storyblok."""
    token = (os.getenv("STORYBLOK_TOKEN") or "").strip()
    space_id_str = (os.getenv("STORYBLOK_SPACE_ID") or "").strip()
    
    if not token or not space_id_str:
        logger.error("Set STORYBLOK_TOKEN and STORYBLOK_SPACE_ID in .env")
        sys.exit(1)
    
    try:
        space_id = int(space_id_str)
    except ValueError:
        logger.error("STORYBLOK_SPACE_ID must be an integer")
        sys.exit(1)
    
    client = StoryblokClient(token, space_id, logger)
    
    # Ensure directory exists: Root → Automation → health-services
    logger.info("Ensuring directory structure: Automation > health-services")
    content_parent_id = client.ensure_content_folder_by_path(CONTENT_PATH)
    logger.info(f"Content folder ID: {content_parent_id}\n")
    
    uploaded_count = 0
    failed_count = 0
    
    for jp in json_paths:
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Skip {jp}: {e}")
            failed_count += 1
            continue
        
        # Validation: Check page_title is not empty
        title = (data.get("page_title") or "").strip()
        if not title:
            logger.error(f"Skip {jp}: page_title is empty")
            failed_count += 1
            continue
        
        # Validation: Check main_paragraphs is not empty
        description = (data.get("body_content", {}).get("main_paragraphs") or "").strip()
        if not description:
            logger.error(f"Skip {jp}: main_paragraphs is empty")
            failed_count += 1
            continue
        
        hero = data.get("hero_image")
        hero_path = None
        
        if hero:
            for p in [jp.parent / hero, jp.parent.parent / hero]:
                if p.exists():
                    hero_path = str(p)
                    break
            if not hero_path:
                hero_path = hero
        
        logger.info(f"Uploading: {title[:50]}...")
        
        image_asset = upload_image_to_storyblok(client, hero_path, asset_folder_id) if hero_path else None
        
        story = create_storyblok_story(client, title, description, image_asset, parent_id=content_parent_id, publish=publish)
        
        if story:
            logger.info(f"  ✓ Success: {story.get('id')} https://app.storyblok.com/#/me/spaces/{space_id}/stories/0/0/{story.get('id')}")
            uploaded_count += 1
        else:
            logger.error(f"  ✗ Failed to create story")
            failed_count += 1
    
    logger.info(f"\n{'='*60}")
    logger.info(f"Upload Summary:")
    logger.info(f"  Successful: {uploaded_count}")
    logger.info(f"  Failed: {failed_count}")
    logger.info(f"  Total: {uploaded_count + failed_count}")
    logger.info(f"{'='*60}")


# ----------------------------
# Main
# ----------------------------
def main():
    """Main execution."""
    script_dir = Path(__file__).resolve().parent
    
    for env_path in [script_dir / ".env", script_dir.parent / ".env", Path.cwd() / ".env"]:
        _load_env_file(env_path)
    
    if load_dotenv:
        load_dotenv(script_dir / ".env")
        load_dotenv(script_dir.parent / ".env")
        load_dotenv()
    
    ap = argparse.ArgumentParser(description="Scrape AKUH health services and upload to Storyblok")
    ap.add_argument("--links-file", default="links.txt", help="File with URLs (one per line)")
    ap.add_argument("--scrape-only", action="store_true", help="Only scrape, do not upload")
    ap.add_argument("--upload-only", metavar="FOLDER", help="Only upload from folder")
    ap.add_argument("--publish", action="store_true", help="Publish stories (default: draft)")
    ap.add_argument("--asset-folder-id", type=int, help="Storyblok asset folder ID")
    
    args = ap.parse_args()
    
    logger = setup_logging()
    
    # Upload only mode
    if args.upload_only:
        folder_path = Path(args.upload_only)
        if not folder_path.exists():
            logger.error(f"Folder not found: {folder_path}")
            sys.exit(1)
        
        json_paths = [p for p in folder_path.glob("*.json") if p.name != "metadata.json"]
        if not json_paths:
            logger.error("No JSON files to upload")
            sys.exit(1)
        
        logger.info(f"Found {len(json_paths)} files to upload\n")
        run_upload(json_paths, logger, publish=args.publish, asset_folder_id=args.asset_folder_id)
        return
    
    # Scrape mode
    links_file = Path(args.links_file)
    if not links_file.exists():
        logger.error(f"Links file not found: {links_file}")
        sys.exit(1)
    
    urls = []
    with open(links_file, 'r', encoding='utf-8') as f:
        for line in f:
            line = line.strip()
            if line and not line.startswith('#'):
                urls.append(line)
    
    logger.info(f"Found {len(urls)} URLs to scrape\n")
    
    # Create output folder
    timestamp = datetime.now().strftime('%Y-%m-%d_%H%M%S')
    output_folder = Path(f'output_{timestamp}')
    output_folder.mkdir(exist_ok=True)
    
    logger.info(f"Output folder: {output_folder}\n")
    
    # Scrape
    results = []
    failed_urls = []
    page_counter = 1
    
    for idx, url in enumerate(urls, 1):
        logger.info(f"[{idx}/{len(urls)}] Scraping...")
        page_data = scrape_page(url)
        
        if page_data:
            results.append(page_data)
            
            title = page_data['page_title']
            safe_filename = sanitize_filename(title)
            json_file = output_folder / f"{page_counter}_{safe_filename}.json"
            
            with open(json_file, 'w', encoding='utf-8') as f:
                json.dump(page_data, f, indent=2, ensure_ascii=False)
            
            logger.info(f"    ✓ Saved: {json_file.name}")
            page_counter += 1
        else:
            failed_urls.append(url)
        
        time.sleep(RATE_LIMIT_DELAY)
    
    # Create metadata
    metadata = {
        'scrape_metadata': {
            'date': datetime.now().isoformat(),
            'total_pages': len(urls),
            'pages_scraped': len(results),
            'pages_failed': len(failed_urls),
            'failed_urls': failed_urls,
            'output_folder': str(output_folder),
        },
        'summary': {'total_files': len(results), 'file_pattern': '{number}_{title}.json'}
    }
    
    metadata_file = output_folder / 'metadata.json'
    with open(metadata_file, 'w', encoding='utf-8') as f:
        json.dump(metadata, f, indent=2, ensure_ascii=False)
    
    # Create CSV
    csv_file = output_folder / 'summary.csv'
    with open(csv_file, 'w', encoding='utf-8', newline='') as f:
        f.write('file_number,url,page_title,has_h1,page_type,body_word_count,faculty_link_count,has_appointment\n')
        for idx, page in enumerate(results, 1):
            url = page['url']
            title = page['page_title'].replace('"', '""')
            has_h1 = str(page['has_h1_title']).lower()
            ptype = page['page_type_classification']
            word_count = page['body_content']['word_count']
            faculty_count = page['faculty_links']['count']
            has_appt = str(page['appointment_section']['present']).lower()
            
            f.write(f'{idx},"{url}","{title}",{has_h1},{ptype},{word_count},{faculty_count},{has_appt}\n')
    
    logger.info(f"\n✓ Scraping complete!")
    logger.info(f"  Successful: {len(results)}/{len(urls)}")
    logger.info(f"  Failed: {len(failed_urls)}")
    logger.info(f"  Output folder: {output_folder}")
    
    # Print page type distribution
    logger.info("\nPage Type Distribution:")
    type_counts = {}
    for page in results:
        ptype = page['page_type_classification']
        type_counts[ptype] = type_counts.get(ptype, 0) + 1
    
    for ptype, count in sorted(type_counts.items()):
        logger.info(f"  {ptype}: {count}")
    
    # Upload if not scrape-only
    if not args.scrape_only:
        logger.info("\n" + "="*50)
        logger.info("Starting upload to Storyblok...")
        logger.info("="*50 + "\n")
        
        json_paths = [p for p in output_folder.glob("*.json") if p.name != "metadata.json"]
        run_upload(json_paths, logger, publish=args.publish, asset_folder_id=args.asset_folder_id)


if __name__ == "__main__":
    main()
