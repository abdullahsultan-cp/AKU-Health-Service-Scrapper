#!/usr/bin/env python
# akuh_uploader.py
# Upload AKUH health services data to Storyblok

import argparse
import json
import logging
import mimetypes
import os
import random
import re
import sys
import time
from pathlib import Path
from typing import Dict, List, Optional
from urllib.parse import urlsplit, quote

import requests
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


# ----------------------------
# Logging
# ----------------------------
def setup_logging() -> logging.Logger:
    logger = logging.getLogger("uploader")
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
def slugify(s: str, max_len: int = 90) -> str:
    """Convert string to URL-safe slug."""
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s, flags=re.UNICODE).strip("-")
    if not s:
        s = f"service-{int(time.time())}"
    return s[:max_len].rstrip("-")


def safe_text(s: str) -> str:
    """Clean text."""
    s = (s or "").replace("\xa0", " ")
    s = re.sub(r"\s+", " ", s).strip()
    return s


# ----------------------------
# Storyblok client
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
                r = self.s.request(
                    method,
                    url,
                    params=params,
                    data=json.dumps(json_body) if json_body else None,
                    timeout=timeout
                )
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
        
        r = requests.post(
            post_url,
            data=fields,
            files={"file": (filename, file_bytes, mime)},
            timeout=180
        )
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
            data = self._req(
                "GET",
                f"/spaces/{self.space_id}/stories",
                params={"folder_only": 1, "per_page": 100, "page": page}
            )
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
            found = next(
                (f for f in folders 
                 if f.get("is_folder") and f.get("name") == name 
                 and int(f.get("parent_id") or 0) == parent_id),
                None
            )
            
            if found:
                parent_id = int(found["id"])
                continue
            
            body = {
                "story": {
                    "name": name,
                    "slug": slugify(name),
                    "is_folder": True,
                    "parent_id": parent_id,
                    "content": {"component": "folder"}
                }
            }
            created = self._req("POST", f"/spaces/{self.space_id}/stories", json_body=body)
            folder = created.get("story") or created
            parent_id = int(folder.get("id"))
            folders.append(folder)
        
        return parent_id

    def create_story(self, title: str, slug: str, content: dict, parent_id: int = 0, publish: bool = False) -> dict:
        """Create a story in Storyblok."""
        body = {
            "story": {
                "name": title,
                "slug": slug,
                "parent_id": int(parent_id),
                "content": content
            }
        }
        return self._req(
            "POST",
            f"/spaces/{self.space_id}/stories",
            params={"publish": 1} if publish else None,
            json_body=body
        )


# ----------------------------
# Upload functions
# ----------------------------
def upload_image_to_storyblok(
    client: StoryblokClient,
    image_path: str,
    asset_folder_id: Optional[int] = None,
    max_retries: int = 3
) -> Optional[dict]:
    """Upload image to Storyblok and return asset object."""
    if not image_path:
        return None
    
    path_obj = Path(image_path)
    
    # Try to resolve relative paths
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
                mime = (
                    "image/png" if ext == ".png"
                    else "image/jpeg" if ext in (".jpg", ".jpeg")
                    else "image/gif" if ext == ".gif"
                    else "image/webp" if ext == ".webp"
                    else "image/jpeg"
                )
            
            filename = path_obj.name or f"image-{int(time.time())}{client._ext_from_mime(mime)}"
            
            # Create signed asset
            signed = client.create_signed_asset(filename, asset_folder_id)
            payload = signed.get("data") or signed
            
            # Upload to S3
            client.upload_asset_from_bytes(payload, file_bytes, filename, mime)
            
            # Get asset key
            key = (payload.get("fields") or {}).get("key")
            if not key:
                raise RuntimeError("No asset key")
            
            asset_obj = {
                "filename": f"https://a.storyblok.com/{key}",
                "fieldtype": "asset"
            }
            
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


def create_storyblok_story(
    client: StoryblokClient,
    title: str,
    description: str,
    image_asset: Optional[dict],
    parent_id: int = 0,
    publish: bool = False
) -> Optional[dict]:
    """Create story in Storyblok with retry logic."""
    base_slug = slugify(title)
    
    for slug in [base_slug, f"{base_slug}-{random.randint(1000, 9999)}", f"{base_slug}-{int(time.time())}"]:
        try:
            content = {
                "component": CONTENT_TYPE,
                FIELD_TITLE: title,
                FIELD_DESCRIPTION: description
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


def run_upload(
    json_paths: List[Path],
    logger: logging.Logger,
    *,
    publish: bool = False,
    asset_folder_id: Optional[int] = None,
) -> None:
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
    content_parent_id = client.ensure_content_folder_by_path(CONTENT_PATH)
    logger.info(f"Content folder ID: {content_parent_id}")
    
    uploaded_count = 0
    failed_count = 0
    
    for jp in json_paths:
        try:
            data = json.loads(jp.read_text(encoding="utf-8"))
        except Exception as e:
            logger.error(f"Skip {jp}: {e}")
            failed_count += 1
            continue
        
        title = (data.get("page_title") or "").strip()
        if not title:
            logger.error(f"Skip {jp}: no title")
            failed_count += 1
            continue
        
        # Combine description and body content
        description = (data.get("body_content", {}).get("main_paragraphs") or "").strip()
        if not description:
            logger.warning(f"No description for {title}")
        
        # Get hero image
        hero = data.get("hero_image")
        hero_path = None
        
        if hero:
            # Try to find image in output folder
            for p in [jp.parent / hero, jp.parent.parent / hero]:
                if p.exists():
                    hero_path = str(p)
                    break
            if not hero_path:
                hero_path = hero
        
        logger.info(f"Uploading: {title[:50]}...")
        
        # Upload image if present
        image_asset = upload_image_to_storyblok(client, hero_path, asset_folder_id) if hero_path else None
        
        # Create story
        story = create_storyblok_story(
            client,
            title,
            description,
            image_asset,
            parent_id=content_parent_id,
            publish=publish
        )
        
        if story:
            logger.info(f"  ✓ {story.get('id')} https://app.storyblok.com/#/me/spaces/{space_id}/stories/0/0/{story.get('id')}")
            uploaded_count += 1
        else:
            logger.error(f"  ✗ Failed to create story")
            failed_count += 1
    
    logger.info(f"\nUpload complete: {uploaded_count} successful, {failed_count} failed")


# ----------------------------
# Main
# ----------------------------
def main():
    """Main execution."""
    script_dir = Path(__file__).resolve().parent
    
    # Load environment variables
    for env_path in [script_dir / ".env", script_dir.parent / ".env", Path.cwd() / ".env"]:
        _load_env_file(env_path)
    
    if load_dotenv:
        load_dotenv(script_dir / ".env")
        load_dotenv(script_dir.parent / ".env")
        load_dotenv()
    
    ap = argparse.ArgumentParser(description="Upload AKUH health services to Storyblok")
    ap.add_argument("--folder", required=True, help="Output folder from scraper (e.g., output_2026-02-16_131021)")
    ap.add_argument("--publish", action="store_true", help="Publish stories (default: draft)")
    ap.add_argument("--asset-folder-id", type=int, help="Storyblok asset folder ID for images")
    
    args = ap.parse_args()
    
    logger = setup_logging()
    
    # Find JSON files
    folder_path = Path(args.folder)
    
    if not folder_path.exists():
        logger.error(f"Folder not found: {folder_path}")
        sys.exit(1)
    
    # Get all JSON files except metadata
    json_paths = [p for p in folder_path.glob("*.json") if p.name != "metadata.json"]
    
    if not json_paths:
        logger.error("No JSON files to upload")
        sys.exit(1)
    
    logger.info(f"Found {len(json_paths)} files to upload\n")
    
    # Upload
    run_upload(json_paths, logger, publish=args.publish, asset_folder_id=args.asset_folder_id)


if __name__ == "__main__":
    main()
