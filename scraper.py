import threading
import trafilatura
from bs4 import BeautifulSoup
from langdetect import detect, LangDetectException


def normalize_url(raw: str) -> str | None:
    """Normalize a URL line from the input file. Returns None if invalid."""
    url = raw.strip().strip('"')
    if not url or url in ("https://", "http://", "https", "http"):
        return None
    if not url.startswith(("http://", "https://")):
        url = "https://" + url
    return url


def _extract_metadata(html: str) -> dict:
    """Extract metadata from HTML using BeautifulSoup."""
    soup = BeautifulSoup(html, "html.parser")

    title = None
    tag = soup.find("title")
    if tag and tag.string:
        title = tag.string.strip()

    description = None
    tag = soup.find("meta", attrs={"name": "description"})
    if tag and tag.get("content"):
        description = tag["content"].strip()

    meta_keywords = None
    tag = soup.find("meta", attrs={"name": "keywords"})
    if tag and tag.get("content"):
        meta_keywords = tag["content"].strip()

    og_tags = {}
    for tag in soup.find_all("meta", attrs={"property": True}):
        prop = tag.get("property", "")
        if prop.startswith("og:") and tag.get("content"):
            og_tags[prop] = tag["content"].strip()

    language_hint = None
    html_tag = soup.find("html")
    if html_tag and html_tag.get("lang"):
        language_hint = html_tag["lang"].strip().split("-")[0].lower()

    return {
        "title": title,
        "description": description,
        "meta_keywords": meta_keywords,
        "og_tags": og_tags,
        "language_hint": language_hint,
    }


def _scrape_with_timeout(url: str, timeout: int, result: dict):
    """Run scraping in a thread with timeout support."""
    try:
        downloaded = trafilatura.fetch_url(url)
        if not downloaded:
            result["error"] = "Failed to download URL"
            return

        metadata = _extract_metadata(downloaded)
        result.update(metadata)

        text = trafilatura.extract(downloaded)
        if text:
            result["text_content"] = text[:4000]

        if not result["language_hint"] and result["text_content"]:
            try:
                result["language_hint"] = detect(result["text_content"])
            except LangDetectException:
                pass

    except Exception as e:
        result["error"] = str(e)


def scrape_url(url: str, timeout: int = 15) -> dict:
    """
    Scrape a URL and extract structured content.

    Returns a dict with: url, title, description, og_tags, meta_keywords,
    text_content, language_hint, error. Never raises exceptions.
    Works in both main thread and worker threads.
    """
    result = {
        "url": url,
        "title": None,
        "description": None,
        "og_tags": {},
        "meta_keywords": None,
        "text_content": None,
        "language_hint": None,
        "error": None,
    }

    thread = threading.Thread(target=_scrape_with_timeout, args=(url, timeout, result))
    thread.start()
    thread.join(timeout=timeout)

    if thread.is_alive():
        result["error"] = f"Timed out after {timeout}s"

    return result
