import json
import time
import re
from abc import ABC, abstractmethod

import requests
from google import genai
from google.genai import types

MAX_RETRIES = 3


IAB_TAXONOMY = (
    "IAB1=Arts & Entertainment, IAB2=Automotive, IAB3=Business, IAB4=Careers, "
    "IAB5=Education, IAB6=Family & Parenting, IAB7=Health & Fitness, "
    "IAB8=Food & Drink, IAB9=Hobbies & Interests, IAB10=Home & Garden, "
    "IAB11=Law/Gov't & Politics, IAB12=News, IAB13=Personal Finance, "
    "IAB14=Society, IAB15=Science, IAB16=Pets, IAB17=Sports, "
    "IAB18=Style & Fashion, IAB19=Technology & Computing, IAB20=Travel, "
    "IAB21=Real Estate, IAB22=Shopping, IAB23=Religion & Spirituality, "
    "IAB24=Uncategorized, IAB25=Non-Standard Content, IAB26=Illegal Content"
)

EXPECTED_FIELDS = [
    "site_cat", "site_pagecat", "site_content_cat",
    "site_content_language", "site_content_keywords", "site_content_title",
]


def build_prompt(scraped_data: dict) -> str:
    """Build the classification prompt from scraped page data."""
    parts = []
    if scraped_data.get("title"):
        parts.append(f"Page title: {scraped_data['title']}")
    if scraped_data.get("description"):
        parts.append(f"Meta description: {scraped_data['description']}")
    if scraped_data.get("meta_keywords"):
        parts.append(f"Meta keywords: {scraped_data['meta_keywords']}")
    if scraped_data.get("og_tags"):
        for k, v in scraped_data["og_tags"].items():
            parts.append(f"OG {k}: {v}")
    if scraped_data.get("text_content"):
        text = scraped_data["text_content"][:3000]
        parts.append(f"Page content:\n{text}")
    if scraped_data.get("language_hint"):
        parts.append(f"Detected language: {scraped_data['language_hint']}")

    page_context = "\n".join(parts)

    return f"""You are an expert content classifier for the digital advertising industry.

Analyze the following web page data and classify it according to the IAB Tech Lab Content Taxonomy 1.0.

{page_context}

Respond with a JSON object containing exactly these fields:

- "site_cat": array of IAB 1.0 category codes for the overall site (e.g., ["IAB12"] for News). Use the top-level category. Provide 1-3 codes.
- "site_pagecat": array of IAB 1.0 category codes for this specific page (e.g., ["IAB12-2"] for National News). Be as specific as possible using subcategories. Provide 1-3 codes.
- "site_content_cat": array of IAB 1.0 category codes for the content itself (e.g., ["IAB12-2"]). This should reflect the actual topic of the article/content. Provide 1-3 codes.
- "site_content_language": ISO 639-1 two-letter language code (e.g., "en", "es", "de", "pt").
- "site_content_keywords": comma-separated string of 3-8 relevant keywords describing the content.
- "site_content_title": the most appropriate title for this content. Use the page title if suitable, otherwise generate a descriptive one.

IAB 1.0 Top-Level Categories for reference:
{IAB_TAXONOMY}

Important rules:
- Always use the "IABx" or "IABx-y" format for category codes.
- If the content is clearly not suitable for advertising (adult content, illegal content, etc.), use IAB25 or IAB26.
- If there is insufficient information to classify, use IAB24 (Uncategorized).
- For games/puzzles sites, use IAB9-18 (Video & Computer Games) or IAB9 (Hobbies & Interests).
- For email/webmail pages, use IAB19 (Technology & Computing).
- Respond ONLY with the JSON object, no additional text."""


def parse_llm_response(raw_text: str) -> dict:
    """Parse LLM JSON response, handling common formatting issues."""
    text = raw_text.strip()

    # Strip markdown code fences if present
    if text.startswith("```"):
        text = text.split("\n", 1)[1] if "\n" in text else text[3:]
        if text.endswith("```"):
            text = text[:-3]
        text = text.strip()
        if text.startswith("json"):
            text = text[4:].strip()

    result = json.loads(text)

    for field in EXPECTED_FIELDS:
        if field not in result:
            result[field] = None

    return result


class LLMProvider(ABC):
    @abstractmethod
    def classify(self, scraped_data: dict) -> dict:
        """Classify scraped page data. Returns dict with IAB fields."""
        ...


class GeminiProvider(LLMProvider):
    def __init__(self, api_key: str, model: str = "gemini-2.0-flash"):
        self.client = genai.Client(api_key=api_key)
        self.model = model

    def classify(self, scraped_data: dict) -> dict:
        prompt = build_prompt(scraped_data)
        for attempt in range(MAX_RETRIES):
            try:
                response = self.client.models.generate_content(
                    model=self.model,
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        response_mime_type="application/json",
                        temperature=0.1,
                    ),
                )
                return parse_llm_response(response.text)
            except Exception as e:
                error_str = str(e)
                if "429" in error_str and attempt < MAX_RETRIES - 1:
                    # Extract retry delay from error or use exponential backoff
                    match = re.search(r'retryDelay.*?(\d+)s', error_str)
                    wait = int(match.group(1)) if match else 30 * (attempt + 1)
                    print(f"  Rate limited, waiting {wait}s (retry {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(wait)
                else:
                    raise


class OllamaProvider(LLMProvider):
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3"):
        self.base_url = base_url.rstrip("/")
        self.model = model

    def classify(self, scraped_data: dict) -> dict:
        prompt = build_prompt(scraped_data)
        response = requests.post(
            f"{self.base_url}/api/generate",
            json={
                "model": self.model,
                "prompt": prompt,
                "stream": False,
                "format": "json",
                "options": {"temperature": 0.1},
            },
            timeout=120,
        )
        response.raise_for_status()
        return parse_llm_response(response.json()["response"])


class GroqProvider(LLMProvider):
    def __init__(self, api_key: str, model: str = "llama-3.3-70b-versatile"):
        self.api_key = api_key
        self.model = model

    def classify(self, scraped_data: dict) -> dict:
        prompt = build_prompt(scraped_data)
        for attempt in range(MAX_RETRIES):
            try:
                response = requests.post(
                    "https://api.groq.com/openai/v1/chat/completions",
                    headers={
                        "Authorization": f"Bearer {self.api_key}",
                        "Content-Type": "application/json",
                    },
                    json={
                        "model": self.model,
                        "messages": [{"role": "user", "content": prompt}],
                        "temperature": 0.1,
                        "response_format": {"type": "json_object"},
                    },
                    timeout=120,
                )
                if response.status_code == 429 and attempt < MAX_RETRIES - 1:
                    retry_after = int(response.headers.get("retry-after", 30 * (attempt + 1)))
                    print(f"  Rate limited, waiting {retry_after}s (retry {attempt + 1}/{MAX_RETRIES})...")
                    time.sleep(retry_after)
                    continue
                response.raise_for_status()
                text = response.json()["choices"][0]["message"]["content"]
                return parse_llm_response(text)
            except requests.exceptions.HTTPError:
                if attempt < MAX_RETRIES - 1 and response.status_code == 429:
                    continue
                raise
