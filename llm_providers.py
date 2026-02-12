import json
import time
import re
from abc import ABC, abstractmethod

import requests
from google import genai
from google.genai import types

MAX_RETRIES = 3


IAB_TAXONOMY = json.dumps({
    "IAB1": {"name": "Arts & Entertainment", "sub": {
        "IAB1-1": "Books & Literature", "IAB1-2": "Celebrity Fan/Gossip",
        "IAB1-3": "Fine Art", "IAB1-4": "Humor", "IAB1-5": "Movies",
        "IAB1-6": "Music", "IAB1-7": "Television"
    }},
    "IAB2": {"name": "Automotive", "sub": {
        "IAB2-1": "Auto Parts", "IAB2-2": "Auto Repair",
        "IAB2-3": "Buying/Selling Cars", "IAB2-4": "Car Culture",
        "IAB2-5": "Certified Pre-Owned", "IAB2-6": "Convertible",
        "IAB2-7": "Coupe", "IAB2-8": "Crossover", "IAB2-9": "Diesel",
        "IAB2-10": "Electric Vehicle", "IAB2-11": "Hatchback",
        "IAB2-12": "Hybrid", "IAB2-13": "Luxury", "IAB2-14": "MiniVan",
        "IAB2-15": "Motorcycles", "IAB2-16": "Off-Road Vehicles",
        "IAB2-17": "Performance Vehicles", "IAB2-18": "Pickup",
        "IAB2-19": "Road-Side Assistance", "IAB2-20": "Sedan",
        "IAB2-21": "Trucks & Accessories", "IAB2-22": "Vintage Cars",
        "IAB2-23": "Wagon"
    }},
    "IAB3": {"name": "Business", "sub": {
        "IAB3-1": "Advertising", "IAB3-2": "Agriculture",
        "IAB3-3": "Biotech/Biomedical", "IAB3-4": "Business Software",
        "IAB3-5": "Construction", "IAB3-6": "Forestry",
        "IAB3-7": "Government", "IAB3-8": "Green Solutions",
        "IAB3-9": "Human Resources", "IAB3-10": "Logistics",
        "IAB3-11": "Marketing", "IAB3-12": "Metals"
    }},
    "IAB4": {"name": "Careers", "sub": {
        "IAB4-1": "Career Planning", "IAB4-2": "College",
        "IAB4-3": "Financial Aid", "IAB4-4": "Job Fairs",
        "IAB4-5": "Job Search", "IAB4-6": "Resume Writing/Advice",
        "IAB4-7": "Nursing", "IAB4-8": "Scholarships",
        "IAB4-9": "Telecommuting", "IAB4-10": "U.S. Military",
        "IAB4-11": "Career Advice"
    }},
    "IAB5": {"name": "Education", "sub": {
        "IAB5-1": "7-12 Education", "IAB5-2": "Adult Education",
        "IAB5-3": "Art History", "IAB5-4": "College Administration",
        "IAB5-5": "College Life", "IAB5-6": "Distance Learning",
        "IAB5-7": "English as a 2nd Language", "IAB5-8": "Language Learning",
        "IAB5-9": "Graduate School", "IAB5-10": "Homeschooling",
        "IAB5-11": "Homework/Study Tips", "IAB5-12": "K-6 Educators",
        "IAB5-13": "Private School", "IAB5-14": "Special Education",
        "IAB5-15": "Studying Business"
    }},
    "IAB6": {"name": "Family & Parenting", "sub": {
        "IAB6-1": "Adoption", "IAB6-2": "Babies & Toddlers",
        "IAB6-3": "Daycare/Pre School", "IAB6-4": "Family Internet",
        "IAB6-5": "Parenting - K-6 Kids", "IAB6-6": "Parenting Teens",
        "IAB6-7": "Pregnancy", "IAB6-8": "Special Needs Kids",
        "IAB6-9": "Eldercare"
    }},
    "IAB7": {"name": "Health & Fitness", "sub": {
        "IAB7-1": "Exercise", "IAB7-2": "A.D.D.", "IAB7-3": "AIDS/HIV",
        "IAB7-4": "Allergies", "IAB7-5": "Alternative Medicine",
        "IAB7-6": "Arthritis", "IAB7-7": "Asthma", "IAB7-8": "Autism/PDD",
        "IAB7-9": "Bipolar Disorder", "IAB7-10": "Brain Tumor",
        "IAB7-11": "Cancer", "IAB7-12": "Cholesterol",
        "IAB7-13": "Chronic Fatigue Syndrome", "IAB7-14": "Chronic Pain",
        "IAB7-15": "Cold & Flu", "IAB7-16": "Deafness",
        "IAB7-17": "Dental Care", "IAB7-18": "Depression",
        "IAB7-19": "Dermatology", "IAB7-20": "Diabetes",
        "IAB7-21": "Epilepsy", "IAB7-22": "GERD/Acid Reflux",
        "IAB7-23": "Headaches/Migraines", "IAB7-24": "Heart Disease",
        "IAB7-25": "Herbs for Health", "IAB7-26": "Holistic Healing",
        "IAB7-27": "IBS/Crohn's Disease", "IAB7-28": "Incest/Abuse Support",
        "IAB7-29": "Incontinence", "IAB7-30": "Infertility",
        "IAB7-31": "Men's Health", "IAB7-32": "Nutrition",
        "IAB7-33": "Orthopedics", "IAB7-34": "Panic/Anxiety Disorders",
        "IAB7-35": "Pediatrics", "IAB7-36": "Physical Therapy",
        "IAB7-37": "Psychology/Psychiatry", "IAB7-38": "Senior Health",
        "IAB7-39": "Sexuality", "IAB7-40": "Sleep Disorders",
        "IAB7-41": "Smoking Cessation", "IAB7-42": "Substance Abuse",
        "IAB7-43": "Thyroid Disease", "IAB7-44": "Weight Loss",
        "IAB7-45": "Women's Health"
    }},
    "IAB8": {"name": "Food & Drink", "sub": {
        "IAB8-1": "American Cuisine", "IAB8-2": "Barbecues & Grilling",
        "IAB8-3": "Cajun/Creole", "IAB8-4": "Chinese Cuisine",
        "IAB8-5": "Cocktails/Beer", "IAB8-6": "Coffee/Tea",
        "IAB8-7": "Cuisine-Specific", "IAB8-8": "Desserts & Baking",
        "IAB8-9": "Dining Out", "IAB8-10": "Food Allergies",
        "IAB8-11": "French Cuisine", "IAB8-12": "Health/Lowfat Cooking",
        "IAB8-13": "Italian Cuisine", "IAB8-14": "Japanese Cuisine",
        "IAB8-15": "Mexican Cuisine", "IAB8-16": "Vegan",
        "IAB8-17": "Vegetarian", "IAB8-18": "Wine"
    }},
    "IAB9": {"name": "Hobbies & Interests", "sub": {
        "IAB9-1": "Art/Technology", "IAB9-2": "Arts & Crafts",
        "IAB9-3": "Beadwork", "IAB9-4": "Birdwatching",
        "IAB9-5": "Board Games/Puzzles", "IAB9-6": "Candle & Soap Making",
        "IAB9-7": "Card Games", "IAB9-8": "Chess", "IAB9-9": "Cigars",
        "IAB9-10": "Collecting", "IAB9-11": "Comic Books",
        "IAB9-12": "Drawing/Sketching", "IAB9-13": "Freelance Writing",
        "IAB9-14": "Genealogy", "IAB9-15": "Getting Published",
        "IAB9-16": "Guitar", "IAB9-17": "Home Recording",
        "IAB9-18": "Investors & Patents", "IAB9-19": "Jewelry Making",
        "IAB9-20": "Magic & Illusion", "IAB9-21": "Needlework",
        "IAB9-22": "Painting", "IAB9-23": "Photography",
        "IAB9-24": "Radio", "IAB9-25": "Roleplaying Games",
        "IAB9-26": "Sci-Fi & Fantasy", "IAB9-27": "Scrapbooking",
        "IAB9-28": "Screenwriting", "IAB9-29": "Stamps & Coins",
        "IAB9-30": "Video & Computer Games", "IAB9-31": "Woodworking"
    }},
    "IAB10": {"name": "Home & Garden", "sub": {
        "IAB10-1": "Appliances", "IAB10-2": "Entertaining",
        "IAB10-3": "Environmental Safety", "IAB10-4": "Gardening",
        "IAB10-5": "Home Repair", "IAB10-6": "Home Theater",
        "IAB10-7": "Interior Decorating", "IAB10-8": "Landscaping",
        "IAB10-9": "Remodeling & Construction"
    }},
    "IAB11": {"name": "Law, Gov't & Politics", "sub": {
        "IAB11-1": "Immigration", "IAB11-2": "Legal Issues",
        "IAB11-3": "U.S. Government Resources", "IAB11-4": "Politics",
        "IAB11-5": "Commentary"
    }},
    "IAB12": {"name": "News", "sub": {
        "IAB12-1": "International News", "IAB12-2": "National News",
        "IAB12-3": "Local News"
    }},
    "IAB13": {"name": "Personal Finance", "sub": {
        "IAB13-1": "Beginning Investing", "IAB13-2": "Credit/Debt & Loans",
        "IAB13-3": "Financial News", "IAB13-4": "Financial Planning",
        "IAB13-5": "Hedge Fund", "IAB13-6": "Insurance",
        "IAB13-7": "Investing", "IAB13-8": "Mutual Funds",
        "IAB13-9": "Options", "IAB13-10": "Retirement Planning",
        "IAB13-11": "Stocks", "IAB13-12": "Tax Planning"
    }},
    "IAB14": {"name": "Society", "sub": {
        "IAB14-1": "Dating", "IAB14-2": "Divorce Support",
        "IAB14-3": "Gay Life", "IAB14-4": "Marriage",
        "IAB14-5": "Senior Living", "IAB14-6": "Teens",
        "IAB14-7": "Weddings", "IAB14-8": "Ethnic Specific"
    }},
    "IAB15": {"name": "Science", "sub": {
        "IAB15-1": "Astrology", "IAB15-2": "Biology",
        "IAB15-3": "Chemistry", "IAB15-4": "Geology",
        "IAB15-5": "Paranormal Phenomena", "IAB15-6": "Physics",
        "IAB15-7": "Space/Astronomy", "IAB15-8": "Geography",
        "IAB15-9": "Botany", "IAB15-10": "Weather"
    }},
    "IAB16": {"name": "Pets", "sub": {
        "IAB16-1": "Aquariums", "IAB16-2": "Birds", "IAB16-3": "Cats",
        "IAB16-4": "Dogs", "IAB16-5": "Large Animals",
        "IAB16-6": "Reptiles", "IAB16-7": "Veterinary Medicine"
    }},
    "IAB17": {"name": "Sports", "sub": {
        "IAB17-1": "Auto Racing", "IAB17-2": "Baseball",
        "IAB17-3": "Bicycling", "IAB17-4": "Bodybuilding",
        "IAB17-5": "Boxing", "IAB17-6": "Canoeing/Kayaking",
        "IAB17-7": "Cheerleading", "IAB17-8": "Climbing",
        "IAB17-9": "Cricket", "IAB17-10": "Figure Skating",
        "IAB17-11": "Fly Fishing", "IAB17-12": "Football",
        "IAB17-13": "Freshwater Fishing", "IAB17-14": "Game & Fish",
        "IAB17-15": "Golf", "IAB17-16": "Horse Racing",
        "IAB17-17": "Horses", "IAB17-18": "Hunting/Shooting",
        "IAB17-19": "Inline Skating", "IAB17-20": "Martial Arts",
        "IAB17-21": "Mountain Biking", "IAB17-22": "NASCAR Racing",
        "IAB17-23": "Olympics", "IAB17-24": "Paintball",
        "IAB17-25": "Power & Motorcycles", "IAB17-26": "Pro Basketball",
        "IAB17-27": "Pro Ice Hockey", "IAB17-28": "Rodeo",
        "IAB17-29": "Rugby", "IAB17-30": "Running/Jogging",
        "IAB17-31": "Sailing", "IAB17-32": "Saltwater Fishing",
        "IAB17-33": "Scuba Diving", "IAB17-34": "Skateboarding",
        "IAB17-35": "Skiing", "IAB17-36": "Snowboarding",
        "IAB17-37": "Surfing/Bodyboarding", "IAB17-38": "Swimming",
        "IAB17-39": "Table Tennis/Ping-Pong", "IAB17-40": "Tennis",
        "IAB17-41": "Volleyball", "IAB17-42": "Walking",
        "IAB17-43": "Waterski/Wakeboard", "IAB17-44": "World Soccer"
    }},
    "IAB18": {"name": "Style & Fashion", "sub": {
        "IAB18-1": "Beauty", "IAB18-2": "Body Art",
        "IAB18-3": "Fashion", "IAB18-4": "Jewelry",
        "IAB18-5": "Clothing", "IAB18-6": "Accessories"
    }},
    "IAB19": {"name": "Technology & Computing", "sub": {
        "IAB19-1": "3-D Graphics", "IAB19-2": "Animation",
        "IAB19-3": "Antivirus Software", "IAB19-4": "C/C++",
        "IAB19-5": "Cameras & Camcorders", "IAB19-6": "Cell Phones",
        "IAB19-7": "Computer Certification", "IAB19-8": "Computer Networking",
        "IAB19-9": "Computer Peripherals", "IAB19-10": "Computer Reviews",
        "IAB19-11": "Data Centers", "IAB19-12": "Databases",
        "IAB19-13": "Desktop Publishing", "IAB19-14": "Desktop Video",
        "IAB19-15": "Email", "IAB19-16": "Graphics Software",
        "IAB19-17": "Home Video/DVD", "IAB19-18": "Internet Technology",
        "IAB19-19": "Java", "IAB19-20": "JavaScript",
        "IAB19-21": "Mac Support", "IAB19-22": "MP3/MIDI",
        "IAB19-23": "Net Conferencing", "IAB19-24": "Net for Beginners",
        "IAB19-25": "Network Security", "IAB19-26": "Palmtops/PDAs",
        "IAB19-27": "PC Support", "IAB19-28": "Portable",
        "IAB19-29": "Entertainment", "IAB19-30": "Shareware/Freeware",
        "IAB19-31": "Unix", "IAB19-32": "Visual Basic",
        "IAB19-33": "Web Clip Art", "IAB19-34": "Web Design/HTML",
        "IAB19-35": "Web Search", "IAB19-36": "Windows"
    }},
    "IAB20": {"name": "Travel", "sub": {
        "IAB20-1": "Adventure Travel", "IAB20-2": "Africa",
        "IAB20-3": "Air Travel", "IAB20-4": "Australia & New Zealand",
        "IAB20-5": "Bed & Breakfasts", "IAB20-6": "Budget Travel",
        "IAB20-7": "Business Travel", "IAB20-8": "By US Locale",
        "IAB20-9": "Camping", "IAB20-10": "Canada",
        "IAB20-11": "Caribbean", "IAB20-12": "Cruises",
        "IAB20-13": "Eastern Europe", "IAB20-14": "Europe",
        "IAB20-15": "France", "IAB20-16": "Greece",
        "IAB20-17": "Honeymoons/Getaways", "IAB20-18": "Hotels",
        "IAB20-19": "Italy", "IAB20-20": "Japan",
        "IAB20-21": "Mexico & Central America", "IAB20-22": "National Parks",
        "IAB20-23": "South America", "IAB20-24": "Spas",
        "IAB20-25": "Theme Parks", "IAB20-26": "Traveling with Kids",
        "IAB20-27": "United Kingdom"
    }},
    "IAB21": {"name": "Real Estate", "sub": {
        "IAB21-1": "Apartments", "IAB21-2": "Architects",
        "IAB21-3": "Buying/Selling Homes"
    }},
    "IAB22": {"name": "Shopping", "sub": {
        "IAB22-1": "Contests & Freebies", "IAB22-2": "Couponing",
        "IAB22-3": "Comparison", "IAB22-4": "Engines"
    }},
    "IAB23": {"name": "Religion & Spirituality", "sub": {
        "IAB23-1": "Alternative Religions", "IAB23-2": "Atheism/Agnosticism",
        "IAB23-3": "Buddhism", "IAB23-4": "Catholicism",
        "IAB23-5": "Christianity", "IAB23-6": "Hinduism",
        "IAB23-7": "Islam", "IAB23-8": "Judaism",
        "IAB23-9": "Latter-Day Saints", "IAB23-10": "Pagan/Wiccan"
    }},
    "IAB24": {"name": "Uncategorized", "sub": {}},
    "IAB25": {"name": "Non-Standard Content", "sub": {
        "IAB25-1": "Unmoderated UGC",
        "IAB25-2": "Extreme Graphic/Explicit Violence",
        "IAB25-3": "Pornography", "IAB25-4": "Profane Content",
        "IAB25-5": "Hate Content", "IAB25-6": "Under Construction",
        "IAB25-7": "Incentivized"
    }},
    "IAB26": {"name": "Illegal Content", "sub": {
        "IAB26-1": "Illegal Content", "IAB26-2": "Warez",
        "IAB26-3": "Spyware/Malware", "IAB26-4": "Copyright Infringement"
    }}
}, indent=2)

EXPECTED_FIELDS = [
    "site_cat", "site_pagecat", "site_content_cat",
    "site_content_language", "site_content_keywords", "site_content_title",
]


def _build_page_context(scraped_data: dict) -> str:
    """Extract page data into a text block."""
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
    return "\n".join(parts)


def build_prompt_short(scraped_data: dict) -> str:
    """Build a short prompt for models that have the taxonomy in their system prompt (Modelfile)."""
    page_context = _build_page_context(scraped_data)
    return f"""Classify this web page. Respond ONLY with the JSON object.

{page_context}"""


def build_prompt(scraped_data: dict) -> str:
    """Build the full classification prompt including taxonomy (for models without a Modelfile)."""
    page_context = _build_page_context(scraped_data)

    return f"""You are an expert content classifier for the digital advertising industry.

Analyze the following web page data and classify it according to the IAB Tech Lab Content Taxonomy 1.0.

{page_context}

Respond with a JSON object containing exactly these fields:

- "site_cat": array of ALL applicable IAB 1.0 top-level category codes for the overall site (e.g., ["IAB12", "IAB3"]). Include every relevant category. Provide as many as apply.
- "site_pagecat": array of ALL applicable IAB 1.0 category codes for this specific page (e.g., ["IAB12-2", "IAB3-1"]). Be as specific as possible using subcategories. Provide as many as apply.
- "site_content_cat": array of ALL applicable IAB 1.0 category codes for the content itself (e.g., ["IAB12-2", "IAB9-18"]). This should reflect the actual topics of the article/content. Provide as many as apply.
- "site_content_language": ISO 639-1 two-letter language code (e.g., "en", "es", "de", "pt").
- "site_content_keywords": comma-separated string of 3-8 relevant keywords describing the content.
- "site_content_title": the most appropriate title for this content. Use the page title if suitable, otherwise generate a descriptive one.

IAB 1.0 Complete Taxonomy (categories and subcategories):
{IAB_TAXONOMY}

Important rules:
- Always use the "IABx" or "IABx-y" format for category codes.
- Assign ALL categories that are relevant. More categories is better than fewer. Be thorough.
- Classify based on the SUBJECT and TOPIC of the content, not the HTML format or metadata type.
- Do NOT confuse og:type "article" with news. A page about a TV show marked as "article" is still Arts & Entertainment, not News.
- For streaming, TV series, movies, and video content sites, use IAB1 (Arts & Entertainment).
- For games/puzzles sites, use IAB9-30 (Video & Computer Games) or IAB9 (Hobbies & Interests).
- For email/webmail pages, use IAB19 (Technology & Computing).
- If the content is clearly not suitable for advertising (adult content, illegal content, etc.), use IAB25 or IAB26.
- If there is insufficient information to classify, use IAB24 (Uncategorized).
- Respond ONLY with the JSON object, no additional text."""


CAT_FIELDS = ["site_cat", "site_pagecat", "site_content_cat"]


def _ensure_parent_cats(cats: list) -> list:
    """For any subcategory like IAB1-7, ensure the parent IAB1 is also present."""
    if not cats:
        return cats
    result = list(cats)
    for code in cats:
        if "-" in code:
            parent = code.split("-")[0]
            if parent not in result:
                result.append(parent)
    return result


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

    for field in CAT_FIELDS:
        if isinstance(result.get(field), list):
            result[field] = _ensure_parent_cats(result[field])

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
    def __init__(self, base_url: str = "http://localhost:11434", model: str = "llama3", use_short_prompt: bool = False):
        self.base_url = base_url.rstrip("/")
        self.model = model
        self.use_short_prompt = use_short_prompt

    def classify(self, scraped_data: dict) -> dict:
        prompt = build_prompt_short(scraped_data) if self.use_short_prompt else build_prompt(scraped_data)
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
