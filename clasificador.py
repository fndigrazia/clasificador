import argparse
import json
import os
import sys
import time
import threading
from concurrent.futures import ThreadPoolExecutor, as_completed

from scraper import normalize_url, scrape_url
from llm_providers import GeminiProvider, OllamaProvider, GroqProvider


def load_urls(filepath: str) -> list[str]:
    """Load and normalize URLs from file, skipping invalid ones."""
    urls = []
    with open(filepath) as f:
        for line in f:
            url = normalize_url(line)
            if url:
                urls.append(url)
    return urls


def load_existing_results(filepath: str) -> list[dict]:
    """Load existing results for resume capability."""
    try:
        with open(filepath) as f:
            return json.load(f)
    except (FileNotFoundError, json.JSONDecodeError):
        return []


def save_results(results: list[dict], filepath: str):
    """Save results as formatted JSON."""
    with open(filepath, "w") as f:
        json.dump(results, f, indent=2, ensure_ascii=False)


def create_provider(args):
    """Create the appropriate LLM provider based on CLI args."""
    if args.provider == "gemini":
        api_key = args.api_key or os.environ.get("GEMINI_API_KEY")
        if not api_key:
            print("Error: --api-key or GEMINI_API_KEY env var required for Gemini provider")
            sys.exit(1)
        model = args.model or "gemini-2.0-flash"
        return GeminiProvider(api_key=api_key, model=model)
    elif args.provider == "groq":
        api_key = args.api_key or os.environ.get("GROQ_API_KEY")
        if not api_key:
            print("Error: --api-key or GROQ_API_KEY env var required for Groq provider")
            sys.exit(1)
        model = args.model or "llama-3.3-70b-versatile"
        return GroqProvider(api_key=api_key, model=model)
    else:
        model = args.model or "llama3"
        return OllamaProvider(base_url=args.ollama_url, model=model)


def parse_args():
    parser = argparse.ArgumentParser(
        description="Classify URLs using IAB 1.0 taxonomy via LLM"
    )
    parser.add_argument(
        "--provider", required=True, choices=["gemini", "ollama", "groq"],
        help="LLM provider to use"
    )
    parser.add_argument(
        "--model", default=None,
        help="Model name (default: gemini-2.0-flash for Gemini, llama-3.3-70b-versatile for Groq, llama3 for Ollama)"
    )
    parser.add_argument(
        "--input", default="urls.txt",
        help="Path to URLs file (default: urls.txt)"
    )
    parser.add_argument(
        "--output", default="results.json",
        help="Path to output JSON (default: results.json)"
    )
    parser.add_argument(
        "--api-key", default=None,
        help="API key for Gemini (GEMINI_API_KEY) or Groq (GROQ_API_KEY)"
    )
    parser.add_argument(
        "--ollama-url", default="http://localhost:11434",
        help="Ollama base URL (default: http://localhost:11434)"
    )
    parser.add_argument(
        "--delay", type=float, default=1.0,
        help="Seconds between LLM requests (default: 1.0)"
    )
    parser.add_argument(
        "--timeout", type=int, default=15,
        help="Scraping timeout in seconds (default: 15)"
    )
    parser.add_argument(
        "--retry-errors", action="store_true",
        help="Re-process URLs that previously failed (scrape or LLM errors)"
    )
    parser.add_argument(
        "--max-requests", type=int, default=0,
        help="Max number of LLM requests to make (0 = unlimited)"
    )
    parser.add_argument(
        "--workers", type=int, default=1,
        help="Number of parallel workers (default: 1)"
    )
    return parser.parse_args()


def process_url(url: str, provider, timeout: int) -> dict:
    """Scrape and classify a single URL. Thread-safe."""
    scraped = scrape_url(url, timeout=timeout)

    if scraped.get("error"):
        return {
            "url": url,
            "scrape_error": scraped["error"],
            "site_cat": None,
            "site_pagecat": None,
            "site_content_cat": None,
            "site_content_language": scraped.get("language_hint"),
            "site_content_keywords": None,
            "site_content_title": scraped.get("title"),
        }

    try:
        classification = provider.classify(scraped)
        return {"url": url, "scrape_error": None, **classification}
    except Exception as e:
        return {
            "url": url,
            "llm_error": str(e),
            "site_cat": None,
            "site_pagecat": None,
            "site_content_cat": None,
            "site_content_language": scraped.get("language_hint"),
            "site_content_keywords": None,
            "site_content_title": scraped.get("title"),
        }


def main():
    args = parse_args()

    # Load URLs
    urls = load_urls(args.input)
    print(f"Loaded {len(urls)} URLs from {args.input}")

    # Load existing results for resume
    results = load_existing_results(args.output)

    if args.retry_errors:
        error_urls = {r["url"] for r in results if r.get("llm_error") or r.get("scrape_error")}
        results = [r for r in results if r["url"] not in error_urls]
        processed_urls = {r["url"] for r in results}
        print(f"Retrying {len(error_urls)} previously failed URLs")
    else:
        processed_urls = {r["url"] for r in results}

    # Filter pending
    pending = [u for u in urls if u not in processed_urls]
    print(f"Already processed: {len(processed_urls)}, Pending: {len(pending)}")

    if not pending:
        print("Nothing to process.")
        return

    # Apply max-requests limit
    if args.max_requests > 0 and len(pending) > args.max_requests:
        pending = pending[:args.max_requests]
        print(f"Limited to {args.max_requests} URLs by --max-requests")

    # Initialize provider
    provider = create_provider(args)
    print(f"Using provider: {args.provider} (model: {args.model or 'default'}), workers: {args.workers}")
    print()

    lock = threading.Lock()
    completed_count = 0

    if args.workers > 1:
        # Parallel execution
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_url, url, provider, args.timeout): url
                for url in pending
            }
            for future in as_completed(futures):
                entry = future.result()
                with lock:
                    results.append(entry)
                    completed_count += 1
                    cats = entry.get("site_cat", [])
                    error = entry.get("scrape_error") or entry.get("llm_error")
                    status = f"-> {cats}" if cats else f"ERROR: {str(error)[:60]}"
                    print(f"[{completed_count}/{len(pending)}] {entry['url']}  {status}")

                    if completed_count % 10 == 0:
                        save_results(results, args.output)

            # Final save
            save_results(results, args.output)
    else:
        # Sequential execution
        llm_requests = 0
        for i, url in enumerate(pending):
            print(f"[{i + 1}/{len(pending)}] {url}")

            entry = process_url(url, provider, args.timeout)

            if entry.get("site_cat"):
                llm_requests += 1
                print(f"  -> {entry['site_cat']} (LLM req #{llm_requests})")
            elif entry.get("scrape_error"):
                print(f"  Scrape error: {entry['scrape_error']}")
            else:
                print(f"  LLM error: {entry.get('llm_error', 'unknown')}")

            results.append(entry)
            save_results(results, args.output)

            if args.delay > 0 and i < len(pending) - 1:
                time.sleep(args.delay)

    print(f"\nDone. {len(results)} results saved to {args.output}")


if __name__ == "__main__":
    main()
