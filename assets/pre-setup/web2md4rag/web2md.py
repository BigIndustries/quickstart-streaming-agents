#!/usr/bin/env python3
"""
Fetch a web page and save it as a Markdown file.

Usage:
    python web2md.py <url> <output_dir>

Examples:
    python3 assets/pre-setup/web2md4rag/web2md.py https://www.bigindustries.be assets/lab3/ragData/
    python3 assets/pre-setup/web2md4rag/web2md.py https://www.bigindustries.be/services assets/lab3/ragData/
"""
import argparse
import re
import sys
from pathlib import Path
from urllib.parse import urlparse

import html2text
import requests


def url_to_filename(url: str) -> str:
    """Derive a safe .md filename from a URL path."""
    path = urlparse(url).path.rstrip("/")
    slug = path.split("/")[-1] if path else urlparse(url).netloc
    # Strip query-string fragments and file extensions, then sanitise
    slug = slug.split("?")[0].split("#")[0]
    slug = re.sub(r"[^\w\-]", "-", slug).strip("-") or "page"
    return f"{slug}.md"


def fetch_and_convert(url: str) -> str:
    """Fetch a page and return its Markdown representation."""
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return html2text.html2text(response.text)


def main():
    parser = argparse.ArgumentParser(
        description="Fetch a web page and save it as a Markdown file.",
        epilog="Example: python web2md.py https://example.com/page ./output",
    )
    parser.add_argument("url", help="URL of the page to fetch")
    parser.add_argument("output_dir", help="Directory to save the Markdown file")
    args = parser.parse_args()

    output_path = Path(args.output_dir)
    output_path.mkdir(parents=True, exist_ok=True)

    filename = url_to_filename(args.url)
    filepath = output_path / filename

    print(f"Fetching: {args.url}")
    try:
        markdown = fetch_and_convert(args.url)
    except requests.RequestException as e:
        print(f"ERROR: {e}", file=sys.stderr)
        sys.exit(1)

    filepath.write_text(markdown, encoding="utf-8")
    print(f"  ✓ Saved: {filepath}")


if __name__ == "__main__":
    main()
