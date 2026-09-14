#!/usr/bin/env python3
"""
doi_utils.py

Utility functions for validating, normalizing, and extracting DOIs from text.

Functions:
- validate(doi_str, strict=False)
- validate_part(doi_part)
- normalize(doi_str, strict=False)
- build_url(doi_str, strict=False)
- extract(text, strict=False)
- is_open_funder_registry(doi_str)

Includes special handling:
- eLife article URLs -> 10.7554/eLife.<id>
- Zenodo record URLs -> 10.5281/zenodo.<id>

CLI:
- Run with text arguments or pipe text in; prints normalized DOIs (one per line).
"""

import re
import sys
from typing import List, Optional, Set

# DOI "suffix" allowed characters are broad; Crossref recommends:
# 10.\d{4,9}/[-._;()/:A-Z0-9]+ (case-insensitive)
# We'll use a practical, tolerant pattern for extraction and a stricter one for validation.
_DOI_PART_RE = re.compile(r'^10\.\d{4,9}/\S+$', re.IGNORECASE)
# For extraction, allow many non-whitespace, non-<>"' parentheses-close chars, but we'll strip trailing punctuation later.
_DOI_EXTRACT_RE = re.compile(
    r'(?:doi:\s*|https?://(?:dx\.)?doi\.org/)?(10\.\d{4,9}/[^\s"<>\)\]\;:,]+)',
    re.IGNORECASE
)

# Recognize doi.org-style URLs and dx.doi.org
_DOI_URL_RE = re.compile(r'https?://(?:dx\.)?doi\.org/([^?\s#]+)', re.IGNORECASE)

# eLife and Zenodo special patterns
_ELIFE_URL_RE = re.compile(r'https?://(?:www\.)?elifesciences\.org/articles/(\d+)', re.IGNORECASE)
_ZENODO_RECORD_URL_RE = re.compile(r'https?://(?:www\.)?zenodo\.org/records?/(\d+)', re.IGNORECASE)

# Characters to strip off the end of extracted tokens (common trailing punctuation)
_TRAILING_PUNCT = '.,;:)\]\}>'

def _strip_trailing(token: str) -> str:
    # remove trailing punctuation that commonly appends to DOIs in running text
    while token and token[-1] in _TRAILING_PUNCT:
        token = token[:-1]
    # strip surrounding angle brackets or quotes
    token = token.strip(' <>\"\'')
    return token

def validate_part(doi_part: str) -> bool:
    """
    Validate the DOI 'suffix' including prefix, e.g. "10.1016/j.cageo.2015.09.015".
    Returns True if it matches a Crossref-style DOI pattern.
    """
    if not doi_part or not isinstance(doi_part, str):
        return False
    doi_part = doi_part.strip()
    return bool(_DOI_PART_RE.match(doi_part))

def validate(doi_str: str, strict: bool = False) -> bool:
    """
    Validate a single DOI string. Tolerant of leading link or 'doi:' strings by default.
    If strict=True, only accept doi.org URLs or 'doi:' prefixes (per README's 'strict' option).
    """
    if not doi_str or not isinstance(doi_str, str):
        return False
    s = doi_str.strip()

    # check DOI URL
    m = _DOI_URL_RE.match(s)
    if m:
        return validate_part(_strip_trailing(m.group(1)))

    # check doi: prefix
    if s.lower().startswith('doi:'):
        return validate_part(_strip_trailing(s[4:].strip()))

    # if strict, don't accept bare DOI parts without prefix/url
    if strict:
        return False

    # otherwise, try to see if it looks like a bare DOI
    return validate_part(_strip_trailing(s))

def normalize(doi_str: str, strict: bool = False) -> Optional[str]:
    """
    Normalize a DOI input to the standard form "10.xxxx/yyy".
    Accepts bare DOIs, doi: prefixes, doi.org/dx.doi.org URLs, and some repository URLs (eLife, Zenodo).
    Returns the normalized DOI or None if it cannot be recognized.
    """
    if not doi_str or not isinstance(doi_str, str):
        return None
    s = doi_str.strip()

    # eLife URL -> map to 10.7554/eLife.<id>
    m = _ELIFE_URL_RE.match(s)
    if m:
        return f'10.7554/eLife.{m.group(1)}'

    # Zenodo record url -> map to 10.5281/zenodo.<id>
    m = _ZENODO_RECORD_URL_RE.match(s)
    if m:
        return f'10.5281/zenodo.{m.group(1)}'

    # doi.org URL
    m = _DOI_URL_RE.match(s)
    if m:
        candidate = _strip_trailing(m.group(1))
        return candidate if validate_part(candidate) else None

    # doi: prefix
    if s.lower().startswith('doi:'):
        candidate = _strip_trailing(s[4:].strip())
        return candidate if validate_part(candidate) else None

    # If strict, only accept the above forms
    if strict:
        return None

    # Try to extract a bare DOI-like part from the string
    candidate = _strip_trailing(s)
    if validate_part(candidate):
        return candidate

    # As a last attempt, run the extraction regex and take first match
    m = _DOI_EXTRACT_RE.search(s)
    if m:
        candidate = _strip_trailing(m.group(1))
        return candidate if validate_part(candidate) else None

    return None

def build_url(doi_str: str, strict: bool = False) -> Optional[str]:
    """
    Build a canonical https://doi.org/... URL from any DOI-like input.
    Returns the secure DOI URL or None if the input couldn't be normalized.
    """
    n = normalize(doi_str, strict=strict)
    if not n:
        return None
    return f'https://doi.org/{n}'

def extract(text: str, strict: bool = False) -> List[str]:
    """
    Extract DOIs from arbitrary text. Returns a list of normalized DOIs (unique, in order of discovery).
    If strict=True, only considers explicit doi: prefixed strings and doi.org URLs (no heuristic bare DOI parsing).
    """
    if not text:
        return []

    found: List[str] = []
    seen: Set[str] = set()

    # First handle special repo URLs (eLife, Zenodo)
    for m in _ELIFE_URL_RE.finditer(text):
        doi = normalize(m.group(0))
        if doi and doi not in seen:
            found.append(doi); seen.add(doi)
    for m in _ZENODO_RECORD_URL_RE.finditer(text):
        doi = normalize(m.group(0))
        if doi and doi not in seen:
            found.append(doi); seen.add(doi)

    # Find doi.org / dx.doi.org links
    for m in re.finditer(r'https?://(?:dx\.)?doi\.org/([^?\s#<>]+)', text, flags=re.IGNORECASE):
        candidate = _strip_trailing(m.group(1))
        if validate_part(candidate):
            if candidate not in seen:
                found.append(candidate); seen.add(candidate)

    # Find doi: prefixed tokens
    for m in re.finditer(r'doi:\s*(10\.\d{4,9}/[^\s"<>\)\]\;:,]+)', text, flags=re.IGNORECASE):
        candidate = _strip_trailing(m.group(1))
        if validate_part(candidate):
            if candidate not in seen:
                found.append(candidate); seen.add(candidate)

    if not strict:
        # A broader search for bare DOI-looking tokens (this will also find many false positives in arbitrary text,
        # but is helpful when DOIs are embedded without prefixes).
        for m in _DOI_EXTRACT_RE.finditer(text):
            candidate = _strip_trailing(m.group(1))
            if validate_part(candidate) and candidate not in seen:
                found.append(candidate); seen.add(candidate)

    return found

def is_open_funder_registry(doi_str: str) -> bool:
    """
    Returns True if the DOI indicates an Open Funder Registry ID.
    The Open Funder Registry uses prefix 10.13039.
    """
    n = normalize(doi_str, strict=False)
    if not n:
        return False
    return n.lower().startswith('10.13039/')

# Simple CLI: print extracted normalized DOIs
def _cli_main(argv):
    import argparse
    parser = argparse.ArgumentParser(description="Extract and normalize DOIs from text")
    parser.add_argument('text', nargs='*', help='Text or tokens to scan for DOIs. If omitted, reads stdin.')
    parser.add_argument('--strict', action='store_true', help='Only accept explicit DOI urls and doi: prefixes')
    parser.add_argument('--unique', action='store_true', help='Print unique DOIs only (default keeps discovery order)')
    args = parser.parse_args(argv[1:])

    if args.text:
        input_text = ' '.join(args.text)
    else:
        input_text = sys.stdin.read()

    dois = extract(input_text, strict=args.strict)
    if args.unique:
        seen = set()
        filtered = []
        for d in dois:
            if d not in seen:
                filtered.append(d); seen.add(d)
        dois = filtered

    for d in dois:
        print(d)

if __name__ == '__main__':
    _cli_main(sys.argv)