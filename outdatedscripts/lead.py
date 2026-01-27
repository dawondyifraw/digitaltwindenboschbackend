import re
import time
import random
import logging
from typing import List, Tuple, Optional

import requests
from bs4 import BeautifulSoup
import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

# --------------------------------------------------
# Configuration
# --------------------------------------------------

INPUT_CSV = "cbs_codes_en_gemeente_namen.csv"
OUTPUT_CSV = "municipal_prospects_scored.csv"

USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7)",
    "Mozilla/5.0 (X11; Linux x86_64)"
]

# Keywords that indicate relevance for UDT / smart city
KEYWORDS_SMART_CITY = [
    "smart city",
    "digitale stad",
    "digital twin",
    "digital twins",
    "urban digital twin",
    "slimme stad",
    "open data",
    "open data portal",
    "datagedreven",
    "data gedreven",
    "geo-informatie",
    "geodata",
    "sensor",
    "mobiliteit",
    "verkeer",
    "luchtkwaliteit",
    "co2",
    "duurzaamheid",
    "klimaatadaptatie",
    "omgevingsvisie"
]

EMAIL_REGEX = r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}"


# --------------------------------------------------
# Utility functions
# --------------------------------------------------

def clean_url(url: str) -> Optional[str]:
    if not isinstance(url, str) or not url.strip():
        return None
    url = url.strip()
    if not url.startswith("http"):
        url = "https://" + url
    return url


def fetch_html(url: str, timeout: int = 10) -> Optional[str]:
    headers = {
        "User-Agent": random.choice(USER_AGENTS)
    }
    try:
        resp = requests.get(url, headers=headers, timeout=timeout)
        if resp.status_code == 200:
            return resp.text
        logging.warning(f"Non-200 status for {url}: {resp.status_code}")
        return None
    except Exception as e:
        logging.warning(f"Error fetching {url}: {e}")
        return None


def extract_emails_and_links(html: str, base_url: str) -> Tuple[List[str], List[str]]:
    if not html:
        return [], []
    emails = sorted(set(re.findall(EMAIL_REGEX, html)))
    soup = BeautifulSoup(html, "html.parser")

    linkedin_links = []
    for a in soup.find_all("a", href=True):
        href = a["href"]
        if "linkedin.com" in href:
            linkedin_links.append(href)

    linkedin_links = sorted(set(linkedin_links))
    return emails, linkedin_links


def detect_keywords(html: str, keywords: List[str]) -> List[str]:
    if not html:
        return []
    found = []
    lower = html.lower()
    for kw in keywords:
        if kw.lower() in lower:
            found.append(kw)
    return sorted(set(found))


def score_municipality(
    has_website: bool,
    keywords_found: List[str],
    num_emails: int,
    has_linkedin: bool
) -> int:
    """
    Very simple rule based scoring.
    You can tune this later.
    """
    score = 0

    if has_website:
        score += 1

    # Strong signals
    strong_terms = [
        "smart city", "digital twin", "urban digital twin",
        "slimme stad", "open data portal"
    ]
    if any(t in [k.lower() for k in keywords_found] for t in strong_terms):
        score += 4

    # Medium signals
    medium_terms = [
        "open data", "datagedreven", "geo-informatie", "geodata",
        "sensor", "mobiliteit", "luchtkwaliteit", "duurzaamheid"
    ]
    if any(t in [k.lower() for k in keywords_found] for t in medium_terms):
        score += 2

    # Emails and LinkedIn as weak but useful signals
    if num_emails >= 1:
        score += 1
    if num_emails >= 3:
        score += 1
    if has_linkedin:
        score += 1

    return score


# --------------------------------------------------
# Main pipeline
# --------------------------------------------------

def main():
    df = pd.read_csv(INPUT_CSV)

    # Optional: if you added Website column; if not, create it empty
    if "Website" not in df.columns:
        df["Website"] = ""

    results = []

    for idx, row in df.iterrows():
        name = row.get("Official Name", "")
        website = clean_url(row.get("Website", ""))

        logging.info(f"Processing: {name} ({website if website else 'no website'})")

        html = None
        emails = []
        linkedin_links = []
        keywords_found = []

        if website:
            html = fetch_html(website)
            if html:
                emails, linkedin_links = extract_emails_and_links(html, website)
                keywords_found = detect_keywords(html, KEYWORDS_SMART_CITY)

            # Sleep a bit to avoid hammering sites
            time.sleep(random.uniform(1.0, 2.5))

        score = score_municipality(
            has_website=website is not None,
            keywords_found=keywords_found,
            num_emails=len(emails),
            has_linkedin=len(linkedin_links) > 0
        )

        results.append(
            {
                "CBS Code": row.get("CBS Code", ""),
                "Official Name": name,
                "Website": website if website else "",
                "Emails": "; ".join(emails),
                "LinkedIn URLs": "; ".join(linkedin_links),
                "Keywords Found": "; ".join(keywords_found),
                "Prospect Score": score,
            }
        )

    out_df = pd.DataFrame(results)
    out_df.sort_values(by="Prospect Score", ascending=False, inplace=True)
    out_df.to_csv(OUTPUT_CSV, index=False)
    logging.info(f"Saved results to {OUTPUT_CSV}")


if __name__ == "__main__":
    main()
