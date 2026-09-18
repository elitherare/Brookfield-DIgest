"""
Datastore Sanitizer and Normalizer for Brookfield Private Equity Intelligence.
Cleans headline formatting artifacts, fixes missing dates, removes invalid deal fragments,
and deduplicates historical test entries.
"""

import os
import re
import shutil
import pandas as pd
from config import DEFAULT_LOG_CSV

MONTH_MAP = {
    "jan": "Jan", "feb": "Feb", "mar": "Mar", "apr": "Apr",
    "may": "May", "jun": "Jun", "jul": "Jul", "aug": "Aug",
    "sep": "Sep", "oct": "Oct", "nov": "Nov", "dec": "Dec"
}


def sanitize_row(row: pd.Series) -> pd.Series:
    headline = str(row.get("headline", "")).strip()
    pub_date = str(row.get("published_date", "")).strip()
    if pub_date == "nan" or pub_date == "None":
        pub_date = ""

    # 1. Clean "Press ReleaseMon YYYYHeadline" artifacts
    m = re.match(r"^Press\s*Release\s*([A-Za-z]{3})\s*(\d{4})\s*(.*)$", headline, re.IGNORECASE)
    if m:
        month_str = m.group(1).capitalize()
        year_str = m.group(2)
        clean_headline = m.group(3).strip()

        if not pub_date:
            pub_date = f"{month_str} {year_str}"
        headline = clean_headline

    # 2. If date still missing, try to infer from URL or headline
    if not pub_date:
        url = str(row.get("source_url", "")).lower()
        if "second-quarter-2026" in url or "q2-2026" in url:
            pub_date = "Jul 2026"
        elif "first-quarter-2026" in url or "q1-2026" in url:
            pub_date = "May 2026"
        elif "fourth-quarter-2025" in url or "q4-2025" in url:
            pub_date = "Jan 2026"
        elif "third-quarter-2025" in url or "q3-2025" in url:
            pub_date = "Nov 2025"
        elif "second-quarter-2025" in url or "q2-2025" in url:
            pub_date = "Aug 2025"
        elif "first-quarter-2025" in url or "q1-2025" in url:
            pub_date = "May 2025"
        else:
            pub_date = "2026"

    # 3. Clean trailing / invalid fragments
    if "enhances our" in headline.lower():
        headline = headline.replace("enhances our", "").strip()

    summary = str(row.get("one_line_summary", "")).strip()
    m_sum = re.match(r"^Press\s*Release\s*([A-Za-z]{3})\s*(\d{4})\s*(.*)$", summary, re.IGNORECASE)
    if m_sum:
        summary = m_sum.group(3).strip()
    row["one_line_summary"] = summary

    # 4. Correct Region-by-Asset for known assets
    primary_region = str(row.get("primary_region", "")).strip()
    if "reliance" in headline.lower():
        primary_region = "Asia Pacific"
    elif "oaktree" in headline.lower():
        primary_region = "North America"
    elif "multiplex" in headline.lower():
        primary_region = "Asia Pacific"
    elif "world freight" in headline.lower() or "fosber" in headline.lower():
        primary_region = "Europe"
    elif "network" in headline.lower():
        primary_region = "Middle East / GCC"

    row["headline"] = headline
    row["published_date"] = pub_date
    row["primary_region"] = primary_region
    return row


def clean_datastore(csv_path: str = DEFAULT_LOG_CSV) -> int:
    if not os.path.exists(csv_path):
        print(f"Datastore not found at {csv_path}")
        return 0

    backup_path = csv_path.replace(".csv", "_before_cleanup.csv")
    shutil.copyfile(csv_path, backup_path)
    print(f"Backed up datastore to {backup_path}")

    df = pd.read_csv(csv_path)
    initial_count = len(df)
    print(f"Loaded {initial_count} initial records.")

    # Apply row sanitization
    df = df.apply(sanitize_row, axis=1)

    # Filter out empty or meaningless fragment headlines
    df = df[~df["headline"].str.lower().str.startswith("brookfield acquires aypa enhances our")]
    df = df[df["headline"].str.len() > 10]

    # Normalize key for deduplication: lowercase headline + source_url
    df["_norm_key"] = (
        df["headline"].str.lower().str.replace(r"\s+", " ", regex=True).str.strip()
        + "||"
        + df["source_url"].astype(str).str.strip()
    )

    # Keep first occurrence of each unique headline + source_url
    df = df.drop_duplicates(subset=["_norm_key"], keep="first")
    df = df.drop(columns=["_norm_key"])

    final_count = len(df)
    df.to_csv(csv_path, index=False)
    print(f"Cleanup complete! Removed {initial_count - final_count} duplicate/fragment rows.")
    print(f"Clean records remaining: {final_count}")
    return final_count


if __name__ == "__main__":
    clean_datastore()
