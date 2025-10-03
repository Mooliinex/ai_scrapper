#!/usr/bin/env python3
import argparse, os, time, sys, math, json, csv, re
import pandas as pd
import feedparser, requests, yaml
from urllib.parse import urlencode, urlparse, parse_qs, urljoin
from dateutil import parser as dtp
from dateparser import parse as dateparse
from tqdm import tqdm

def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def clamp_date(dt, since, until):
    if dt is None: return None
    if dt < since or dt > until: return None
    return dt

def iso_or_none(dt):
    return dt.isoformat() if dt else None

def clean_text(s):
    # Normalize input to string and strip HTML/whitespace. Handles bytes, dicts, and objects.
    if s is None:
        return None
    # Decode bytes
    if isinstance(s, bytes):
        try:
            s = s.decode('utf-8', errors='ignore')
        except Exception:
            s = s.decode(errors='ignore')
    # Extract common textual fields from mappings/objects (e.g., feedparser entries)
    if not isinstance(s, str):
        extracted = None
        for key in ("title", "value", "label", "name", "text"):
            try:
                extracted = s.get(key)
            except AttributeError:
                extracted = getattr(s, key, None)
            if extracted:
                break
        if isinstance(extracted, bytes):
            extracted = extracted.decode('utf-8', errors='ignore')
        s = extracted if isinstance(extracted, str) else str(s)
    return re.sub(r'\s+', ' ', re.sub(r'<[^>]+>', ' ', s)).strip()

def harvest_rss(urls, since, until, rate, outdir, label="rss"):
    rows = []
    for url in tqdm(urls, desc=f"RSS ({label})"):
        feed = feedparser.parse(url)
        for e in feed.entries:
            # pick best date
            dt = None
            for k in ["published_parsed","updated_parsed"]:
                if getattr(e, k, None):
                    try:
                        dt = dtp.parse(time.strftime('%Y-%m-%dT%H:%M:%S', getattr(e, k)))
                        break
                    except Exception:
                        pass
            if not dt and getattr(e, "published", None):
                dt = dateparse(e.published)
            if not dt and getattr(e, "updated", None):
                dt = dateparse(e.updated)
            if dt:
                dt = clamp_date(dt, since, until)
                if not dt: 
                    continue
            link = getattr(e, "link", None)
            title = clean_text(getattr(e, "title", None))
            summary = clean_text(getattr(e, "summary", None))
            rows.append({
                "date_pub": iso_or_none(dt),
                "type_source": "Presse",
                "titre": title,
                "lien": link,
                "langue": None,
                "mots_cles": None,
                "extrait_citation": summary,
                "source_name": clean_text(getattr(e, "source", None)) or clean_text(getattr(e, "author", None)),
                "source_type": "rss",
                "source_country": None
            })
        time.sleep(rate)
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(outdir, f"rss_{int(time.time())}.csv"), index=False)
    return len(rows)

def harvest_openalex(conf, since, until, rate, outdir):
    base = "https://api.openalex.org/works"
    params = {
        "search": conf.get("query",""),
        "from_publication_date": since.date().isoformat(),
        "to_publication_date": until.date().isoformat(),
        "per_page": conf.get("per_page", 200),
        "mailto": conf.get("mailto","")
    }
    page = 1
    rows = []
    with tqdm(desc="OpenAlex", unit="page") as pbar:
        while True:
            params["page"] = page
            try:
                r = requests.get(base, params=params, timeout=30)
                r.raise_for_status()
                js = r.json()
            except Exception as e:
                print("OpenAlex error:", e, file=sys.stderr)
                break
            results = js.get("results", [])
            for w in results:
                title = w.get("title")
                link = w.get("doi") or (w.get("primary_location") or {}).get("source",{}).get("homepage_url")
                date_pub = w.get("publication_date") or (w.get("from_indexed_date") or "").split("T")[0]
                lang = w.get("language")
                rows.append({
                    "date_pub": date_pub,
                    "type_source": "Académique",
                    "titre": title,
                    "lien": link or (w.get("id")),
                    "langue": lang,
                    "mots_cles": ",".join([c.get("display_name") for c in w.get("concepts",[])[:10]]),
                    "extrait_citation": None,
                    "source_name": (w.get("primary_location") or {}).get("source",{}).get("display_name"),
                    "source_type": "openalex",
                    "source_country": None
                })
            pbar.update(1)
            if "next_cursor" in js or js.get("meta",{}).get("count",0) > page*params["per_page"]:
                # OpenAlex paginates by page until empty
                if not results: break
                page += 1
                time.sleep(rate)
            else:
                break
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(outdir, f"openalex_{int(time.time())}.csv"), index=False)
    return len(rows)

def harvest_gdelt(conf, since, until, rate, outdir):
    # Use GDELT Article Search API 2.0 (doc api.gdeltproject.org)
    # Query across date range by splitting into months to avoid large responses.
    q = conf.get("gkg_search","")
    max_records = conf.get("max_records", 5000)
    rows = []
    # monthly windows
    start = since.date().replace(day=1)
    end = until.date()
    import datetime as dt
    def month_iter(d1, d2):
        d = d1
        while d <= d2:
            yield d
            if d.month == 12:
                d = d.replace(year=d.year+1, month=1, day=1)
            else:
                d = d.replace(month=d.month+1, day=1)
    for mstart in tqdm(list(month_iter(start, end)), desc="GDELT months"):
        mend = (mstart.replace(day=28) + pd.Timedelta(days=4)).replace(day=1) - pd.Timedelta(days=1)
        if mend > end: mend = end
        params = {
            "query": q,
            "mode": "ArtList",
            "format": "json",
            "maxrecords": max_records,
            "startdatetime": mstart.strftime("%Y%m%d%H%M%S"),
            "enddatetime": mend.strftime("%Y%m%d%H%M%S")
        }
        try:
            r = requests.get("https://api.gdeltproject.org/api/v2/doc/doc", params=params, timeout=30)
            r.raise_for_status()
            js = r.json()
        except Exception as e:
            print("GDELT error:", e, file=sys.stderr)
            continue
        for a in js.get("articles", []):
            rows.append({
                "date_pub": a.get("seendate","")[:10],
                "type_source": "Presse",
                "titre": a.get("title"),
                "lien": a.get("url"),
                "langue": a.get("language"),
                "mots_cles": None,
                "extrait_citation": None,
                "source_name": a.get("sourcecountry"),
                "source_type": "gdelt",
                "source_country": a.get("sourcecountry")
            })
        time.sleep(rate)
    if rows:
        df = pd.DataFrame(rows)
        df.to_csv(os.path.join(outdir, f"gdelt_{int(time.time())}.csv"), index=False)
    return len(rows)

def parse_args():
    # Conserved for compatibility but not used anymore; values are set directly in main()
    ap = argparse.ArgumentParser()
    ap.add_argument("--since", required=False, help="YYYY-MM-DD")
    ap.add_argument("--until", required=False, help="YYYY-MM-DD")
    ap.add_argument("--config", required=False, help="config.yaml path")
    ap.add_argument("--out", default="data/raw", help="output directory for raw csvs")
    return ap.parse_args()

def main():
    # Paramètres définis en dur (exécuter sans arguments CLI)
    since_str = "2015-05-01"
    until_str = "2025-10-01"
    config_path = "config.yaml"
    out_dir = "data/raw"

    with open(config_path, "r") as f:
        conf = yaml.safe_load(f)
    ensure_dir(out_dir)
    since = dtp.parse(since_str)
    until = dtp.parse(until_str)
    rate = conf.get("rate_limit",{}).get("sleep_seconds", 1.0)
    # RSS (generic + NGO)
    total = 0
    total += harvest_rss(conf["sources"].get("rss",[]), since, until, rate, out_dir, label="news")
    total += harvest_rss(conf["sources"].get("ngo_rss",[]), since, until, rate, out_dir, label="ngo")
    # OpenAlex
    total += harvest_openalex(conf["sources"].get("openalex",{}), since, until, rate, out_dir)
    # GDELT
    total += harvest_gdelt(conf["sources"].get("gdelt",{}), since, until, rate, out_dir)
    print(f"Harvested rows: {total}")

if __name__ == "__main__":
    main()
