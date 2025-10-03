#!/usr/bin/env python3
import os, sys, time, re, requests
import pandas as pd
import feedparser
import yaml
from urllib.parse import urlparse
from dateutil import parser as dtp
from dateparser import parse as dateparse
from tqdm import tqdm
from rapidfuzz import fuzz
import trafilatura


# Paramètres à modifier ici
SINCE = "2015-05-01"
UNTIL = "2025-10-01"
CONFIG_PATH = "config.yaml"
RAW_OUT_DIR = "data/raw"
CLEAN_OUT_PATH = "data/clean/corpus.csv"
EXTRACT_TEXT = True  # False pour désactiver l'extraction de texte


# Utilitaires communs
def ensure_dir(d):
    os.makedirs(d, exist_ok=True)

def clamp_date(dt, since, until):
    if dt is None: return None
    if dt < since or dt > until: return None
    return dt

def iso_or_none(dt):
    return dt.isoformat() if dt else None

def clean_text(s):
    # Normalise en chaîne et retire HTML/espaces; gère None/bytes/dicts/objets
    if s is None:
        return None
    if isinstance(s, bytes):
        try:
            s = s.decode('utf-8', errors='ignore')
        except Exception:
            s = s.decode(errors='ignore')
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


# Récolte (harvest)
def harvest_rss(urls, since, until, rate, outdir, label="rss"):
    rows = []
    for url in tqdm(urls, desc=f"RSS ({label})"):
        feed = feedparser.parse(url)
        for e in feed.entries:
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
    q = conf.get("gkg_search","")
    max_records = conf.get("max_records", 5000)
    rows = []
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


# Nettoyage / Fusion
SCHEMA = [
    "id","date_pub","type_source","titre","lien","langue","controverse","secteur","territoire",
    "acteurs","role_acteurs","rapports_pouvoir","issue","mots_cles","extrait_citation","note_analytique",
    "source_name","source_type","source_country"
]

def load_raw(raw_dir):
    frames = []
    for f in os.listdir(raw_dir):
        if f.endswith(".csv"):
            df = pd.read_csv(os.path.join(raw_dir, f))
            frames.append(df)
    if not frames:
        return pd.DataFrame(columns=SCHEMA)
    return pd.concat(frames, ignore_index=True)

def normalize(df):
    for c in SCHEMA:
        if c not in df.columns:
            df[c] = None
    df["titre"] = df["titre"].fillna("").str.strip()
    df["lien"] = df["lien"].fillna("").str.strip()
    df["date_pub"] = pd.to_datetime(df["date_pub"], errors="coerce")
    df["domain"] = df["lien"].apply(lambda u: urlparse(u).netloc if isinstance(u, str) and u else None)
    return df

def dedupe(df, thresh=90):
    df = df.sort_values("date_pub", ascending=False).reset_index(drop=True)
    keep = []
    seen = [False]*len(df)
    for i in tqdm(range(len(df)), desc="Dedup"):
        if seen[i]: continue
        keep.append(i)
        ti = df.at[i, "titre"]
        di = df.at[i, "domain"]
        for j in range(i+1, len(df)):
            if seen[j]: continue
            dj = df.at[j, "domain"]
            score = fuzz.token_set_ratio(ti, df.at[j, "titre"])
            if (di and dj and di==dj and score>=thresh) or (score>=98):
                seen[j] = True
    return df.iloc[keep].copy()

def extract_text(url, timeout=25):
    try:
        r = requests.get(url, timeout=timeout, headers={"User-Agent":"Mozilla/5.0"})
        if r.status_code >= 400:
            return None
        downloaded = trafilatura.extract(r.text, url=url, include_comments=False, include_tables=False)
        return downloaded
    except Exception:
        return None

def maybe_extract(df, do_extract=False):
    if not do_extract:
        return df
    texts = []
    for u in tqdm(df["lien"].fillna("").tolist(), desc="Extract text"):
        if not u:
            texts.append(None)
            continue
        texts.append(extract_text(u))
    if "fulltext" not in df.columns:
        df["fulltext"] = texts
    else:
        df["fulltext"] = texts
    return df


# Orchestration
def run_harvest(since_str: str, until_str: str, config_path: str, out_dir: str) -> int:
    with open(config_path, "r") as f:
        conf = yaml.safe_load(f)
    ensure_dir(out_dir)
    since = dtp.parse(since_str)
    until = dtp.parse(until_str)
    rate = conf.get("rate_limit",{}).get("sleep_seconds", 1.0)

    total = 0
    total += harvest_rss(conf["sources"].get("rss",[]), since, until, rate, out_dir, label="news")
    total += harvest_rss(conf["sources"].get("ngo_rss",[]), since, until, rate, out_dir, label="ngo")
    total += harvest_openalex(conf["sources"].get("openalex",{}), since, until, rate, out_dir)
    total += harvest_gdelt(conf["sources"].get("gdelt",{}), since, until, rate, out_dir)
    print(f"Harvested rows: {total}")
    return total

def run_clean(raw_dir: str, out_path: str, extract_text: bool) -> int:
    df = load_raw(raw_dir)
    if df.empty:
        print("No raw CSVs found.")
        return 0
    df = normalize(df)
    df = df.dropna(subset=["titre"]).query("titre != ''")
    df = dedupe(df, thresh=90)
    df = df.reset_index(drop=True)
    df.insert(0, "id", range(1, len(df)+1))
    df = maybe_extract(df, do_extract=extract_text)
    cols = SCHEMA + (["fulltext"] if "fulltext" in df.columns else [])
    df = df[cols]
    os.makedirs(os.path.dirname(out_path), exist_ok=True)
    df.to_csv(out_path, index=False)
    print(f"Wrote {out_path} with {len(df)} rows.")
    return len(df)

def main():
    run_harvest(SINCE, UNTIL, CONFIG_PATH, RAW_OUT_DIR)
    time.sleep(0.2)
    run_clean(RAW_OUT_DIR, CLEAN_OUT_PATH, EXTRACT_TEXT)

if __name__ == "__main__":
    main()


