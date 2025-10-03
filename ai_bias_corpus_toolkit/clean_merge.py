#!/usr/bin/env python3
import argparse, os, re, sys, time
import pandas as pd
from urllib.parse import urlparse
from rapidfuzz import fuzz
from tqdm import tqdm
import requests
import trafilatura

SCHEMA = ["id","date_pub","type_source","titre","lien","langue","controverse","secteur","territoire",
 "acteurs","role_acteurs","rapports_pouvoir","issue","mots_cles","extrait_citation","note_analytique",
 "source_name","source_type","source_country"]

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
    # enforce columns
    for c in SCHEMA:
        if c not in df.columns:
            df[c] = None
    # simple normalizations
    df["titre"] = df["titre"].fillna("").str.strip()
    df["lien"] = df["lien"].fillna("").str.strip()
    df["date_pub"] = pd.to_datetime(df["date_pub"], errors="coerce")
    # domain helper
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
            # if same domain and titles similar, drop j
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

def parse_args():
    ap = argparse.ArgumentParser()
    ap.add_argument("--config", required=True)
    ap.add_argument("--raw_dir", required=True)
    ap.add_argument("--out", required=True)
    ap.add_argument("--extract-text", action="store_true", help="extract article text into 'fulltext' column")
    return ap.parse_args()

def main():
    args = parse_args()
    df = load_raw(args.raw_dir)
    if df.empty:
        print("No raw CSVs found.", file=sys.stderr)
        sys.exit(1)
    df = normalize(df)
    df = df.dropna(subset=["titre"]).query("titre != ''")
    df = dedupe(df, thresh=90)
    df = df.reset_index(drop=True)
    df.insert(0, "id", range(1, len(df)+1))
    df = maybe_extract(df, do_extract=args.extract_text)
    # keep only schema + optional fulltext if present
    cols = SCHEMA + ([ "fulltext"] if "fulltext" in df.columns else [])
    df = df[cols]
    os.makedirs(os.path.dirname(args.out), exist_ok=True)
    df.to_csv(args.out, index=False)
    print(f"Wrote {args.out} with {len(df)} rows.")

if __name__ == "__main__":
    main()
