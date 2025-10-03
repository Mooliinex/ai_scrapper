# AI Bias Controversies Corpus — Harvester (2015–2025)

This toolkit builds a **systematic corpus** (FR/EN) on controversies around **AI bias / discrimination / inequalities** from **May 1, 2015** to **today**.

## What it does
- **Harvests** from:
  - **Google News RSS** (news & media)
  - **GDELT 2.1** (global news metadata)
  - **OpenAlex** (academic works)
  - **Civic/NGO reports** (via RSS/Atom where available; configurable)
- **Normalizes** into a unified schema (`data/clean/corpus.csv`)
- **Deduplicates** (fuzzy on title+domain+date)
- **(Optional)** extracts main text (via `trafilatura`) for CDA/TAL pipelines
- **Logs** all runs for reproducibility

> You can extend sources by editing `config.yaml` (keywords, languages, sources).

## Quick start

```bash
python3 -m venv .venv && source .venv/bin/activate
pip install -r requirements.txt

# 1) Harvest (raw CSVs per source)
python harvest.py --since 2015-05-01 --until 2025-10-01 --config config.yaml --out data/raw

# 2) Clean/merge/deduplicate into one corpus
python clean_merge.py --config config.yaml --raw_dir data/raw --out data/clean/corpus.csv

# (Optional) Extract article text for discourse analysis
python clean_merge.py --config config.yaml --raw_dir data/raw --out data/clean/corpus.csv --extract-text
```

## Output schema (columns)
See `schema.csv` (same as your pilot table):  
`id,date_pub,type_source,titre,lien,langue,controverse,secteur,territoire,acteurs,role_acteurs,rapports_pouvoir,issue,mots_cles,extrait_citation,note_analytique,source_name,source_type,source_country`

- The *annotation* fields (controverse/secteur/acteurs/…) are left blank for you to fill later (or with a separate codebook).  
- `source_*` columns are automatically populated when available.

## Notes
- Respect websites' robots.txt and ToS. This tool focuses on **metadata** & short excerpts.
- For heavy/full-text use, prefer open sources or your personal access rights.
- GDELT & OpenAlex have usage limits; the code paginates and respects rate limits.

---
**Authorship:** generated for Julie Marques (STS feminist sociologist) — reproducible research.
