# SEO Indexing Monitor (Local DB)

This is a separate local project that keeps indexing state in SQLite, exports JSON files, and shows data in a Streamlit dashboard.

## What this solves
- No Google Sheets quota dependency for live state
- One durable DB for discovery + GSC checks
- Optional JSON exports for integrations
- Dashboard reads from DB directly

## Project files
- `run_monitor.py`: worker runner
- `dashboard.py`: Streamlit dashboard
- `config.example.json`: template config
- `config.local.json`: your editable local config
- `monitor/db.py`: SQLite schema + DB helpers
- `monitor/sitemap.py`: sitemap parsing
- `monitor/gsc.py`: GSC URL Inspection client
- `monitor/worker.py`: discovery + poll orchestration
- `monitor/export.py`: JSON export

## Setup
```bash
cd /Users/kartikaykhosla/Documents/article-analyzer/QC/seo-indexing-monitor-local
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
cp config.example.json config.local.json
```

Then edit `config.local.json`:
- `service_account_json_path`
- `cutoff_datetime` (e.g. `today_ist` or fixed datetime)
- sitemap lists and caps

## Run worker
Run all properties:
```bash
python run_monitor.py --config config.local.json --mode all
```

Run one property:
```bash
python run_monitor.py --config config.local.json --property thedailyjagran.com --mode all
```

One-time reset + fixed cutoff:
```bash
python run_monitor.py \
  --config config.local.json \
  --cutoff-datetime "2026-04-08T18:10:00+05:30" \
  --reset-db \
  --mode all
```

Run discovery only:
```bash
python run_monitor.py --config config.local.json --mode discover
```

Run GSC polling only:
```bash
python run_monitor.py --config config.local.json --mode poll
```

Export JSON only:
```bash
python run_monitor.py --config config.local.json --mode export
```

## Run dashboard
```bash
streamlit run dashboard.py
```

## Suggested local cron strategy
Use one cron per property and stagger runs to avoid API bursts.

Example:
```cron
*/20 * * * * cd /Users/kartikaykhosla/Documents/article-analyzer/QC/seo-indexing-monitor-local && . .venv/bin/activate && python run_monitor.py --config config.local.json --property thedailyjagran.com --mode all
4,24,44 * * * * cd /Users/kartikaykhosla/Documents/article-analyzer/QC/seo-indexing-monitor-local && . .venv/bin/activate && python run_monitor.py --config config.local.json --property herzindagi.com_en --mode all
12,42 * * * * cd /Users/kartikaykhosla/Documents/article-analyzer/QC/seo-indexing-monitor-local && . .venv/bin/activate && python run_monitor.py --config config.local.json --property jagranjosh.com --mode all
16,46 * * * * cd /Users/kartikaykhosla/Documents/article-analyzer/QC/seo-indexing-monitor-local && . .venv/bin/activate && python run_monitor.py --config config.local.json --property jagran.com --mode all
```

## Notes
- `indexing_latency_minutes` is calculated as `google_last_crawl_at - sitemap_published_date`.
- Once a URL is `Indexed`, it is not polled again.
- If GSC returns quota errors, backoff is applied per property.
