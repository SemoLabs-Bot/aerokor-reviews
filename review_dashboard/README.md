# Review Hub Dashboard (static)

A lightweight UI dashboard to browse reviews by product, platform, and brand.

## Data
- Source: Google Sheets `Review Hub` → tab `looker_reviews`
- Export command:

```bash
cd /Users/semobot/Desktop/workspace/My-Assistant
python3 scripts/review-hub/export_reviews_json.py
```

This writes:
- `data/reviews.json`

## Run locally

```bash
cd /Users/semobot/Desktop/workspace/My-Assistant
python3 scripts/review-hub/export_reviews_json.py
python3 -m http.server 8088
```

Open:
- http://localhost:8088/review_dashboard/

## Deploy

Pick one:
- Vercel / Netlify: deploy the repo as a static site (serve `/review_dashboard` + `/data`).
- GitHub Pages: publish `review_dashboard/` and `data/`.

If you need the dashboard to remain private, prefer a host with access control (Vercel password protection / Cloudflare Access / basic auth reverse proxy).
