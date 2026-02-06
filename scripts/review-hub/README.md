# review-hub scripts

## Append reviews to Google Sheets (gog)

Config: `config/review-hub/google-sheets.sink.json`

Input: JSON array of review objects.

```bash
node scripts/review-hub/append-reviews-to-sheets.mjs --input /path/to/reviews.json
```

Dry-run:

```bash
node scripts/review-hub/append-reviews-to-sheets.mjs --input /path/to/reviews.json --dry-run
```

OAuth prerequisite:
- `gog auth credentials /path/to/client_secret.json`
- `gog auth add <you@gmail.com> --services sheets,drive`
- `gog auth list`
