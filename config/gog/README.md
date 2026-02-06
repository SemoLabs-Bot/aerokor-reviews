# Google OAuth (local only)

Put your Google OAuth client secret JSON here if you want (this folder is gitignored).

Typical flow:

```bash
gog auth credentials config/gog/client_secret.json
gog auth add <you@gmail.com> --services sheets,drive --no-input
# optionally set default account or export GOG_ACCOUNT
```
