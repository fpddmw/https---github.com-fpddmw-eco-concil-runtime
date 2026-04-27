# Open-Meteo Air Quality OpenClaw Templates

## Recon

Use when a moderator or listener has identified a place and date window but physical validation has not started.

```text
Use $fetch-open-meteo-air-quality.
Run:
python3 scripts/fetch_open_meteo_air_quality.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var pm2_5 \
  --timezone GMT \
  --dry-run \
  --pretty
Return only the JSON result.
```

## Background Validation

Use when you need modeled PM or gas background conditions to complement OpenAQ station evidence.

```text
Use $fetch-open-meteo-air-quality.
Run:
python3 scripts/fetch_open_meteo_air_quality.py fetch \
  --location [LATITUDE,LONGITUDE] \
  --start-date [YYYY-MM-DD] \
  --end-date [YYYY-MM-DD] \
  --hourly-var pm2_5 \
  --hourly-var pm10 \
  --hourly-var nitrogen_dioxide \
  --hourly-var ozone \
  --hourly-var us_aqi \
  --domain auto \
  --cell-selection nearest \
  --timezone GMT \
  --pretty
Return only the JSON result.
```
