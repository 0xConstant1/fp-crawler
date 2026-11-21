# FP-Scraper

Scrapes FP TOP 10 catalogs, resolves titles to TMDB IDs, and writes one JSON file per region.

## Usage

Install the project:

```bash
python -m pip install .
```

Scrape the global page:

```bash
python -m flixpatrol_scraper --region global --output global.json
```

Scrape one region:

```bash
python -m flixpatrol_scraper --region united-states --output united-states.json
```

Scrape multiple regions:

```bash
python -m flixpatrol_scraper --region global,united-states,brazil --output exports
```

Scrape all supported regions:

```bash
python -m flixpatrol_scraper --all-regions --output exports
```

Scrape all supported regions except regions known to have no or low catalog coverage:

```bash
python -m flixpatrol_scraper --all-regions-slim --output exports
```

Resolve TMDB IDs while scraping:

```bash
python -m flixpatrol_scraper --all-regions --output exports --resolve-tmdb --tmdb-api-key YOUR_TMDB_API_KEY
```

Resolve ambiguous TMDB matches using FlixPatrol title details. At a request rate of `0.5`, chart requests are spaced two seconds apart. Up to five title detail requests can start in a rolling two-second window, with 0–500 ms of jitter between starts.

```bash
python -m flixpatrol_scraper --region global --output global.json --resolve-tmdb --tmdb-api-key YOUR_TMDB_API_KEY --fetch-title-details --requests-per-second 0.5
```

## GitHub Actions

The repo includes a scheduled workflow in [.github/workflows/update-catalogs.yml](.github/workflows/update-catalogs.yml).

It:

- runs once per day at `15:30 UTC`
- can also be triggered manually with `workflow_dispatch`
- scrapes `--all-regions-slim`
- resolves TMDB IDs
- writes outputs into `catalogs/`
- commits and pushes updated catalog files back to the repository

Required GitHub secrets:

- `TMDB_API_KEY`, or
- `TMDB_ACCESS_TOKEN`
