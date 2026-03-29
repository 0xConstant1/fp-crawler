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

Resolve TMDB IDs while scraping:

```bash
python -m flixpatrol_scraper --all-regions --output exports --resolve-tmdb --tmdb-api-key YOUR_TMDB_API_KEY
```

## GitHub Actions

The repo includes a scheduled workflow in [.github/workflows/update-catalogs.yml](.github/workflows/update-catalogs.yml).

It:

- runs once per day at `08:00 UTC`
- can also be triggered manually with `workflow_dispatch`
- scrapes `--all-regions`
- resolves TMDB IDs
- writes outputs into `catalogs/`
- commits and pushes updated catalog files back to the repository

Required GitHub secrets:

- `TMDB_API_KEY`, or
- `TMDB_ACCESS_TOKEN`
