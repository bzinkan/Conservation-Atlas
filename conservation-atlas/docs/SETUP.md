# Setup Guide

## Prerequisites

- **Node.js** 20+
- **PostgreSQL** 15+ with PostGIS
- **GDAL** for WDPA import

## Local Development

### 1. Database Setup

```bash
createdb conservation_atlas
psql conservation_atlas -c "CREATE EXTENSION IF NOT EXISTS postgis;"
psql conservation_atlas -f packages/shared/db/schema.sql
psql conservation_atlas -f packages/shared/db/protected_areas.sql
```

### 2. Environment Configuration

```bash
cp .env.example .env
```

Required variables:
- `DATABASE_URL`
- `OPENAI_API_KEY`
- `AWS_REGION`

### 3. Import WDPA (Protected Areas)

```bash
# Download from Protected Planet (~2GB)
wget https://d1gam3xoknrgr2.cloudfront.net/current/WDPA_Jan2026_Public.gpkg

# Load and map
LAYER=$(npx ts-node tools/pick_wdpa_layer.ts --file WDPA_Jan2026_Public.gpkg)
npx ts-node tools/generate_wdpa_mapping.ts \
  --file WDPA_Jan2026_Public.gpkg \
  --layer "$LAYER" \
  --release 2026-01 > wdpa_map.sql

ogr2ogr -f "PostgreSQL" PG:"$DATABASE_URL" WDPA_Jan2026_Public.gpkg \
  -nln protected_areas_staging -nlt PROMOTE_TO_MULTI \
  -lco GEOMETRY_NAME=geom -t_srs EPSG:4326

psql "$DATABASE_URL" -f wdpa_map.sql
```

### 4. Run Development

```bash
npm run dev:api    # API server
npm run dev:worker # Queue consumer
```

### 5. Test Place Brief

```bash
curl "http://localhost:3000/api/place-brief?lat=39.51&lng=-84.73"
```

## Deployment

See full deployment instructions for AWS ECS Fargate setup.
