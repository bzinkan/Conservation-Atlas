-- ============================================
-- PROTECTED AREAS SCHEMA (WDPA-Ready)
-- ============================================
-- Schema optimized for Place Brief queries:
-- - ST_Contains for "inside protected area"
-- - ST_DWithin for "nearby protected areas"
-- - Monthly WDPA refresh via staging table

-- Requires PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;

-- ============================================
-- MAIN TABLE
-- ============================================
-- Stores both WDPA polygons and points in one table

CREATE TABLE IF NOT EXISTS protected_areas (
  id               BIGSERIAL PRIMARY KEY,

  -- WDPA identifiers
  wdpa_id          BIGINT NOT NULL,          -- WDPAID
  wdpa_pid         TEXT,                     -- WDPA_PID (string identifier)
  name             TEXT NOT NULL,            -- NAME

  -- Designation / classification
  designation      TEXT,                     -- DESIG
  designation_type TEXT,                     -- DESIG_TYPE (National, Regional, etc.)
  iucn_category    TEXT,                     -- IUCN_CAT (Ia, Ib, II, III, IV, V, VI, Not Reported)
  status           TEXT,                     -- STATUS (Designated, Proposed, etc.)
  status_year      INTEGER,                  -- STATUS_YR
  gov_type         TEXT,                     -- GOV_TYPE (Federal, State, etc.)
  own_type         TEXT,                     -- OWN_TYPE (State, Private, etc.)
  mang_auth        TEXT,                     -- MANG_AUTH (Managing authority)
  mang_plan        TEXT,                     -- MANG_PLAN (Management plan URL)
  verif            TEXT,                     -- VERIF (Verification state)

  -- Geography
  iso3             TEXT,                     -- ISO3 country code
  parent_iso3      TEXT,                     -- PARENT_ISO3 (if applicable)
  marine           TEXT,                     -- MARINE (0=terrestrial, 1=coastal, 2=marine)
  reported_area_km2 DOUBLE PRECISION,        -- REP_AREA (reported area in km²)
  gis_area_km2     DOUBLE PRECISION,         -- GIS_AREA (GIS-calculated area km²)
  gis_m_area_km2   DOUBLE PRECISION,         -- GIS_M_AREA (marine area km²)

  -- Data provenance
  wdpa_release     TEXT NOT NULL,            -- e.g. "2026-01"
  source           TEXT NOT NULL DEFAULT 'wdpa',

  -- Geometry (stored in WGS84)
  -- For polygons: MultiPolygon; for points: Point
  geom             geometry(GEOMETRY, 4326) NOT NULL,
  centroid         geometry(Point, 4326) NOT NULL,

  created_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- Prevent duplicate loads of same feature in same release
  UNIQUE (wdpa_release, wdpa_id, name)
);

-- ============================================
-- SPATIAL INDEXES (Critical for performance)
-- ============================================

-- Main geometry index for ST_Contains queries
CREATE INDEX IF NOT EXISTS idx_pa_geom_gist ON protected_areas USING GIST (geom);

-- Centroid index for ST_DWithin "nearby" queries
CREATE INDEX IF NOT EXISTS idx_pa_centroid_gist ON protected_areas USING GIST (centroid);

-- ============================================
-- ATTRIBUTE INDEXES
-- ============================================

CREATE INDEX IF NOT EXISTS idx_pa_wdpa_id ON protected_areas (wdpa_id);
CREATE INDEX IF NOT EXISTS idx_pa_iso3 ON protected_areas (iso3);
CREATE INDEX IF NOT EXISTS idx_pa_iucn ON protected_areas (iucn_category);
CREATE INDEX IF NOT EXISTS idx_pa_name ON protected_areas (name);
CREATE INDEX IF NOT EXISTS idx_pa_release ON protected_areas (wdpa_release);
CREATE INDEX IF NOT EXISTS idx_pa_designation ON protected_areas (designation);

-- ============================================
-- STAGING TABLE (for atomic updates)
-- ============================================

CREATE TABLE IF NOT EXISTS protected_areas_staging (LIKE protected_areas INCLUDING ALL);

-- ============================================
-- HELPER VIEWS
-- ============================================

-- View: Only latest WDPA release
CREATE OR REPLACE VIEW protected_areas_latest AS
SELECT *
FROM protected_areas
WHERE wdpa_release = (SELECT MAX(wdpa_release) FROM protected_areas);

-- View: Protected areas with simplified info
CREATE OR REPLACE VIEW protected_areas_summary AS
SELECT
  id,
  wdpa_id,
  name,
  designation,
  iucn_category,
  iso3,
  CASE 
    WHEN marine = '0' THEN 'terrestrial'
    WHEN marine = '1' THEN 'coastal'
    WHEN marine = '2' THEN 'marine'
    ELSE 'unknown'
  END AS marine_type,
  COALESCE(gis_area_km2, reported_area_km2) AS area_km2,
  centroid,
  ST_GeometryType(geom) AS geom_type,
  wdpa_release
FROM protected_areas;

-- ============================================
-- HELPER FUNCTIONS
-- ============================================

-- Function: Check if point is inside any protected area
CREATE OR REPLACE FUNCTION point_in_protected_area(
  p_lng DOUBLE PRECISION,
  p_lat DOUBLE PRECISION
) RETURNS TABLE (
  protected_area_id BIGINT,
  name TEXT,
  designation TEXT,
  iucn_category TEXT
) AS $$
BEGIN
  RETURN QUERY
  SELECT
    pa.id AS protected_area_id,
    pa.name,
    pa.designation,
    pa.iucn_category
  FROM protected_areas pa
  WHERE ST_Contains(
    pa.geom,
    ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326)
  )
  ORDER BY pa.name;
END;
$$ LANGUAGE plpgsql STABLE;

-- Function: Get protected areas near a point
CREATE OR REPLACE FUNCTION protected_areas_near_point(
  p_lng DOUBLE PRECISION,
  p_lat DOUBLE PRECISION,
  p_radius_km DOUBLE PRECISION DEFAULT 50
) RETURNS TABLE (
  protected_area_id BIGINT,
  name TEXT,
  designation TEXT,
  iucn_category TEXT,
  distance_km DOUBLE PRECISION,
  relation TEXT
) AS $$
DECLARE
  v_point geometry;
  v_radius_m INTEGER;
BEGIN
  v_point := ST_SetSRID(ST_MakePoint(p_lng, p_lat), 4326);
  v_radius_m := (p_radius_km * 1000)::INTEGER;
  
  RETURN QUERY
  -- First: areas containing the point
  SELECT
    pa.id AS protected_area_id,
    pa.name,
    pa.designation,
    pa.iucn_category,
    0.0::DOUBLE PRECISION AS distance_km,
    'inside'::TEXT AS relation
  FROM protected_areas pa
  WHERE ST_Contains(pa.geom, v_point)
  
  UNION ALL
  
  -- Then: areas nearby but not containing
  SELECT
    pa.id AS protected_area_id,
    pa.name,
    pa.designation,
    pa.iucn_category,
    (ST_DistanceSphere(pa.centroid, v_point) / 1000.0) AS distance_km,
    'near'::TEXT AS relation
  FROM protected_areas pa
  WHERE NOT ST_Contains(pa.geom, v_point)
    AND ST_DWithin(pa.centroid::geography, v_point::geography, v_radius_m)
  ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql STABLE;


-- ============================================
-- IMPORT WORKFLOW NOTES
-- ============================================

/*
WDPA Import Pipeline (Monthly)
==============================

1. Download from Protected Planet:
   - WDPA_WDOECM_<Month><Year>_Public.gpkg (or .shp)

2. Create staging table:
   TRUNCATE protected_areas_staging;

3. Load polygons with ogr2ogr:
   ogr2ogr -f "PostgreSQL" \
     PG:"$DATABASE_URL" \
     WDPA_polygons.gpkg \
     -nln protected_areas_staging \
     -nlt PROMOTE_TO_MULTI \
     -lco GEOMETRY_NAME=geom \
     -t_srs EPSG:4326 \
     -append

4. Load points with ogr2ogr:
   ogr2ogr -f "PostgreSQL" \
     PG:"$DATABASE_URL" \
     WDPA_points.gpkg \
     -nln protected_areas_staging \
     -nlt POINT \
     -lco GEOMETRY_NAME=geom \
     -t_srs EPSG:4326 \
     -append

5. Generate mapping SQL:
   npx ts-node tools/generate_wdpa_mapping.ts \
     --file WDPA_polygons.gpkg \
     --layer WDPA_poly_Jan2026 \
     --release 2026-01 > wdpa_map.sql

6. Run mapping:
   psql "$DATABASE_URL" -f wdpa_map.sql

7. Swap into production:
   BEGIN;
   TRUNCATE protected_areas;
   INSERT INTO protected_areas SELECT * FROM protected_areas_staging;
   COMMIT;
   VACUUM ANALYZE protected_areas;

WDPA License Note
=================
WDPA usage is governed by UNEP-WCMC's Data License.
Commercial use may require permission.
See: https://www.protectedplanet.net/en/legal
*/
