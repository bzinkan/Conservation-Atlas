-- ============================================
-- PLACE BRIEF SQL QUERIES
-- ============================================
-- These queries power the "click anywhere on map" feature
-- Parameters:
--   $1 = lng (double)
--   $2 = lat (double)
--   $3 = radius_meters (int)
--   $4 = window_days (int)
--   $5 = limit (int)
--   $6 = offset (int)

-- ============================================
-- A) Protected areas that CONTAIN the clicked point
-- ============================================
-- Returns protected areas where the user's click is inside the boundary

WITH params AS (
  SELECT ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt
)
SELECT
  pa.id AS protected_area_id,
  pa.name,
  pa.designation,
  pa.iucn_category,
  pa.country,
  pa.wdpa_id AS external_id,
  'wdpa' AS source,
  'inside' AS relation,
  0.0::double precision AS distance_km,
  ST_Y(COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary))) AS centroid_lat,
  ST_X(COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary))) AS centroid_lng,
  pa.area_km2
FROM protected_areas pa, params
WHERE pa.geom_boundary IS NOT NULL 
  AND ST_Contains(pa.geom_boundary, params.pt)
ORDER BY pa.name
LIMIT 50;


-- ============================================
-- B) Protected areas NEAR the clicked point
-- ============================================
-- Returns protected areas within radius but NOT containing the point

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m
)
SELECT
  pa.id AS protected_area_id,
  pa.name,
  pa.designation,
  pa.iucn_category,
  pa.country,
  pa.wdpa_id AS external_id,
  'wdpa' AS source,
  'near' AS relation,
  (ST_DistanceSphere(
    COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary)), 
    params.pt
  ) / 1000.0) AS distance_km,
  ST_Y(COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary))) AS centroid_lat,
  ST_X(COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary))) AS centroid_lng,
  pa.area_km2
FROM protected_areas pa, params
WHERE
  (pa.geom_boundary IS NOT NULL OR pa.geom_centroid IS NOT NULL)
  AND NOT COALESCE(ST_Contains(pa.geom_boundary, params.pt), false)
  AND ST_DWithin(
    COALESCE(pa.geom_centroid, ST_Centroid(pa.geom_boundary))::geography,
    params.pt::geography,
    params.radius_m
  )
ORDER BY distance_km ASC
LIMIT 20;


-- ============================================
-- C) Events within radius + time window (paginated)
-- ============================================
-- Main query for events near the clicked point

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m,
    (NOW() - make_interval(days => $4::integer)) AS since_ts
)
SELECT
  e.id AS event_id,
  e.title,
  e.event_type_primary,
  e.event_type_secondary,
  e.status,
  e.severity_level,
  e.confidence_extraction,
  e.confidence_geolocation,
  e.event_start,
  e.event_end,
  e.is_ongoing,
  e.summary_short,
  e.location_name,
  e.country,
  e.admin1,
  e.admin2,
  ST_Y(e.geom_point) AS lat,
  ST_X(e.geom_point) AS lng,
  (ST_DistanceSphere(e.geom_point, params.pt) / 1000.0) AS distance_km,
  e.nearest_protected_area_id,
  e.is_inside_protected_area,
  e.distance_to_protected_km,
  e.created_at,
  e.updated_at,
  COUNT(*) OVER() AS total_count
FROM events e, params
WHERE
  e.status = 'active'
  AND e.geom_point IS NOT NULL
  AND e.created_at >= params.since_ts
  AND ST_DWithin(e.geom_point::geography, params.pt::geography, params.radius_m)
ORDER BY e.severity_level DESC, e.created_at DESC
LIMIT $5 OFFSET $6;


-- ============================================
-- D) Sources for a set of events
-- ============================================
-- Pass an array of event ids: $1 = bigint[]

SELECT
  es.event_id,
  s.id AS source_id,
  s.url,
  s.publisher,
  s.published_at,
  s.source_type,
  s.credibility_score
FROM event_sources es
JOIN sources s ON s.id = es.source_id
WHERE es.event_id = ANY($1::bigint[])
ORDER BY es.event_id, s.published_at DESC NULLS LAST;


-- ============================================
-- E) Organizations for a set of events
-- ============================================
-- Pass an array of event ids: $1 = bigint[]

SELECT
  eo.event_id,
  o.id AS organization_id,
  o.name,
  o.org_type,
  o.website,
  eo.role,
  eo.involvement_confidence,
  eo.evidence_snippet
FROM event_organizations eo
JOIN organizations o ON o.id = eo.organization_id
WHERE eo.event_id = ANY($1::bigint[])
ORDER BY eo.event_id, eo.involvement_confidence DESC, o.name;


-- ============================================
-- F) Aggregated organizations with full metrics (JSON)
-- ============================================
-- Returns one row per org with JSON arrays for roles, event types, and top events

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m,
    (NOW() - make_interval(days => $4::integer)) AS since_ts
),
area_events AS (
  SELECT
    e.id AS event_id,
    e.title,
    e.event_type_primary,
    e.severity_level,
    e.created_at AS updated_at,
    e.geom_point
  FROM events e, params
  WHERE
    e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND e.created_at >= params.since_ts
    AND ST_DWithin(e.geom_point::geography, params.pt::geography, params.radius_m)
),
org_events AS (
  SELECT
    o.id AS organization_id,
    o.name,
    o.org_type,
    o.website,
    o.logo_url,
    eo.role,
    eo.involvement_confidence,
    ae.event_id,
    ae.title AS event_title,
    ae.event_type_primary,
    ae.severity_level,
    ae.updated_at
  FROM area_events ae
  JOIN event_organizations eo ON eo.event_id = ae.event_id
  JOIN organizations o ON o.id = eo.organization_id
),
org_rollup AS (
  SELECT
    organization_id,
    name,
    org_type,
    website,
    logo_url,
    COUNT(DISTINCT event_id) AS events_count,
    MAX(updated_at) AS most_recent_activity
  FROM org_events
  GROUP BY organization_id, name, org_type, website, logo_url
),
top_orgs AS (
  SELECT *
  FROM org_rollup
  ORDER BY events_count DESC, most_recent_activity DESC
  LIMIT $5
),
roles_ranked AS (
  SELECT
    oe.organization_id,
    oe.role,
    COUNT(*)::int AS count,
    ROW_NUMBER() OVER (PARTITION BY oe.organization_id ORDER BY COUNT(*) DESC, oe.role ASC) AS rn
  FROM org_events oe
  JOIN top_orgs t ON t.organization_id = oe.organization_id
  GROUP BY oe.organization_id, oe.role
),
event_types_ranked AS (
  SELECT
    oe.organization_id,
    oe.event_type_primary AS event_type,
    COUNT(DISTINCT oe.event_id)::int AS count,
    ROW_NUMBER() OVER (PARTITION BY oe.organization_id ORDER BY COUNT(DISTINCT oe.event_id) DESC, oe.event_type_primary ASC) AS rn
  FROM org_events oe
  JOIN top_orgs t ON t.organization_id = oe.organization_id
  GROUP BY oe.organization_id, oe.event_type_primary
),
events_ranked AS (
  SELECT
    oe.organization_id,
    oe.event_id,
    oe.event_title,
    oe.severity_level,
    oe.updated_at,
    ROW_NUMBER() OVER (
      PARTITION BY oe.organization_id
      ORDER BY oe.severity_level DESC, oe.updated_at DESC, oe.event_id DESC
    ) AS rn
  FROM (
    SELECT DISTINCT
      organization_id, event_id, event_title, severity_level, updated_at
    FROM org_events
  ) oe
  JOIN top_orgs t ON t.organization_id = oe.organization_id
)
SELECT
  t.organization_id,
  t.name,
  COALESCE(t.org_type, 'unknown') AS org_type,
  t.website,
  t.logo_url,
  t.events_count,
  t.most_recent_activity,
  COUNT(*) OVER() AS total_orgs,

  COALESCE(
    (
      SELECT jsonb_agg(jsonb_build_object('role', rr.role, 'count', rr.count) ORDER BY rr.count DESC, rr.role ASC)
      FROM roles_ranked rr
      WHERE rr.organization_id = t.organization_id AND rr.rn <= 3
    ),
    '[]'::jsonb
  ) AS most_common_roles,

  COALESCE(
    (
      SELECT jsonb_agg(jsonb_build_object('event_type', et.event_type, 'count', et.count) ORDER BY et.count DESC, et.event_type ASC)
      FROM event_types_ranked et
      WHERE et.organization_id = t.organization_id AND et.rn <= 3
    ),
    '[]'::jsonb
  ) AS top_event_types,

  COALESCE(
    (
      SELECT jsonb_agg(
        jsonb_build_object(
          'event_id', er.event_id,
          'title', er.event_title,
          'severity_level', er.severity_level
        )
        ORDER BY er.severity_level DESC, er.updated_at DESC
      )
      FROM events_ranked er
      WHERE er.organization_id = t.organization_id AND er.rn <= 3
    ),
    '[]'::jsonb
  ) AS top_related_events

FROM top_orgs t
ORDER BY t.events_count DESC, t.most_recent_activity DESC;


-- ============================================
-- G) Sources backing each org's activity (JSON)
-- ============================================
-- Returns top N sources per org as JSON array
-- $5 = org_ids (bigint[]), $6 = per_org_sources_limit

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m,
    (NOW() - make_interval(days => $4::integer)) AS since_ts
),
area_events AS (
  SELECT e.id AS event_id, e.created_at, e.geom_point
  FROM events e, params
  WHERE
    e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND e.created_at >= params.since_ts
    AND ST_DWithin(e.geom_point::geography, params.pt::geography, params.radius_m)
),
org_event_sources AS (
  SELECT
    eo.organization_id,
    s.id AS source_id,
    s.url,
    s.publisher,
    s.title,
    s.published_at,
    s.source_type,
    ROW_NUMBER() OVER (
      PARTITION BY eo.organization_id
      ORDER BY s.published_at DESC NULLS LAST, s.id DESC
    ) AS rn
  FROM area_events ae
  JOIN event_organizations eo ON eo.event_id = ae.event_id
  JOIN event_sources es ON es.event_id = ae.event_id
  JOIN sources s ON s.id = es.source_id
  WHERE eo.organization_id = ANY($5::bigint[])
)
SELECT
  organization_id,
  jsonb_agg(
    jsonb_build_object(
      'source_id', source_id,
      'url', url,
      'publisher', publisher,
      'published_at', published_at,
      'source_type', source_type
    )
    ORDER BY published_at DESC NULLS LAST
  ) AS sources
FROM org_event_sources
WHERE rn <= $6
GROUP BY organization_id;


-- ============================================
-- H) Activity summary per org (template-based, no LLM)
-- ============================================
-- Generates human-readable summaries from role + event type + date
-- Example: "Recently reported updates on invasive species in this area (latest: Jan 17, 2026)."

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m,
    (NOW() - make_interval(days => $4::integer)) AS since_ts
),
area_events AS (
  SELECT e.id AS event_id, e.event_type_primary, e.created_at AS updated_at, e.geom_point
  FROM events e, params
  WHERE
    e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND e.created_at >= params.since_ts
    AND ST_DWithin(e.geom_point::geography, params.pt::geography, params.radius_m)
),
org_events AS (
  SELECT
    eo.organization_id,
    eo.role,
    ae.event_type_primary,
    ae.updated_at
  FROM area_events ae
  JOIN event_organizations eo ON eo.event_id = ae.event_id
  WHERE eo.organization_id = ANY($5::bigint[])
),
top_role AS (
  SELECT organization_id, role
  FROM (
    SELECT
      organization_id,
      role,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY COUNT(*) DESC, role ASC) AS rn
    FROM org_events
    GROUP BY organization_id, role
  ) x
  WHERE rn = 1
),
top_type AS (
  SELECT organization_id, event_type_primary
  FROM (
    SELECT
      organization_id,
      event_type_primary,
      COUNT(*) AS cnt,
      ROW_NUMBER() OVER (PARTITION BY organization_id ORDER BY COUNT(*) DESC, event_type_primary ASC) AS rn
    FROM org_events
    GROUP BY organization_id, event_type_primary
  ) x
  WHERE rn = 1
),
recent AS (
  SELECT organization_id, MAX(updated_at) AS most_recent_activity
  FROM org_events
  GROUP BY organization_id
)
SELECT
  r.organization_id,
  (
    'Recently ' ||
    (CASE COALESCE(tr.role, 'mentioned')
      WHEN 'reporting' THEN 'reported updates on '
      WHEN 'researching' THEN 'shared research related to '
      WHEN 'funding' THEN 'supported work related to '
      WHEN 'managing' THEN 'managed efforts related to '
      WHEN 'implementing' THEN 'implemented actions related to '
      WHEN 'responding' THEN 'responded to '
      WHEN 'enforcing' THEN 'supported enforcement related to '
      ELSE 'worked on '
    END) ||
    (CASE COALESCE(tt.event_type_primary, 'conservation')
      WHEN 'wildfire'            THEN 'wildfire impacts'
      WHEN 'deforestation'       THEN 'forest loss and protection'
      WHEN 'illegal_logging'     THEN 'illegal logging concerns'
      WHEN 'invasive_species'    THEN 'invasive species'
      WHEN 'pollution'           THEN 'pollution concerns'
      WHEN 'oil_spill'           THEN 'oil spill response'
      WHEN 'disease_outbreak'    THEN 'wildlife health and disease'
      WHEN 'coral_bleaching'     THEN 'coral bleaching'
      WHEN 'habitat_loss'        THEN 'habitat protection'
      WHEN 'climate_impact'      THEN 'climate impacts'
      WHEN 'restoration'         THEN 'restoration projects'
      WHEN 'conservation_win'    THEN 'conservation successes'
      WHEN 'policy_change'       THEN 'policy and management changes'
      WHEN 'poaching'            THEN 'anti-poaching efforts'
      ELSE REPLACE(COALESCE(tt.event_type_primary, 'conservation'), '_', ' ')
    END) ||
    ' in this area (latest: ' ||
    to_char(r.most_recent_activity, 'Mon DD, YYYY') ||
    ').'
  ) AS activity_summary
FROM recent r
LEFT JOIN top_role tr ON tr.organization_id = r.organization_id
LEFT JOIN top_type tt ON tt.organization_id = r.organization_id;


-- ============================================
-- H) Coverage statistics for an area
-- ============================================
-- Used to calculate coverage_score

WITH params AS (
  SELECT
    ST_SetSRID(ST_MakePoint($1::double precision, $2::double precision), 4326) AS pt,
    $3::integer AS radius_m,
    (NOW() - make_interval(days => $4::integer)) AS since_ts
),
area_events AS (
  SELECT e.id
  FROM events e, params
  WHERE e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND e.created_at >= params.since_ts
    AND ST_DWithin(e.geom_point::geography, params.pt::geography, params.radius_m)
)
SELECT
  COUNT(DISTINCT ae.id) AS events_count,
  COUNT(DISTINCT s.id) AS sources_count
FROM area_events ae
LEFT JOIN event_sources es ON ae.id = es.event_id
LEFT JOIN sources s ON es.source_id = s.id;


-- ============================================
-- I) Top events for organizations
-- ============================================
-- Get top events for a list of org IDs
-- $1 = bigint[] of organization IDs

SELECT 
  eo.organization_id,
  e.id AS event_id,
  e.title,
  e.severity_level
FROM event_organizations eo
JOIN events e ON eo.event_id = e.id
WHERE eo.organization_id = ANY($1::bigint[])
  AND e.status = 'active'
ORDER BY eo.organization_id, e.severity_level DESC, e.created_at DESC;


-- ============================================
-- PERFORMANCE NOTES
-- ============================================

-- Required indexes for good performance:
-- 
-- CREATE INDEX idx_events_geom ON events USING GIST (geom_point);
-- CREATE INDEX idx_events_status_created ON events(status, created_at DESC);
-- CREATE INDEX idx_protected_areas_geom ON protected_areas USING GIST (geom_boundary);
-- CREATE INDEX idx_protected_areas_centroid ON protected_areas USING GIST (geom_centroid);
-- CREATE INDEX idx_event_sources_event ON event_sources(event_id);
-- CREATE INDEX idx_event_orgs_event ON event_organizations(event_id);
-- CREATE INDEX idx_event_orgs_org ON event_organizations(organization_id);

-- For very large datasets (millions of events), consider:
-- 1. Partitioning events by created_at (range partitioning)
-- 2. Using PostGIS clustering (ST_ClusterDBSCAN)
-- 3. Pre-computing statistics per grid cell (H3 hexagons)
-- 4. Caching popular areas (Redis with geo bounds as key)
