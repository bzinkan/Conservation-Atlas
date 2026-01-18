-- Conservation Atlas - Minimal Postgres/PostGIS Schema
-- 
-- Run with: psql -d conservation_atlas -f schema.sql
-- Requires PostGIS extension

-- Enable PostGIS
CREATE EXTENSION IF NOT EXISTS postgis;
CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ============================================
-- SOURCES: Raw ingested items from ScraperBee/RSS
-- One row per URL fetch
-- ============================================
CREATE TABLE IF NOT EXISTS sources (
  id              BIGSERIAL PRIMARY KEY,
  url             TEXT NOT NULL UNIQUE,
  publisher       TEXT,
  title           TEXT,
  language        TEXT DEFAULT 'en',
  published_at    TIMESTAMPTZ,
  retrieved_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  -- MVP: store cleaned text in DB (later move to S3 + store s3_key)
  raw_text        TEXT,
  raw_html        TEXT,
  s3_raw_key      TEXT,

  -- Source classification
  source_type     TEXT DEFAULT 'news', -- gov, ngo, academic, news, blog
  credibility_score DOUBLE PRECISION,

  -- Processing status
  extraction_status TEXT DEFAULT 'pending', -- pending, extracted, failed, irrelevant, too_short

  -- Content hash for deduplication
  content_hash    TEXT,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_sources_published_at ON sources(published_at);
CREATE INDEX IF NOT EXISTS idx_sources_retrieved_at ON sources(retrieved_at);
CREATE INDEX IF NOT EXISTS idx_sources_extraction_status ON sources(extraction_status);
CREATE INDEX IF NOT EXISTS idx_sources_content_hash ON sources(content_hash);

-- ============================================
-- EVENTS: Canonical event records (deduped)
-- ============================================
CREATE TABLE IF NOT EXISTS events (
  id                      BIGSERIAL PRIMARY KEY,
  
  -- Identification
  title                   TEXT NOT NULL,
  event_type_primary      TEXT NOT NULL DEFAULT 'other',
  event_type_secondary    TEXT,
  
  -- Scoring
  severity_level          INTEGER NOT NULL DEFAULT 3 CHECK (severity_level BETWEEN 1 AND 5),
  confidence_extraction   DOUBLE PRECISION NOT NULL DEFAULT 0.5 CHECK (confidence_extraction BETWEEN 0 AND 1),
  confidence_geolocation  DOUBLE PRECISION DEFAULT 0.5 CHECK (confidence_geolocation BETWEEN 0 AND 1),
  
  -- Content
  summary_short           TEXT NOT NULL DEFAULT '',
  summary_detailed        TEXT,

  -- Status
  status                  TEXT NOT NULL DEFAULT 'active', -- active, merged, archived, disputed

  -- Temporal
  event_start             TIMESTAMPTZ,
  event_end               TIMESTAMPTZ,
  is_ongoing              BOOLEAN DEFAULT true,

  -- Geography
  location_name           TEXT NOT NULL DEFAULT '',
  country                 TEXT NOT NULL DEFAULT '',
  admin1                  TEXT,
  admin2                  TEXT,
  geom_point              geometry(Point, 4326),

  -- Classification
  is_classroom_safe       BOOLEAN DEFAULT false,
  classroom_topic_tags    TEXT[] DEFAULT '{}',

  -- Protected area relationship
  nearest_protected_area_id BIGINT,
  distance_to_protected_km  DOUBLE PRECISION,
  is_inside_protected_area  BOOLEAN DEFAULT false,

  -- Source tracking
  source_count            INTEGER DEFAULT 1,
  primary_source_type     TEXT,

  -- Processing flags
  episode_generated       BOOLEAN DEFAULT false,
  episode_eligible        BOOLEAN DEFAULT false,
  briefing_generated      BOOLEAN DEFAULT false,

  -- Clustering/deduplication
  cluster_key             TEXT,
  merged_into_id          BIGINT REFERENCES events(id),

  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_events_type ON events(event_type_primary);
CREATE INDEX IF NOT EXISTS idx_events_severity ON events(severity_level);
CREATE INDEX IF NOT EXISTS idx_events_country ON events(country);
CREATE INDEX IF NOT EXISTS idx_events_geom ON events USING GIST (geom_point);
CREATE INDEX IF NOT EXISTS idx_events_cluster_key ON events(cluster_key);
CREATE INDEX IF NOT EXISTS idx_events_status ON events(status);
CREATE INDEX IF NOT EXISTS idx_events_classroom_safe ON events(is_classroom_safe) WHERE is_classroom_safe = true;
CREATE INDEX IF NOT EXISTS idx_events_episode_eligible ON events(episode_eligible) WHERE episode_eligible = true;
CREATE INDEX IF NOT EXISTS idx_events_merged_into ON events(merged_into_id);
CREATE INDEX IF NOT EXISTS idx_events_created ON events(created_at DESC);

-- ============================================
-- SOURCE_EXTRACTIONS: Raw LLM extraction output
-- Links source -> event, stores JSON for audit
-- ============================================
CREATE TABLE IF NOT EXISTS source_extractions (
  id              BIGSERIAL PRIMARY KEY,
  source_id       BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,
  event_id        BIGINT REFERENCES events(id) ON DELETE SET NULL,

  schema_version  TEXT NOT NULL,
  model           TEXT NOT NULL,

  extraction_json JSONB NOT NULL,
  
  -- Validation results
  quality_score   DOUBLE PRECISION,
  geo_validation  JSONB,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  
  UNIQUE(source_id)
);

CREATE INDEX IF NOT EXISTS idx_source_extractions_event_id ON source_extractions(event_id);
CREATE INDEX IF NOT EXISTS idx_source_extractions_created ON source_extractions(created_at);

-- ============================================
-- EVENT_SOURCES: Many-to-many link from event to sources
-- After deduplication, multiple sources can be attached
-- ============================================
CREATE TABLE IF NOT EXISTS event_sources (
  id            BIGSERIAL PRIMARY KEY,
  event_id      BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  source_id     BIGINT NOT NULL REFERENCES sources(id) ON DELETE CASCADE,

  -- Provenance / ranking for UI
  relation      TEXT NOT NULL DEFAULT 'supporting', -- primary, supporting
  weight        DOUBLE PRECISION NOT NULL DEFAULT 1.0,

  created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE(event_id, source_id)
);

CREATE INDEX IF NOT EXISTS idx_event_sources_event_id ON event_sources(event_id);
CREATE INDEX IF NOT EXISTS idx_event_sources_source_id ON event_sources(source_id);

-- ============================================
-- EVENT_MERGES: Audit trail for deduplication
-- ============================================
CREATE TABLE IF NOT EXISTS event_merges (
  id                BIGSERIAL PRIMARY KEY,
  primary_event_id  BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  merged_event_id   BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  
  similarity_score  DOUBLE PRECISION NOT NULL,
  merge_reason      TEXT,
  
  merged_at         TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_event_merges_primary ON event_merges(primary_event_id);
CREATE INDEX IF NOT EXISTS idx_event_merges_merged ON event_merges(merged_event_id);

-- ============================================
-- PROTECTED_AREAS: Reference data
-- ============================================
CREATE TABLE IF NOT EXISTS protected_areas (
  id              BIGSERIAL PRIMARY KEY,
  
  name            TEXT NOT NULL,
  designation     TEXT,
  wdpa_id         INTEGER,
  
  country         TEXT NOT NULL,
  geom_boundary   geometry(MultiPolygon, 4326),
  geom_centroid   geometry(Point, 4326),
  area_km2        DOUBLE PRECISION,
  
  iucn_category   TEXT,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_protected_areas_geom ON protected_areas USING GIST (geom_boundary);
CREATE INDEX IF NOT EXISTS idx_protected_areas_country ON protected_areas(country);
CREATE INDEX IF NOT EXISTS idx_protected_areas_wdpa ON protected_areas(wdpa_id);

-- ============================================
-- CLASSROOM_EPISODES: Generated educational content
-- ============================================
CREATE TABLE IF NOT EXISTS classroom_episodes (
  id                      BIGSERIAL PRIMARY KEY,
  
  episode_title           TEXT NOT NULL,
  episode_type            TEXT NOT NULL, -- single_event, weekly_recap, topic_deep_dive
  
  grade_band              TEXT NOT NULL, -- K2, 35, 68, 912
  topic_tags              TEXT[] DEFAULT '{}',
  
  run_time_seconds        INTEGER NOT NULL,
  
  -- Student content
  summary_for_students    TEXT NOT NULL,
  hook_question           TEXT,
  key_takeaway            TEXT,
  
  -- Script
  script_text             TEXT NOT NULL,
  script_sections         JSONB,
  
  -- Teacher toolkit
  learning_targets        TEXT[] DEFAULT '{}',
  vocab_terms             JSONB DEFAULT '[]',
  quick_check             JSONB DEFAULT '[]',
  discussion_prompts      TEXT[] DEFAULT '{}',
  activity_description    TEXT,
  exit_ticket             TEXT,
  
  -- Assets
  teacher_toolkit_pdf_url TEXT,
  video_url               TEXT,
  thumbnail_url           TEXT,
  
  -- Status
  status                  TEXT NOT NULL DEFAULT 'draft', -- draft, generating, published, failed
  generation_error        TEXT,
  pictory_project_id      TEXT,
  
  -- Timestamps
  published_at            TIMESTAMPTZ,
  week_of                 DATE,
  created_at              TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at              TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_episodes_grade ON classroom_episodes(grade_band);
CREATE INDEX IF NOT EXISTS idx_episodes_status ON classroom_episodes(status);
CREATE INDEX IF NOT EXISTS idx_episodes_published ON classroom_episodes(published_at DESC);
CREATE INDEX IF NOT EXISTS idx_episodes_week ON classroom_episodes(week_of);

-- ============================================
-- EPISODE_EVENTS: Link episodes to source events
-- ============================================
CREATE TABLE IF NOT EXISTS episode_events (
  episode_id      BIGINT NOT NULL REFERENCES classroom_episodes(id) ON DELETE CASCADE,
  event_id        BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  
  relation_type   TEXT NOT NULL, -- primary_story, supporting_example, background_context
  order_index     INTEGER NOT NULL DEFAULT 0,
  content_used    TEXT,
  
  PRIMARY KEY (episode_id, event_id)
);

CREATE INDEX IF NOT EXISTS idx_episode_events_episode ON episode_events(episode_id);
CREATE INDEX IF NOT EXISTS idx_episode_events_event ON episode_events(event_id);

-- ============================================
-- VIDEO_BRIEFINGS: Short 30-90 second videos
-- ============================================
CREATE TABLE IF NOT EXISTS video_briefings (
  id                  BIGSERIAL PRIMARY KEY,
  event_id            BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  
  title               TEXT NOT NULL,
  duration_seconds    INTEGER NOT NULL,
  
  script_json         JSONB NOT NULL,
  
  -- Assets
  video_url           TEXT,
  thumbnail_url       TEXT,
  
  -- Status
  status              TEXT NOT NULL DEFAULT 'draft',
  pictory_project_id  TEXT,
  generation_error    TEXT,
  
  created_at          TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  published_at        TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_briefings_event ON video_briefings(event_id);
CREATE INDEX IF NOT EXISTS idx_briefings_status ON video_briefings(status);

-- ============================================
-- RSS_FEED_CONFIGS: Scraping configuration
-- ============================================
CREATE TABLE IF NOT EXISTS rss_feed_configs (
  id              BIGSERIAL PRIMARY KEY,
  
  url             TEXT NOT NULL UNIQUE,
  name            TEXT NOT NULL,
  source_type     TEXT NOT NULL, -- gov, ngo, academic, news
  
  priority        TEXT DEFAULT 'normal', -- high, normal, low
  scrape_interval INTEGER DEFAULT 120, -- minutes
  
  is_active       BOOLEAN DEFAULT true,
  last_scraped_at TIMESTAMPTZ,
  last_error      TEXT,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_rss_feeds_active ON rss_feed_configs(is_active);

-- ============================================
-- JOB_LOGS: Pipeline monitoring
-- ============================================
CREATE TABLE IF NOT EXISTS job_logs (
  id              BIGSERIAL PRIMARY KEY,
  
  job_id          TEXT NOT NULL,
  job_type        TEXT NOT NULL,
  correlation_id  TEXT NOT NULL,
  
  status          TEXT NOT NULL, -- started, completed, failed
  duration_ms     INTEGER,
  
  input_payload   JSONB,
  output_summary  JSONB,
  error_message   TEXT,
  error_stack     TEXT,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_job_logs_job_id ON job_logs(job_id);
CREATE INDEX IF NOT EXISTS idx_job_logs_type ON job_logs(job_type);
CREATE INDEX IF NOT EXISTS idx_job_logs_correlation ON job_logs(correlation_id);
CREATE INDEX IF NOT EXISTS idx_job_logs_status ON job_logs(status);
CREATE INDEX IF NOT EXISTS idx_job_logs_created ON job_logs(created_at DESC);

-- ============================================
-- ORGANIZATIONS: Conservation orgs extracted from sources
-- ============================================
CREATE TABLE IF NOT EXISTS organizations (
  id              BIGSERIAL PRIMARY KEY,

  -- Canonical display name
  name            TEXT NOT NULL,

  -- Normalization/dedupe helpers
  normalized_name TEXT NOT NULL,

  -- Optional enrichments (can be filled later)
  website         TEXT,
  org_type        TEXT, -- ngo|government|academic|community|private|intergov|unknown
  country_base    TEXT,
  description     TEXT,
  logo_url        TEXT,

  -- Provenance / trust
  confidence      DOUBLE PRECISION NOT NULL DEFAULT 0.7 CHECK (confidence BETWEEN 0 AND 1),
  
  -- Stats (denormalized for performance)
  event_count     INTEGER DEFAULT 0,
  last_seen_at    TIMESTAMPTZ,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (normalized_name)
);

CREATE INDEX IF NOT EXISTS idx_orgs_name ON organizations(name);
CREATE INDEX IF NOT EXISTS idx_orgs_normalized ON organizations(normalized_name);
CREATE INDEX IF NOT EXISTS idx_orgs_country_base ON organizations(country_base);
CREATE INDEX IF NOT EXISTS idx_orgs_type ON organizations(org_type);
CREATE INDEX IF NOT EXISTS idx_orgs_event_count ON organizations(event_count DESC);

-- ============================================
-- EVENT_ORGANIZATIONS: Many-to-many link
-- Links events to organizations mentioned/acting in them
-- ============================================
CREATE TABLE IF NOT EXISTS event_organizations (
  id              BIGSERIAL PRIMARY KEY,
  event_id        BIGINT NOT NULL REFERENCES events(id) ON DELETE CASCADE,
  organization_id BIGINT NOT NULL REFERENCES organizations(id) ON DELETE CASCADE,

  -- What is this org doing in the context of the event?
  role            TEXT NOT NULL DEFAULT 'mentioned',
  -- implementing|reporting|funding|managing|researching|enforcing|responding|mentioned|unknown

  -- Confidence that the org is actually involved (vs just mentioned)
  involvement_confidence DOUBLE PRECISION NOT NULL DEFAULT 0.7 CHECK (involvement_confidence BETWEEN 0 AND 1),

  -- Short free text extracted by LLM
  evidence_snippet TEXT,

  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),

  UNIQUE (event_id, organization_id, role)
);

CREATE INDEX IF NOT EXISTS idx_event_orgs_event ON event_organizations(event_id);
CREATE INDEX IF NOT EXISTS idx_event_orgs_org ON event_organizations(organization_id);
CREATE INDEX IF NOT EXISTS idx_event_orgs_role ON event_organizations(role);

-- ============================================
-- COVERAGE_REQUESTS: User requests for better coverage
-- ============================================
CREATE TABLE IF NOT EXISTS coverage_requests (
  id              BIGSERIAL PRIMARY KEY,
  
  -- Location requested
  latitude        DOUBLE PRECISION NOT NULL,
  longitude       DOUBLE PRECISION NOT NULL,
  radius_km       DOUBLE PRECISION DEFAULT 50,
  
  -- Reverse geocoded info
  country         TEXT,
  admin1          TEXT,
  locality        TEXT,
  
  -- User info (optional)
  user_id         UUID REFERENCES users(id),
  user_email      TEXT,
  notes           TEXT,
  
  -- Processing
  status          TEXT DEFAULT 'pending', -- pending, reviewed, actioned, declined
  reviewed_at     TIMESTAMPTZ,
  sources_added   INTEGER DEFAULT 0,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_coverage_requests_status ON coverage_requests(status);
CREATE INDEX IF NOT EXISTS idx_coverage_requests_location ON coverage_requests USING GIST (
  ST_SetSRID(ST_MakePoint(longitude, latitude), 4326)
);

-- ============================================
-- USERS: User accounts
-- ============================================
CREATE TABLE IF NOT EXISTS users (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  
  email           TEXT NOT NULL UNIQUE,
  password_hash   TEXT,
  
  name            TEXT,
  role            TEXT DEFAULT 'user', -- user, educator, researcher, admin
  organization    TEXT,
  
  preferences     JSONB DEFAULT '{}',
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_login_at   TIMESTAMPTZ
);

-- ============================================
-- ALERT_SUBSCRIPTIONS: User alert preferences
-- ============================================
CREATE TABLE IF NOT EXISTS alert_subscriptions (
  id              UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
  user_id         UUID NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  
  -- Filter criteria
  event_types     TEXT[] DEFAULT '{}',
  min_severity    INTEGER,
  countries       TEXT[] DEFAULT '{}',
  
  -- Delivery
  delivery_method TEXT NOT NULL, -- email, push, webhook
  frequency       TEXT NOT NULL, -- immediate, daily, weekly
  
  is_active       BOOLEAN DEFAULT true,
  
  created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  last_sent_at    TIMESTAMPTZ
);

CREATE INDEX IF NOT EXISTS idx_alert_subs_user ON alert_subscriptions(user_id);
CREATE INDEX IF NOT EXISTS idx_alert_subs_active ON alert_subscriptions(is_active);

-- ============================================
-- UPDATED_AT TRIGGER
-- Automatically updates updated_at timestamp
-- ============================================
CREATE OR REPLACE FUNCTION update_updated_at_column()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = NOW();
  RETURN NEW;
END;
$$ language 'plpgsql';

-- Apply trigger to all tables with updated_at
DO $$
DECLARE
  t text;
BEGIN
  FOR t IN 
    SELECT table_name 
    FROM information_schema.columns 
    WHERE column_name = 'updated_at' 
      AND table_schema = 'public'
  LOOP
    EXECUTE format('
      DROP TRIGGER IF EXISTS update_%I_updated_at ON %I;
      CREATE TRIGGER update_%I_updated_at
        BEFORE UPDATE ON %I
        FOR EACH ROW
        EXECUTE FUNCTION update_updated_at_column();
    ', t, t, t, t);
  END LOOP;
END $$;

-- ============================================
-- USEFUL VIEWS
-- ============================================

-- Events with source count and latest source
CREATE OR REPLACE VIEW events_with_sources AS
SELECT 
  e.*,
  COUNT(es.id) as actual_source_count,
  MAX(s.published_at) as latest_source_date,
  array_agg(DISTINCT s.publisher) FILTER (WHERE s.publisher IS NOT NULL) as publishers
FROM events e
LEFT JOIN event_sources es ON e.id = es.event_id
LEFT JOIN sources s ON es.source_id = s.id
WHERE e.status = 'active'
GROUP BY e.id;

-- Events near protected areas
CREATE OR REPLACE VIEW events_near_protected_areas AS
SELECT 
  e.id,
  e.title,
  e.event_type_primary,
  e.severity_level,
  e.country,
  pa.name as protected_area_name,
  pa.designation,
  e.distance_to_protected_km,
  e.is_inside_protected_area
FROM events e
JOIN protected_areas pa ON e.nearest_protected_area_id = pa.id
WHERE e.status = 'active'
  AND (e.is_inside_protected_area = true OR e.distance_to_protected_km < 50);

-- Episode-eligible events
CREATE OR REPLACE VIEW episode_eligible_events AS
SELECT 
  e.*,
  COUNT(DISTINCT es.source_id) as source_count,
  MAX(CASE WHEN s.source_type IN ('gov', 'ngo') THEN 1 ELSE 0 END) as has_authoritative_source
FROM events e
LEFT JOIN event_sources es ON e.id = es.event_id
LEFT JOIN sources s ON es.source_id = s.id
WHERE e.status = 'active'
  AND e.is_classroom_safe = true
  AND e.episode_generated = false
  AND e.severity_level >= 4
  AND e.confidence_extraction >= 0.75
GROUP BY e.id
HAVING COUNT(DISTINCT es.source_id) >= 2 
    OR MAX(CASE WHEN s.source_type IN ('gov', 'ngo') THEN 1 ELSE 0 END) = 1;

-- ============================================
-- PLACE BRIEF VIEWS
-- ============================================

-- Organizations with activity metrics
CREATE OR REPLACE VIEW organizations_with_activity AS
SELECT 
  o.*,
  COUNT(DISTINCT eo.event_id) as calculated_event_count,
  MAX(e.created_at) as last_activity_at,
  array_agg(DISTINCT eo.role) FILTER (WHERE eo.role IS NOT NULL) as roles_used,
  array_agg(DISTINCT e.event_type_primary) FILTER (WHERE e.event_type_primary IS NOT NULL) as event_types_involved
FROM organizations o
LEFT JOIN event_organizations eo ON o.id = eo.organization_id
LEFT JOIN events e ON eo.event_id = e.id AND e.status = 'active'
GROUP BY o.id;

-- Events with full context (for Place Brief)
CREATE OR REPLACE VIEW events_full_context AS
SELECT 
  e.*,
  pa.name as protected_area_name,
  pa.designation as protected_area_designation,
  COUNT(DISTINCT es.source_id) as actual_source_count,
  COUNT(DISTINCT eo.organization_id) as org_count,
  array_agg(DISTINCT s.publisher) FILTER (WHERE s.publisher IS NOT NULL) as publishers,
  array_agg(DISTINCT o.name) FILTER (WHERE o.name IS NOT NULL) as organization_names
FROM events e
LEFT JOIN protected_areas pa ON e.nearest_protected_area_id = pa.id
LEFT JOIN event_sources es ON e.id = es.event_id
LEFT JOIN sources s ON es.source_id = s.id
LEFT JOIN event_organizations eo ON e.id = eo.event_id
LEFT JOIN organizations o ON eo.organization_id = o.id
WHERE e.status = 'active'
GROUP BY e.id, pa.name, pa.designation;

-- ============================================
-- PLACE BRIEF FUNCTIONS
-- ============================================

-- Get organizations active within a radius of a point
CREATE OR REPLACE FUNCTION get_organizations_near_point(
  center_lat DOUBLE PRECISION,
  center_lng DOUBLE PRECISION,
  radius_km DOUBLE PRECISION,
  time_window_days INTEGER DEFAULT 30,
  max_results INTEGER DEFAULT 10
)
RETURNS TABLE (
  organization_id BIGINT,
  name TEXT,
  org_type TEXT,
  website TEXT,
  events_count BIGINT,
  roles TEXT[],
  event_types TEXT[]
) AS $$
DECLARE
  time_start TIMESTAMPTZ := NOW() - (time_window_days || ' days')::INTERVAL;
  radius_meters DOUBLE PRECISION := radius_km * 1000;
BEGIN
  RETURN QUERY
  SELECT 
    o.id as organization_id,
    o.name,
    o.org_type,
    o.website,
    COUNT(DISTINCT e.id) as events_count,
    array_agg(DISTINCT eo.role) as roles,
    array_agg(DISTINCT e.event_type_primary) as event_types
  FROM organizations o
  JOIN event_organizations eo ON o.id = eo.organization_id
  JOIN events e ON eo.event_id = e.id
  WHERE e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND e.created_at >= time_start
    AND ST_DWithin(
      e.geom_point::geography,
      ST_SetSRID(ST_MakePoint(center_lng, center_lat), 4326)::geography,
      radius_meters
    )
  GROUP BY o.id
  ORDER BY events_count DESC
  LIMIT max_results;
END;
$$ LANGUAGE plpgsql;

-- Get coverage statistics for an area
CREATE OR REPLACE FUNCTION get_area_coverage_stats(
  center_lat DOUBLE PRECISION,
  center_lng DOUBLE PRECISION,
  radius_km DOUBLE PRECISION,
  time_window_days INTEGER DEFAULT 30
)
RETURNS TABLE (
  events_count BIGINT,
  sources_count BIGINT,
  organizations_count BIGINT,
  protected_areas_count BIGINT,
  severity_distribution JSONB,
  type_distribution JSONB,
  coverage_score DOUBLE PRECISION
) AS $$
DECLARE
  time_start TIMESTAMPTZ := NOW() - (time_window_days || ' days')::INTERVAL;
  radius_meters DOUBLE PRECISION := radius_km * 1000;
BEGIN
  RETURN QUERY
  WITH area_events AS (
    SELECT e.*
    FROM events e
    WHERE e.status = 'active'
      AND e.geom_point IS NOT NULL
      AND e.created_at >= time_start
      AND ST_DWithin(
        e.geom_point::geography,
        ST_SetSRID(ST_MakePoint(center_lng, center_lat), 4326)::geography,
        radius_meters
      )
  ),
  area_sources AS (
    SELECT DISTINCT s.id
    FROM area_events ae
    JOIN event_sources es ON ae.id = es.event_id
    JOIN sources s ON es.source_id = s.id
  ),
  area_orgs AS (
    SELECT DISTINCT o.id
    FROM area_events ae
    JOIN event_organizations eo ON ae.id = eo.event_id
    JOIN organizations o ON eo.organization_id = o.id
  ),
  area_protected AS (
    SELECT id
    FROM protected_areas
    WHERE ST_DWithin(
      COALESCE(geom_boundary, geom_centroid)::geography,
      ST_SetSRID(ST_MakePoint(center_lng, center_lat), 4326)::geography,
      radius_meters
    )
  ),
  severity_dist AS (
    SELECT jsonb_object_agg(severity_level::text, cnt) as dist
    FROM (
      SELECT severity_level, COUNT(*) as cnt
      FROM area_events
      GROUP BY severity_level
    ) s
  ),
  type_dist AS (
    SELECT jsonb_object_agg(event_type_primary, cnt) as dist
    FROM (
      SELECT event_type_primary, COUNT(*) as cnt
      FROM area_events
      GROUP BY event_type_primary
    ) t
  )
  SELECT 
    (SELECT COUNT(*) FROM area_events)::BIGINT as events_count,
    (SELECT COUNT(*) FROM area_sources)::BIGINT as sources_count,
    (SELECT COUNT(*) FROM area_orgs)::BIGINT as organizations_count,
    (SELECT COUNT(*) FROM area_protected)::BIGINT as protected_areas_count,
    COALESCE((SELECT dist FROM severity_dist), '{}'::jsonb) as severity_distribution,
    COALESCE((SELECT dist FROM type_dist), '{}'::jsonb) as type_distribution,
    LEAST(1.0, (SELECT COUNT(*) FROM area_sources)::DOUBLE PRECISION / 50.0 * 0.6 + 
               (SELECT COUNT(*) FROM area_events)::DOUBLE PRECISION / 20.0 * 0.4) as coverage_score;
END;
$$ LANGUAGE plpgsql;

-- ============================================
-- HELPFUL FUNCTIONS
-- ============================================

-- Find events within radius of a point
CREATE OR REPLACE FUNCTION events_within_radius(
  center_lat DOUBLE PRECISION,
  center_lng DOUBLE PRECISION,
  radius_km DOUBLE PRECISION
)
RETURNS TABLE (
  event_id BIGINT,
  title TEXT,
  distance_km DOUBLE PRECISION
) AS $$
BEGIN
  RETURN QUERY
  SELECT 
    e.id,
    e.title,
    ST_Distance(
      e.geom_point::geography,
      ST_SetSRID(ST_MakePoint(center_lng, center_lat), 4326)::geography
    ) / 1000.0 as distance_km
  FROM events e
  WHERE e.status = 'active'
    AND e.geom_point IS NOT NULL
    AND ST_DWithin(
      e.geom_point::geography,
      ST_SetSRID(ST_MakePoint(center_lng, center_lat), 4326)::geography,
      radius_km * 1000
    )
  ORDER BY distance_km;
END;
$$ LANGUAGE plpgsql;

-- Check if event is inside any protected area
CREATE OR REPLACE FUNCTION check_protected_area_overlap(event_id BIGINT)
RETURNS TABLE (
  protected_area_id BIGINT,
  protected_area_name TEXT,
  is_inside BOOLEAN,
  distance_km DOUBLE PRECISION
) AS $$
BEGIN
  RETURN QUERY
  WITH event_point AS (
    SELECT geom_point FROM events WHERE id = event_id
  )
  SELECT 
    pa.id,
    pa.name,
    ST_Contains(pa.geom_boundary, ep.geom_point) as is_inside,
    CASE 
      WHEN ST_Contains(pa.geom_boundary, ep.geom_point) THEN 0
      ELSE ST_Distance(pa.geom_boundary::geography, ep.geom_point::geography) / 1000.0
    END as distance_km
  FROM protected_areas pa, event_point ep
  WHERE ST_DWithin(pa.geom_boundary::geography, ep.geom_point::geography, 100000) -- 100km
  ORDER BY distance_km
  LIMIT 10;
END;
$$ LANGUAGE plpgsql;

COMMENT ON TABLE events IS 'Canonical conservation event records';
COMMENT ON TABLE sources IS 'Raw ingested content from RSS feeds and web scraping';
COMMENT ON TABLE source_extractions IS 'LLM extraction outputs linked to events';
COMMENT ON TABLE classroom_episodes IS 'Generated educational video episodes';
