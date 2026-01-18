// packages/api/src/routes/placeBrief.route.ts
//
// Production-ready Place Brief API using raw pg + PostGIS
// Returns the exact place_brief_v1 response structure

import type { Request, Response, Router } from "express";
import type { Pool } from "pg";
import { logger } from "../utils/logger";

// ============================================
// Types
// ============================================

type PlaceBriefDeps = {
  pg: Pool;
};

// ============================================
// Utility Functions
// ============================================

function clamp(n: number, min: number, max: number) {
  return Math.max(min, Math.min(max, n));
}

function toInt(v: any, def: number) {
  const n = Number(v);
  return Number.isFinite(n) ? Math.trunc(n) : def;
}

function toFloat(v: any, def: number) {
  const n = Number(v);
  return Number.isFinite(n) ? n : def;
}

function makeCursor(limit: number, offset: number) {
  const next = { limit, offset };
  return Buffer.from(JSON.stringify(next)).toString("base64url");
}

function parseCursor(cursor?: string | null): { limit: number; offset: number } | null {
  if (!cursor) return null;
  try {
    const raw = Buffer.from(cursor, "base64url").toString("utf8");
    const parsed = JSON.parse(raw);
    if (typeof parsed?.limit === "number" && typeof parsed?.offset === "number") return parsed;
    return null;
  } catch {
    return null;
  }
}

function groupBy<T>(arr: T[], keyFn: (x: T) => string): Record<string, T[]> {
  const out: Record<string, T[]> = {};
  for (const item of arr) {
    const k = keyFn(item);
    (out[k] ||= []).push(item);
  }
  return out;
}

function topCounts(values: string[], topN: number): Array<[string, number]> {
  const m = new Map<string, number>();
  for (const v of values) m.set(v, (m.get(v) ?? 0) + 1);
  return [...m.entries()].sort((a, b) => b[1] - a[1]).slice(0, topN);
}

// ============================================
// SQL Queries
// ============================================

const SQL = {
  // Protected areas that CONTAIN the clicked point
  protectedContains: `
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
  `,

  // Protected areas NEAR the clicked point (but not containing it)
  protectedNearby: `
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
  `,

  // Events within radius + time window (paginated)
  events: `
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
  `,

  // Sources for a set of events
  eventSources: `
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
  `,

  // Organizations for a set of events
  eventOrgs: `
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
  `,

  // Aggregated organizations with full metrics (roles, event types, top events as JSON)
  orgCards: `
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
  `,

  // Sources backing each org's activity in the area
  orgSources: `
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
  `,

  // Activity summary per org (template-based, no LLM)
  orgSummaries: `
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
  `,

  // Coverage statistics
  coverageStats: `
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
  `,

  // Protected area name lookup
  protectedAreaName: `
    SELECT id, name FROM protected_areas WHERE id = ANY($1::bigint[]);
  `,

  // Insert coverage request
  insertCoverageRequest: `
    INSERT INTO coverage_requests (latitude, longitude, radius_km, country, admin1, user_id, user_email, notes)
    VALUES ($1, $2, $3, $4, $5, $6::uuid, $7, $8)
    RETURNING id;
  `,
};

// ============================================
// Route Handler
// ============================================

export function registerPlaceBriefRoute(router: Router, deps: PlaceBriefDeps) {
  const { pg } = deps;

  /**
   * GET /place-brief
   * Main endpoint for "click anywhere on map" feature
   */
  router.get("/place-brief", async (req: Request, res: Response) => {
    const lat = toFloat(req.query.lat, NaN);
    const lng = toFloat(req.query.lng, NaN);

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ 
        error: "lat and lng are required and must be numbers",
        example: "/place-brief?lat=39.51&lng=-84.73"
      });
    }

    if (lat < -90 || lat > 90 || lng < -180 || lng > 180) {
      return res.status(400).json({
        error: "Invalid coordinates",
        details: "lat must be -90 to 90, lng must be -180 to 180"
      });
    }

    const radius_km = clamp(toInt(req.query.radius_km, 50), 1, 500);
    const time_window_days = clamp(toInt(req.query.time_window_days, 30), 1, 365);

    // Pagination
    const cursor = typeof req.query.cursor === "string" ? req.query.cursor : null;
    const cursorParsed = parseCursor(cursor);
    const limit = clamp(toInt(req.query.limit, cursorParsed?.limit ?? 20), 1, 50);
    const offset = clamp(toInt(req.query.offset, cursorParsed?.offset ?? 0), 0, 5000);

    const radius_m = Math.trunc(radius_km * 1000);
    const as_of = new Date().toISOString();

    // Org query limits
    const ORG_LIMIT = 10;
    const ORG_SOURCES_LIMIT = 5;

    logger.info({ lat, lng, radius_km, time_window_days, limit, offset }, "Place brief request");

    try {
      // Run main queries in parallel
      const [containsPA, nearbyPA, eventsResp, orgCardsResp, coverageResp] = await Promise.all([
        pg.query(SQL.protectedContains, [lng, lat]),
        pg.query(SQL.protectedNearby, [lng, lat, radius_m]),
        pg.query(SQL.events, [lng, lat, radius_m, time_window_days, limit, offset]),
        pg.query(SQL.orgCards, [lng, lat, radius_m, time_window_days, ORG_LIMIT]),
        pg.query(SQL.coverageStats, [lng, lat, radius_m, time_window_days]),
      ]);

      const eventsRows = eventsResp.rows as any[];
      const eventIds = eventsRows.map((r) => Number(r.event_id));
      const totalEvents = eventsRows[0]?.total_count ? parseInt(eventsRows[0].total_count) : eventsRows.length;

      // Get org IDs for follow-up queries
      const orgIds = orgCardsResp.rows.map((r: any) => Number(r.organization_id));

      // Fetch sources and orgs for events, plus org sources and summaries
      let sourcesByEvent: Record<string, any[]> = {};
      let orgsByEvent: Record<string, any[]> = {};
      let paNames: Record<string, string> = {};
      let orgSourcesByOrg: Record<string, any[]> = {};
      let orgSummaryByOrg: Record<string, string> = {};

      // Event-related queries
      if (eventIds.length) {
        const [srcResp, orgResp] = await Promise.all([
          pg.query(SQL.eventSources, [eventIds]),
          pg.query(SQL.eventOrgs, [eventIds]),
        ]);

        sourcesByEvent = groupBy(srcResp.rows, (x: any) => String(x.event_id));
        orgsByEvent = groupBy(orgResp.rows, (x: any) => String(x.event_id));

        // Get protected area names for events
        const paIds = eventsRows
          .map(e => e.nearest_protected_area_id)
          .filter(Boolean)
          .map(Number);
        
        if (paIds.length) {
          const paResp = await pg.query(SQL.protectedAreaName, [paIds]);
          for (const row of paResp.rows) {
            paNames[row.id] = row.name;
          }
        }
      }

      // Org-related queries (sources and summaries)
      if (orgIds.length) {
        const [orgSourcesResp, orgSummaryResp] = await Promise.all([
          pg.query(SQL.orgSources, [lng, lat, radius_m, time_window_days, orgIds, ORG_SOURCES_LIMIT]),
          pg.query(SQL.orgSummaries, [lng, lat, radius_m, time_window_days, orgIds]),
        ]);

        for (const r of orgSourcesResp.rows as any[]) {
          orgSourcesByOrg[String(r.organization_id)] = r.sources ?? [];
        }

        for (const r of orgSummaryResp.rows as any[]) {
          orgSummaryByOrg[String(r.organization_id)] = r.activity_summary ?? "";
        }
      }

      // Build summary aggregates
      const topThreatTypes = topCounts(
        eventsRows.map((e) => e.event_type_primary), 
        5
      ).map(([event_type, count]) => ({ event_type, count }));

      const severityBreakdown = topCounts(
        eventsRows.map((e) => String(e.severity_level)), 
        5
      ).map(([sev, count]) => ({ severity: Number(sev), count }));

      // Coverage score
      const sourcesCount = parseInt(coverageResp.rows[0]?.sources_count) || 0;
      const coverage_score = clamp(
        (sourcesCount / 50) * 0.6 + (totalEvents / 20) * 0.4,
        0,
        1
      );
      const is_low_coverage = sourcesCount < 10 || totalEvents === 0;

      // Build response
      const response = {
        schema_version: "place_brief_v1",
        request: {
          lat,
          lng,
          radius_km,
          time_window_days,
          as_of,
        },
        place: {
          display_name: `Near ${lat.toFixed(4)}, ${lng.toFixed(4)}`,
          admin: {
            country: eventsRows[0]?.country || null,
            admin1: eventsRows[0]?.admin1 || null,
            admin2: eventsRows[0]?.admin2 || null,
            locality: null,
          },
          geocoding: null, // TODO: Add reverse geocoding
        },
        coverage: {
          coverage_score: Math.round(coverage_score * 100) / 100,
          notes: "Coverage is based on public sources ingested in the selected time window.",
          sources_considered: sourcesCount,
          events_found: totalEvents,
          organizations_found: orgRollupResp.rows[0]?.total_orgs 
            ? parseInt(orgRollupResp.rows[0].total_orgs) 
            : orgRollupResp.rows.length,
          is_low_coverage,
        },
        summary: {
          top_threat_types: topThreatTypes,
          severity_breakdown: severityBreakdown,
          protected_areas_nearby_count: containsPA.rows.length + nearbyPA.rows.length,
          trend: "unknown" as const,
        },
        protected_areas: {
          contains_point: containsPA.rows.map((r: any) => ({
            protected_area_id: Number(r.protected_area_id),
            name: r.name,
            designation: r.designation,
            iucn_category: r.iucn_category,
            country: r.country,
            external_id: r.external_id ? `WDPA:${r.external_id}` : null,
            source: r.source,
            relation: r.relation,
            distance_km: 0,
            centroid: { lat: Number(r.centroid_lat), lng: Number(r.centroid_lng) },
            area_km2: r.area_km2 ? Number(r.area_km2) : undefined,
          })),
          nearby: nearbyPA.rows.map((r: any) => ({
            protected_area_id: Number(r.protected_area_id),
            name: r.name,
            designation: r.designation,
            iucn_category: r.iucn_category,
            country: r.country,
            external_id: r.external_id ? `WDPA:${r.external_id}` : null,
            source: r.source,
            relation: r.relation,
            distance_km: Math.round(Number(r.distance_km) * 10) / 10,
            centroid: { lat: Number(r.centroid_lat), lng: Number(r.centroid_lng) },
            area_km2: r.area_km2 ? Number(r.area_km2) : undefined,
          })),
        },
        events: {
          items: eventsRows.map((e: any) => {
            const evSources = sourcesByEvent[String(e.event_id)] ?? [];
            const evOrgs = orgsByEvent[String(e.event_id)] ?? [];
            const paName = e.nearest_protected_area_id ? paNames[e.nearest_protected_area_id] : null;

            return {
              event_id: Number(e.event_id),
              title: e.title,
              event_type_primary: e.event_type_primary,
              event_type_secondary: e.event_type_secondary,
              status: e.status,
              severity_level: Number(e.severity_level),
              confidence: {
                extraction: Number(e.confidence_extraction) || 0.5,
                geolocation: Number(e.confidence_geolocation) || 0.5,
              },
              time: {
                reported_at: e.created_at ? new Date(e.created_at).toISOString() : null,
                event_start: e.event_start ? new Date(e.event_start).toISOString() : null,
                event_end: e.event_end ? new Date(e.event_end).toISOString() : null,
                is_ongoing: e.is_ongoing ?? true,
              },
              location: {
                name: e.location_name || `${e.admin1 || ''}, ${e.country || ''}`.trim(),
                coordinates: { lat: Number(e.lat), lng: Number(e.lng) },
                geocoding_precision: "approximate" as const,
                distance_km: Math.round(Number(e.distance_km) * 10) / 10,
              },
              summary_short: e.summary_short || "",
              protected_area_context: e.nearest_protected_area_id ? [{
                protected_area_id: Number(e.nearest_protected_area_id),
                name: paName || "Unknown",
                relation: e.is_inside_protected_area ? "inside" as const : "near" as const,
                distance_km: Number(e.distance_to_protected_km) || 0,
              }] : [],
              organizations: evOrgs.slice(0, 5).map((o: any) => ({
                organization_id: Number(o.organization_id),
                name: o.name,
                org_type: o.org_type || "unknown",
                role: o.role || "mentioned",
                involvement_confidence: Number(o.involvement_confidence) || 0.7,
              })),
              sources: evSources.slice(0, 3).map((s: any) => ({
                source_id: Number(s.source_id),
                url: s.url,
                publisher: s.publisher,
                published_at: s.published_at ? new Date(s.published_at).toISOString() : null,
                source_type: s.source_type || "news",
              })),
              briefing_video: {
                available: false,
                video_url: undefined,
                runtime_seconds: undefined,
              },
            };
          }),
          total_count: totalEvents,
          next_cursor: eventsRows.length === limit ? makeCursor(limit, offset + limit) : null,
        },
        organizations: {
          items: orgCardsResp.rows.map((o: any) => {
            const sources = orgSourcesByOrg[String(o.organization_id)] ?? [];
            const summary = orgSummaryByOrg[String(o.organization_id)] ?? "";

            return {
              organization_id: Number(o.organization_id),
              name: o.name,
              org_type: o.org_type || "unknown",
              website: o.website || null,
              logo_url: o.logo_url || null,
              activity_summary: summary,
              activity_window_days: time_window_days,
              in_area_metrics: {
                events_count: Number(o.events_count) || 0,
                most_common_roles: (o.most_common_roles || []).map((x: any) => ({
                  role: x.role,
                  count: Number(x.count),
                })),
                top_event_types: (o.top_event_types || []).map((x: any) => ({
                  event_type: x.event_type,
                  count: Number(x.count),
                })),
              },
              top_related_events: (o.top_related_events || []).map((e: any) => ({
                event_id: Number(e.event_id),
                title: e.title,
                severity_level: Number(e.severity_level),
              })),
              sources: sources.map((s: any) => ({
                source_id: Number(s.source_id),
                url: s.url,
                publisher: s.publisher,
                published_at: s.published_at ? new Date(s.published_at).toISOString() : null,
                source_type: s.source_type || "news",
              })),
            };
          }),
          total_count: orgCardsResp.rows[0]?.total_orgs 
            ? parseInt(orgCardsResp.rows[0].total_orgs) 
            : orgCardsResp.rows.length,
          next_cursor: null,
        },
        actions: {
          subscribe: {
            supported: true,
            default_radius_km: radius_km,
            suggested_filters: {
              min_severity: 3,
              event_types: topThreatTypes.slice(0, 3).map((x) => x.event_type),
            },
          },
          export: {
            supported: true,
            formats: ["csv", "json", "pdf"],
          },
          request_coverage: is_low_coverage ? {
            supported: true,
            reason: "Limited public sources found for this area.",
          } : undefined,
        },
        disclaimer: {
          ai_generated_summaries: true,
          text: "This place brief is generated from public sources and may be incomplete. Use the source links for verification.",
        },
      };

      logger.info({ 
        lat, lng, 
        events: totalEvents, 
        orgs: response.organizations.total_count,
        coverage: coverage_score 
      }, "Place brief generated");

      return res.json(response);
    } catch (err: any) {
      logger.error({ error: err.message, lat, lng }, "Place brief failed");
      return res.status(500).json({ 
        error: "place-brief failed", 
        details: err?.message ?? String(err) 
      });
    }
  });

  /**
   * POST /place-brief/coverage-request
   * Request better coverage for an area
   */
  router.post("/place-brief/coverage-request", async (req: Request, res: Response) => {
    const { lat, lng, radius_km = 50, email, notes } = req.body;

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ error: "lat and lng are required" });
    }

    try {
      const userId = (req as any).user?.id || null;
      
      const result = await pg.query(SQL.insertCoverageRequest, [
        lat,
        lng,
        radius_km,
        null, // country - could reverse geocode
        null, // admin1
        userId,
        email || null,
        notes || null,
      ]);

      logger.info({ lat, lng, radius_km, userId }, "Coverage request submitted");

      return res.status(201).json({
        success: true,
        request_id: result.rows[0].id,
        message: "Coverage request submitted. We will review and add sources for this area.",
      });
    } catch (err: any) {
      logger.error({ error: err.message }, "Coverage request failed");
      return res.status(500).json({ error: "Failed to submit coverage request" });
    }
  });

  /**
   * GET /place-brief/export
   * Export place brief as CSV
   */
  router.get("/place-brief/export", async (req: Request, res: Response) => {
    const lat = toFloat(req.query.lat, NaN);
    const lng = toFloat(req.query.lng, NaN);
    const format = (req.query.format as string) || "json";

    if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
      return res.status(400).json({ error: "lat and lng are required" });
    }

    const radius_km = clamp(toInt(req.query.radius_km, 50), 1, 500);
    const time_window_days = clamp(toInt(req.query.time_window_days, 30), 1, 365);
    const radius_m = Math.trunc(radius_km * 1000);

    try {
      // Fetch more events for export
      const eventsResp = await pg.query(SQL.events, [
        lng, lat, radius_m, time_window_days, 200, 0
      ]);

      if (format === "csv") {
        const headers = [
          "event_id", "title", "event_type", "severity", "confidence",
          "event_start", "location", "country", "distance_km"
        ];
        
        const rows = eventsResp.rows.map((e: any) => [
          e.event_id,
          `"${(e.title || "").replace(/"/g, '""')}"`,
          e.event_type_primary,
          e.severity_level,
          e.confidence_extraction,
          e.event_start || "",
          `"${(e.location_name || "").replace(/"/g, '""')}"`,
          e.country,
          Number(e.distance_km).toFixed(1),
        ]);

        const csv = [headers.join(","), ...rows.map((r: any[]) => r.join(","))].join("\n");

        res.setHeader("Content-Type", "text/csv");
        res.setHeader("Content-Disposition", `attachment; filename="place-brief-${lat}-${lng}.csv"`);
        return res.send(csv);
      }

      // JSON export
      res.setHeader("Content-Type", "application/json");
      res.setHeader("Content-Disposition", `attachment; filename="place-brief-${lat}-${lng}.json"`);
      return res.json({
        exported_at: new Date().toISOString(),
        location: { lat, lng },
        radius_km,
        time_window_days,
        events: eventsResp.rows,
      });
    } catch (err: any) {
      return res.status(500).json({ error: "Export failed", details: err.message });
    }
  });

  return router;
}

export default registerPlaceBriefRoute;
