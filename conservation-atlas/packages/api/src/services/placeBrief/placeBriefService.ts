// packages/api/src/services/placeBrief/placeBriefService.ts
//
// Service for the "Click anywhere on map" Place Brief feature
// Returns events, organizations, and protected areas near a clicked point

import { prisma } from '../../db';
import { logger } from '../../utils/logger';
import type {
  PlaceBriefResponse,
  PlaceBriefQueryParams,
  PlaceInfo,
  CoverageInfo,
  PlaceSummary,
  ProtectedAreasResult,
  EventBrief,
  OrganizationBrief,
} from '@conservation-atlas/shared/types/placeBrief';

// ============================================
// Configuration
// ============================================

const DEFAULT_RADIUS_KM = 50;
const DEFAULT_TIME_WINDOW_DAYS = 30;
const MAX_EVENTS = 20;
const MAX_ORGS = 10;
const MAX_PROTECTED_AREAS = 10;

// ============================================
// Main Service Function
// ============================================

export async function getPlaceBrief(params: PlaceBriefQueryParams): Promise<PlaceBriefResponse> {
  const {
    lat,
    lng,
    radiusKm = DEFAULT_RADIUS_KM,
    timeWindowDays = DEFAULT_TIME_WINDOW_DAYS,
    limit = MAX_EVENTS,
  } = params;

  const radiusMeters = radiusKm * 1000;
  const timeWindowStart = new Date();
  timeWindowStart.setDate(timeWindowStart.getDate() - timeWindowDays);

  logger.info({ lat, lng, radiusKm, timeWindowDays }, 'Generating place brief');

  // Run queries in parallel for performance
  const [
    placeInfo,
    eventsResult,
    protectedAreasResult,
    coverageStats,
  ] = await Promise.all([
    reverseGeocode(lat, lng),
    getEventsNearPoint(lat, lng, radiusMeters, timeWindowStart, limit),
    getProtectedAreasNearPoint(lat, lng, radiusMeters),
    getCoverageStats(lat, lng, radiusMeters, timeWindowStart),
  ]);

  // Get organizations from the events
  const eventIds = eventsResult.events.map(e => e.event_id);
  const orgsResult = eventIds.length > 0
    ? await getOrganizationsForEvents(eventIds, timeWindowStart)
    : { organizations: [], total: 0 };

  // Build summary from results
  const summary = buildSummary(eventsResult.events, protectedAreasResult);

  // Build coverage info
  const coverage: CoverageInfo = {
    coverage_score: calculateCoverageScore(coverageStats),
    notes: 'Coverage is based on public sources ingested in the last 30 days.',
    sources_considered: coverageStats.sourceCount,
    events_found: eventsResult.total,
    organizations_found: orgsResult.total,
    is_low_coverage: coverageStats.sourceCount < 10 || eventsResult.total === 0,
  };

  // Build response
  const response: PlaceBriefResponse = {
    schema_version: 'place_brief_v1',
    
    request: {
      lat,
      lng,
      radius_km: radiusKm,
      time_window_days: timeWindowDays,
      as_of: new Date().toISOString(),
    },
    
    place: placeInfo,
    coverage,
    summary,
    
    protected_areas: protectedAreasResult,
    
    events: {
      items: eventsResult.events,
      total_count: eventsResult.total,
      next_cursor: eventsResult.events.length >= limit ? 'cursor_placeholder' : null,
    },
    
    organizations: {
      items: orgsResult.organizations,
      total_count: orgsResult.total,
      next_cursor: null,
    },
    
    actions: {
      subscribe: {
        supported: true,
        default_radius_km: radiusKm,
        suggested_filters: {
          min_severity: 3,
          event_types: summary.top_threat_types.slice(0, 3).map(t => t.event_type),
        },
      },
      export: {
        supported: true,
        formats: ['csv', 'json', 'pdf'],
      },
      request_coverage: coverage.is_low_coverage ? {
        supported: true,
        reason: 'Limited public sources found for this area.',
      } : undefined,
    },
    
    disclaimer: {
      ai_generated_summaries: true,
      text: 'This place brief is generated from public sources and may be incomplete. Use the source links for verification.',
    },
  };

  logger.info({
    lat, lng,
    events_count: eventsResult.total,
    orgs_count: orgsResult.total,
    coverage_score: coverage.coverage_score,
  }, 'Place brief generated');

  return response;
}

// ============================================
// Spatial Queries
// ============================================

async function getEventsNearPoint(
  lat: number,
  lng: number,
  radiusMeters: number,
  timeWindowStart: Date,
  limit: number
): Promise<{ events: EventBrief[]; total: number }> {
  
  // Use raw SQL for PostGIS spatial query
  const events = await prisma.$queryRaw<any[]>`
    WITH nearby_events AS (
      SELECT 
        e.id as event_id,
        e.title,
        e.event_type_primary,
        e.event_type_secondary,
        e.status,
        e.severity_level,
        e.confidence_extraction,
        e.confidence_geolocation,
        e.summary_short,
        e.location_name,
        ST_X(e.geom_point) as lng,
        ST_Y(e.geom_point) as lat,
        e.event_start,
        e.event_end,
        e.is_ongoing,
        e.created_at,
        e.nearest_protected_area_id,
        e.is_inside_protected_area,
        e.distance_to_protected_km,
        ST_Distance(
          e.geom_point::geography,
          ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography
        ) / 1000.0 as distance_km
      FROM events e
      WHERE e.status = 'active'
        AND e.geom_point IS NOT NULL
        AND e.created_at >= ${timeWindowStart}
        AND ST_DWithin(
          e.geom_point::geography,
          ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
          ${radiusMeters}
        )
      ORDER BY e.severity_level DESC, e.created_at DESC
      LIMIT ${limit}
    )
    SELECT 
      ne.*,
      pa.name as protected_area_name,
      COUNT(*) OVER() as total_count
    FROM nearby_events ne
    LEFT JOIN protected_areas pa ON ne.nearest_protected_area_id = pa.id
  `;

  if (events.length === 0) {
    return { events: [], total: 0 };
  }

  const total = events[0]?.total_count ? parseInt(events[0].total_count) : events.length;

  // Get sources and organizations for each event
  const eventIds = events.map(e => e.event_id);
  
  const [sources, eventOrgs] = await Promise.all([
    getSourcesForEvents(eventIds),
    getEventOrganizations(eventIds),
  ]);

  // Map to EventBrief format
  const eventBriefs: EventBrief[] = events.map(e => ({
    event_id: parseInt(e.event_id),
    title: e.title,
    event_type_primary: e.event_type_primary,
    event_type_secondary: e.event_type_secondary,
    status: e.status,
    severity_level: e.severity_level,
    
    confidence: {
      extraction: parseFloat(e.confidence_extraction) || 0.5,
      geolocation: parseFloat(e.confidence_geolocation) || 0.5,
    },
    
    time: {
      reported_at: e.created_at?.toISOString() ?? null,
      event_start: e.event_start?.toISOString() ?? null,
      event_end: e.event_end?.toISOString() ?? null,
      is_ongoing: e.is_ongoing ?? true,
    },
    
    location: {
      name: e.location_name || 'Unknown location',
      coordinates: e.lat && e.lng ? { lat: parseFloat(e.lat), lng: parseFloat(e.lng) } : null,
      geocoding_precision: 'approximate',
      distance_km: parseFloat(e.distance_km) || 0,
    },
    
    summary_short: e.summary_short || '',
    
    protected_area_context: e.nearest_protected_area_id ? [{
      protected_area_id: parseInt(e.nearest_protected_area_id),
      name: e.protected_area_name || 'Unknown',
      relation: e.is_inside_protected_area ? 'inside' : 'near',
      distance_km: parseFloat(e.distance_to_protected_km) || 0,
    }] : [],
    
    organizations: eventOrgs
      .filter(eo => eo.event_id === parseInt(e.event_id))
      .map(eo => ({
        organization_id: eo.organization_id,
        name: eo.org_name,
        org_type: eo.org_type,
        role: eo.role,
        involvement_confidence: parseFloat(eo.involvement_confidence) || 0.7,
      })),
    
    sources: sources
      .filter(s => s.event_id === parseInt(e.event_id))
      .slice(0, 3) // Limit to top 3 sources per event
      .map(s => ({
        source_id: s.source_id,
        url: s.url,
        publisher: s.publisher,
        published_at: s.published_at?.toISOString() ?? null,
        source_type: s.source_type || 'news',
      })),
  }));

  return { events: eventBriefs, total };
}

async function getProtectedAreasNearPoint(
  lat: number,
  lng: number,
  radiusMeters: number
): Promise<ProtectedAreasResult> {
  
  // Query for protected areas containing the point
  const containingAreas = await prisma.$queryRaw<any[]>`
    SELECT 
      id as protected_area_id,
      name,
      designation,
      iucn_category,
      country,
      wdpa_id,
      area_km2,
      ST_Y(geom_centroid) as centroid_lat,
      ST_X(geom_centroid) as centroid_lng,
      0 as distance_km
    FROM protected_areas
    WHERE geom_boundary IS NOT NULL
      AND ST_Contains(
        geom_boundary,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)
      )
    LIMIT 5
  `;

  // Query for nearby protected areas (not containing)
  const nearbyAreas = await prisma.$queryRaw<any[]>`
    SELECT 
      id as protected_area_id,
      name,
      designation,
      iucn_category,
      country,
      wdpa_id,
      area_km2,
      ST_Y(geom_centroid) as centroid_lat,
      ST_X(geom_centroid) as centroid_lng,
      ST_Distance(
        COALESCE(geom_boundary, geom_centroid)::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography
      ) / 1000.0 as distance_km
    FROM protected_areas
    WHERE (geom_boundary IS NOT NULL OR geom_centroid IS NOT NULL)
      AND NOT ST_Contains(
        COALESCE(geom_boundary, ST_Buffer(geom_centroid, 0.001)),
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)
      )
      AND ST_DWithin(
        COALESCE(geom_boundary, geom_centroid)::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
        ${radiusMeters}
      )
    ORDER BY distance_km
    LIMIT ${MAX_PROTECTED_AREAS}
  `;

  const mapToResult = (area: any, relation: 'inside' | 'near') => ({
    protected_area_id: parseInt(area.protected_area_id),
    name: area.name,
    designation: area.designation,
    iucn_category: area.iucn_category,
    country: area.country,
    external_id: area.wdpa_id ? `WDPA:${area.wdpa_id}` : null,
    source: 'wdpa' as const,
    relation,
    distance_km: parseFloat(area.distance_km) || 0,
    centroid: {
      lat: parseFloat(area.centroid_lat) || lat,
      lng: parseFloat(area.centroid_lng) || lng,
    },
    area_km2: area.area_km2 ? parseFloat(area.area_km2) : undefined,
  });

  return {
    contains_point: containingAreas.map(a => mapToResult(a, 'inside')),
    nearby: nearbyAreas.map(a => mapToResult(a, 'near')),
  };
}

async function getOrganizationsForEvents(
  eventIds: number[],
  timeWindowStart: Date
): Promise<{ organizations: OrganizationBrief[]; total: number }> {
  
  if (eventIds.length === 0) {
    return { organizations: [], total: 0 };
  }

  // Get organizations with their activity metrics
  const orgs = await prisma.$queryRaw<any[]>`
    WITH org_metrics AS (
      SELECT 
        o.id as organization_id,
        o.name,
        o.org_type,
        o.website,
        o.logo_url,
        o.description,
        COUNT(DISTINCT eo.event_id) as events_count,
        array_agg(DISTINCT eo.role) as roles,
        array_agg(DISTINCT e.event_type_primary) as event_types
      FROM organizations o
      JOIN event_organizations eo ON o.id = eo.organization_id
      JOIN events e ON eo.event_id = e.id
      WHERE eo.event_id = ANY(${eventIds}::bigint[])
        AND e.created_at >= ${timeWindowStart}
      GROUP BY o.id
      ORDER BY events_count DESC
      LIMIT ${MAX_ORGS}
    )
    SELECT 
      om.*,
      COUNT(*) OVER() as total_count
    FROM org_metrics om
  `;

  if (orgs.length === 0) {
    return { organizations: [], total: 0 };
  }

  const total = orgs[0]?.total_count ? parseInt(orgs[0].total_count) : orgs.length;

  // Get top events for each org
  const orgIds = orgs.map(o => o.organization_id);
  const topEvents = await prisma.$queryRaw<any[]>`
    SELECT DISTINCT ON (eo.organization_id)
      eo.organization_id,
      e.id as event_id,
      e.title,
      e.severity_level
    FROM event_organizations eo
    JOIN events e ON eo.event_id = e.id
    WHERE eo.organization_id = ANY(${orgIds}::bigint[])
      AND e.id = ANY(${eventIds}::bigint[])
    ORDER BY eo.organization_id, e.severity_level DESC, e.created_at DESC
  `;

  // Map to OrganizationBrief
  const organizationBriefs: OrganizationBrief[] = orgs.map(o => {
    const roles = (o.roles || []).filter((r: string) => r);
    const eventTypes = (o.event_types || []).filter((t: string) => t);
    const orgTopEvents = topEvents.filter(te => te.organization_id === o.organization_id);

    return {
      organization_id: parseInt(o.organization_id),
      name: o.name,
      org_type: o.org_type,
      website: o.website,
      logo_url: o.logo_url,
      
      activity_summary: o.description || null,
      activity_window_days: 30,
      
      in_area_metrics: {
        events_count: parseInt(o.events_count) || 0,
        most_common_roles: roles.slice(0, 3).map((role: string) => ({
          role,
          count: 1, // Simplified; could aggregate properly
        })),
        top_event_types: eventTypes.slice(0, 3).map((event_type: string) => ({
          event_type,
          count: 1,
        })),
      },
      
      top_related_events: orgTopEvents.map(e => ({
        event_id: parseInt(e.event_id),
        title: e.title,
        severity_level: e.severity_level,
      })),
      
      sources: [], // Could populate from event sources
    };
  });

  return { organizations: organizationBriefs, total };
}

// ============================================
// Helper Queries
// ============================================

async function getSourcesForEvents(eventIds: number[]): Promise<any[]> {
  if (eventIds.length === 0) return [];
  
  return prisma.$queryRaw<any[]>`
    SELECT 
      es.event_id,
      s.id as source_id,
      s.url,
      s.publisher,
      s.published_at,
      s.source_type
    FROM event_sources es
    JOIN sources s ON es.source_id = s.id
    WHERE es.event_id = ANY(${eventIds}::bigint[])
    ORDER BY es.event_id, s.published_at DESC
  `;
}

async function getEventOrganizations(eventIds: number[]): Promise<any[]> {
  if (eventIds.length === 0) return [];
  
  return prisma.$queryRaw<any[]>`
    SELECT 
      eo.event_id,
      eo.organization_id,
      eo.role,
      eo.involvement_confidence,
      o.name as org_name,
      o.org_type
    FROM event_organizations eo
    JOIN organizations o ON eo.organization_id = o.id
    WHERE eo.event_id = ANY(${eventIds}::bigint[])
  `;
}

async function getCoverageStats(
  lat: number,
  lng: number,
  radiusMeters: number,
  timeWindowStart: Date
): Promise<{ sourceCount: number; eventCount: number }> {
  
  const result = await prisma.$queryRaw<any[]>`
    SELECT 
      COUNT(DISTINCT s.id) as source_count,
      COUNT(DISTINCT e.id) as event_count
    FROM events e
    LEFT JOIN event_sources es ON e.id = es.event_id
    LEFT JOIN sources s ON es.source_id = s.id
    WHERE e.status = 'active'
      AND e.geom_point IS NOT NULL
      AND e.created_at >= ${timeWindowStart}
      AND ST_DWithin(
        e.geom_point::geography,
        ST_SetSRID(ST_MakePoint(${lng}, ${lat}), 4326)::geography,
        ${radiusMeters}
      )
  `;

  return {
    sourceCount: parseInt(result[0]?.source_count) || 0,
    eventCount: parseInt(result[0]?.event_count) || 0,
  };
}

// ============================================
// Utility Functions
// ============================================

async function reverseGeocode(lat: number, lng: number): Promise<PlaceInfo> {
  // For now, return a basic place info
  // In production, call Google Maps or Mapbox Geocoding API
  
  // TODO: Integrate actual geocoding service
  return {
    display_name: `Location at ${lat.toFixed(4)}, ${lng.toFixed(4)}`,
    admin: {
      country: 'Unknown',
      admin1: null,
      admin2: null,
      locality: null,
    },
    geocoding: null,
  };
}

function buildSummary(events: EventBrief[], protectedAreas: ProtectedAreasResult): PlaceSummary {
  // Count event types
  const typeCounts = new Map<string, number>();
  const severityCounts = new Map<number, number>();
  
  for (const event of events) {
    const type = event.event_type_primary;
    typeCounts.set(type, (typeCounts.get(type) || 0) + 1);
    
    const severity = event.severity_level;
    severityCounts.set(severity, (severityCounts.get(severity) || 0) + 1);
  }

  return {
    top_threat_types: Array.from(typeCounts.entries())
      .sort((a, b) => b[1] - a[1])
      .slice(0, 5)
      .map(([event_type, count]) => ({ event_type, count })),
    
    severity_breakdown: Array.from(severityCounts.entries())
      .sort((a, b) => b[0] - a[0])
      .map(([severity, count]) => ({ severity, count })),
    
    protected_areas_nearby_count: 
      protectedAreas.contains_point.length + protectedAreas.nearby.length,
    
    trend: 'unknown',
  };
}

function calculateCoverageScore(stats: { sourceCount: number; eventCount: number }): number {
  // Simple heuristic: more sources = better coverage
  // Scale from 0-1 based on source count
  const sourceScore = Math.min(stats.sourceCount / 50, 1);
  const eventScore = Math.min(stats.eventCount / 20, 1);
  
  return Math.round((sourceScore * 0.6 + eventScore * 0.4) * 100) / 100;
}

// ============================================
// Coverage Request
// ============================================

export async function submitCoverageRequest(
  lat: number,
  lng: number,
  radiusKm: number,
  userId?: string,
  userEmail?: string,
  notes?: string
): Promise<{ id: string }> {
  
  const result = await prisma.$queryRaw<any[]>`
    INSERT INTO coverage_requests (latitude, longitude, radius_km, user_id, user_email, notes)
    VALUES (${lat}, ${lng}, ${radiusKm}, ${userId ?? null}::uuid, ${userEmail ?? null}, ${notes ?? null})
    RETURNING id
  `;
  
  logger.info({ lat, lng, radiusKm }, 'Coverage request submitted');
  
  return { id: result[0].id.toString() };
}
