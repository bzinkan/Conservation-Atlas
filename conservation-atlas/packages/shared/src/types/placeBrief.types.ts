// packages/shared/src/types/placeBrief.ts
//
// Types for the "Click anywhere on map" Place Brief feature

export interface PlaceBriefRequest {
  lat: number;
  lng: number;
  radius_km?: number;        // Default 50
  time_window_days?: number; // Default 30
}

export interface PlaceBriefResponse {
  schema_version: 'place_brief_v1';
  
  request: {
    lat: number;
    lng: number;
    radius_km: number;
    time_window_days: number;
    as_of: string; // ISO datetime
  };
  
  place: PlaceInfo;
  coverage: CoverageInfo;
  summary: PlaceSummary;
  protected_areas: ProtectedAreasResult;
  events: PaginatedEvents;
  organizations: PaginatedOrganizations;
  actions: AvailableActions;
  disclaimer: Disclaimer;
}

// ============================================
// Place Information
// ============================================

export interface PlaceInfo {
  display_name: string;
  admin: {
    country: string;
    admin1: string | null;
    admin2: string | null;
    locality: string | null;
  };
  geocoding: {
    provider: 'google_maps' | 'mapbox' | 'nominatim' | null;
    place_id: string | null;
    confidence: number;
  } | null;
}

// ============================================
// Coverage Information
// ============================================

export interface CoverageInfo {
  coverage_score: number;  // 0-1, how well covered this area is
  notes: string;
  sources_considered: number;
  events_found: number;
  organizations_found: number;
  is_low_coverage: boolean;
}

// ============================================
// Summary Statistics
// ============================================

export interface PlaceSummary {
  top_threat_types: Array<{
    event_type: string;
    count: number;
  }>;
  severity_breakdown: Array<{
    severity: number;
    count: number;
  }>;
  protected_areas_nearby_count: number;
  trend: 'increasing' | 'stable' | 'decreasing' | 'unknown';
}

// ============================================
// Protected Areas
// ============================================

export interface ProtectedAreasResult {
  contains_point: ProtectedAreaBrief[];
  nearby: ProtectedAreaBrief[];
}

export interface ProtectedAreaBrief {
  protected_area_id: number;
  name: string;
  designation: string | null;
  iucn_category: string | null;
  country: string;
  external_id: string | null;
  source: 'wdpa' | 'national' | 'local';
  relation: 'inside' | 'near';
  distance_km: number;
  centroid: {
    lat: number;
    lng: number;
  };
  area_km2?: number;
}

// ============================================
// Events
// ============================================

export interface PaginatedEvents {
  items: EventBrief[];
  total_count: number;
  next_cursor: string | null;
}

export interface EventBrief {
  event_id: number;
  title: string;
  event_type_primary: string;
  event_type_secondary?: string | null;
  status: string;
  severity_level: number;
  
  confidence: {
    extraction: number;
    geolocation: number;
  };
  
  time: {
    reported_at: string | null;
    event_start: string | null;
    event_end: string | null;
    is_ongoing: boolean;
  };
  
  location: {
    name: string;
    coordinates: {
      lat: number;
      lng: number;
    } | null;
    geocoding_precision: 'exact' | 'approximate' | 'centroid' | null;
    distance_km: number;
  };
  
  summary_short: string;
  
  protected_area_context: Array<{
    protected_area_id: number;
    name: string;
    relation: 'inside' | 'near';
    distance_km: number;
  }>;
  
  organizations: Array<{
    organization_id: number;
    name: string;
    org_type: string | null;
    role: string;
    involvement_confidence: number;
  }>;
  
  sources: Array<{
    source_id: number;
    url: string;
    publisher: string | null;
    published_at: string | null;
    source_type: string;
  }>;
  
  briefing_video?: {
    available: boolean;
    video_url?: string;
    runtime_seconds?: number;
  };
  
  classroom_episode?: {
    available: boolean;
    episode_id?: number;
    grade_bands?: string[];
  };
}

// ============================================
// Organizations
// ============================================

export interface PaginatedOrganizations {
  items: OrganizationBrief[];
  total_count: number;
  next_cursor: string | null;
}

export interface OrganizationBrief {
  organization_id: number;
  name: string;
  org_type: string | null;
  website: string | null;
  logo_url?: string | null;
  
  activity_summary: string | null;
  activity_window_days: number;
  
  in_area_metrics: {
    events_count: number;
    most_common_roles: Array<{
      role: string;
      count: number;
    }>;
    top_event_types: Array<{
      event_type: string;
      count: number;
    }>;
  };
  
  top_related_events: Array<{
    event_id: number;
    title: string;
    severity_level: number;
  }>;
  
  sources: Array<{
    source_id: number;
    url: string;
    publisher: string | null;
    published_at: string | null;
  }>;
}

// ============================================
// Actions
// ============================================

export interface AvailableActions {
  subscribe: {
    supported: boolean;
    default_radius_km: number;
    suggested_filters: {
      min_severity: number;
      event_types: string[];
    };
  };
  export: {
    supported: boolean;
    formats: string[];
  };
  request_coverage?: {
    supported: boolean;
    reason?: string;
  };
}

// ============================================
// Disclaimer
// ============================================

export interface Disclaimer {
  ai_generated_summaries: boolean;
  text: string;
}

// ============================================
// Helper types for queries
// ============================================

export interface PlaceBriefQueryParams {
  lat: number;
  lng: number;
  radiusKm: number;
  timeWindowDays: number;
  limit?: number;
  cursor?: string;
}

export interface OrganizationRole {
  role: 'implementing' | 'reporting' | 'funding' | 'managing' | 'researching' | 'enforcing' | 'responding' | 'mentioned' | 'unknown';
}

export const ORGANIZATION_ROLES = [
  'implementing',
  'reporting',
  'funding',
  'managing',
  'researching',
  'enforcing',
  'responding',
  'mentioned',
  'unknown',
] as const;

export const ORGANIZATION_TYPES = [
  'ngo',
  'government',
  'academic',
  'community',
  'private',
  'intergov',
  'unknown',
] as const;
