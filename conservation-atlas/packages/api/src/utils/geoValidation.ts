// packages/api/src/utils/geoValidation.ts

/**
 * Geolocation validation for extracted events
 * 
 * Checks:
 * 1. Coordinates within valid ranges
 * 2. Country-coordinate consistency
 * 3. Ocean/land validation
 */

import { logger } from './logger';

export interface GeoValidationResult {
  valid: boolean;
  issues: string[];
  adjustedConfidence: number;
  shouldNullifyCoords: boolean;
  suggestedFix?: {
    action: 'nullify' | 'swap' | 'use_centroid';
    reason: string;
  };
}

interface ExtractionLocation {
  geometry?: {
    type: string;
    coordinates?: [number, number]; // [lng, lat]
    precision?: string;
  } | null;
  admin: {
    country: string;
    admin1?: string | null;
    admin2?: string | null;
    locality?: string | null;
  };
}

// Country bounding boxes (approximate) - [minLng, minLat, maxLng, maxLat]
const COUNTRY_BOUNDS: Record<string, [number, number, number, number]> = {
  'united states': [-125, 24, -66, 49],
  'usa': [-125, 24, -66, 49],
  'canada': [-141, 41, -52, 84],
  'brazil': [-74, -34, -32, 6],
  'australia': [112, -44, 154, -10],
  'china': [73, 18, 135, 54],
  'india': [68, 6, 97, 36],
  'russia': [19, 41, 180, 82],
  'united kingdom': [-8, 49, 2, 61],
  'france': [-5, 41, 10, 51],
  'germany': [5, 47, 16, 55],
  'japan': [123, 24, 146, 46],
  'indonesia': [95, -11, 141, 6],
  'mexico': [-118, 14, -86, 33],
  'south africa': [16, -35, 33, -22],
  'kenya': [33, -5, 42, 5],
  'tanzania': [29, -12, 41, -1],
  'peru': [-82, -18, -68, 0],
  'colombia': [-79, -4, -67, 14],
  'democratic republic of congo': [12, -14, 32, 6],
  'madagascar': [43, -26, 51, -12],
  'new zealand': [166, -47, 179, -34],
  'norway': [4, 57, 32, 71],
  'sweden': [11, 55, 24, 69],
  'finland': [20, 59, 32, 70],
  'italy': [6, 36, 19, 47],
  'spain': [-10, 35, 5, 44],
  'portugal': [-10, 36, -6, 42],
  'philippines': [116, 5, 127, 21],
  'vietnam': [102, 8, 110, 24],
  'thailand': [97, 5, 106, 21],
  'malaysia': [99, 0, 120, 8],
  'singapore': [103, 1, 104, 2],
  'costa rica': [-86, 8, -82, 11],
  'panama': [-83, 7, -77, 10],
  'ecuador': [-81, -5, -75, 2],
  'chile': [-76, -56, -66, -17],
  'argentina': [-74, -55, -53, -21],
  'egypt': [24, 22, 37, 32],
  'morocco': [-13, 27, -1, 36],
  'nigeria': [2, 4, 15, 14],
  'ethiopia': [33, 3, 48, 15],
  'botswana': [20, -27, 30, -17],
  'namibia': [11, -29, 25, -17],
  'iceland': [-24, 63, -13, 67],
  'greenland': [-73, 59, -11, 84],
};

/**
 * Validate extracted geolocation data
 */
export function validateGeolocation(extraction: { location: ExtractionLocation }): GeoValidationResult {
  const { location } = extraction;
  const issues: string[] = [];
  let adjustedConfidence = 1.0;
  let shouldNullifyCoords = false;
  let suggestedFix: GeoValidationResult['suggestedFix'];

  // If no coordinates, that's fine - just use admin location
  if (!location.geometry?.coordinates) {
    return {
      valid: true,
      issues: [],
      adjustedConfidence: 0.5, // Lower confidence for admin-only
      shouldNullifyCoords: false,
    };
  }

  const [lng, lat] = location.geometry.coordinates;

  // Check 1: Valid coordinate ranges
  if (lat < -90 || lat > 90) {
    issues.push(`Invalid latitude: ${lat} (must be -90 to 90)`);
    shouldNullifyCoords = true;
    suggestedFix = { action: 'nullify', reason: 'Latitude out of range' };
  }

  if (lng < -180 || lng > 180) {
    issues.push(`Invalid longitude: ${lng} (must be -180 to 180)`);
    shouldNullifyCoords = true;
    suggestedFix = { action: 'nullify', reason: 'Longitude out of range' };
  }

  // Check 2: Lat/Lng possibly swapped (common LLM error)
  if (!shouldNullifyCoords && Math.abs(lat) > 90 && Math.abs(lng) <= 90) {
    issues.push(`Coordinates may be swapped (lat=${lat}, lng=${lng})`);
    adjustedConfidence *= 0.3;
    suggestedFix = { action: 'swap', reason: 'Lat/lng appear to be swapped' };
  }

  // Check 3: Country-coordinate consistency
  if (!shouldNullifyCoords && location.admin.country) {
    const countryCheck = checkCountryCoordinateMatch(
      location.admin.country,
      lng,
      lat
    );
    
    if (!countryCheck.matches) {
      issues.push(countryCheck.reason);
      adjustedConfidence *= 0.4;
      
      if (countryCheck.severeMismatch) {
        suggestedFix = { 
          action: 'use_centroid', 
          reason: `Coordinates don't match country (${location.admin.country})` 
        };
      }
    }
  }

  // Check 4: Point in ocean (basic check for obvious errors)
  if (!shouldNullifyCoords) {
    const oceanCheck = checkIfLikelyOcean(lng, lat);
    if (oceanCheck.isOcean && !isMarineEventExpected(extraction)) {
      issues.push(`Coordinates appear to be in the ocean`);
      adjustedConfidence *= 0.5;
    }
  }

  // Check 5: Precision sanity
  if (location.geometry.precision === 'exact' && issues.length > 0) {
    issues.push(`Precision marked as 'exact' but validation found issues`);
    adjustedConfidence *= 0.7;
  }

  // Calculate final confidence
  if (shouldNullifyCoords) {
    adjustedConfidence = 0;
  } else {
    adjustedConfidence = Math.max(0.1, adjustedConfidence);
  }

  const result: GeoValidationResult = {
    valid: issues.length === 0,
    issues,
    adjustedConfidence,
    shouldNullifyCoords,
    suggestedFix,
  };

  if (issues.length > 0) {
    logger.debug({ result }, 'Geo validation issues found');
  }

  return result;
}

/**
 * Check if coordinates fall within expected country bounds
 */
function checkCountryCoordinateMatch(
  country: string,
  lng: number,
  lat: number
): { matches: boolean; reason: string; severeMismatch: boolean } {
  const normalizedCountry = country.toLowerCase().trim();
  const bounds = COUNTRY_BOUNDS[normalizedCountry];

  if (!bounds) {
    // Country not in our lookup - can't validate
    return { matches: true, reason: '', severeMismatch: false };
  }

  const [minLng, minLat, maxLng, maxLat] = bounds;
  
  // Add some buffer (coordinates near borders might be slightly off)
  const buffer = 2; // degrees
  
  const inBounds = (
    lng >= minLng - buffer &&
    lng <= maxLng + buffer &&
    lat >= minLat - buffer &&
    lat <= maxLat + buffer
  );

  if (inBounds) {
    return { matches: true, reason: '', severeMismatch: false };
  }

  // Calculate how far off we are
  const lngDist = Math.min(
    Math.abs(lng - minLng),
    Math.abs(lng - maxLng)
  );
  const latDist = Math.min(
    Math.abs(lat - minLat),
    Math.abs(lat - maxLat)
  );
  const totalDist = Math.sqrt(lngDist ** 2 + latDist ** 2);

  return {
    matches: false,
    reason: `Coordinates (${lng}, ${lat}) are ~${totalDist.toFixed(0)}° outside ${country} bounds`,
    severeMismatch: totalDist > 30, // More than 30 degrees off is clearly wrong
  };
}

/**
 * Basic check if point is likely in the ocean
 * This is a rough heuristic - proper solution would use a land/ocean mask
 */
function checkIfLikelyOcean(lng: number, lat: number): { isOcean: boolean } {
  // Pacific Ocean (rough)
  if (lng > 140 || lng < -120) {
    if (lat > -60 && lat < 60) {
      // Could be Pacific islands, Japan, etc - uncertain
      return { isOcean: false };
    }
  }
  
  // Atlantic Ocean (rough mid-Atlantic)
  if (lng > -40 && lng < -20 && lat > -30 && lat < 40) {
    return { isOcean: true };
  }
  
  // Indian Ocean (rough)
  if (lng > 50 && lng < 90 && lat > -40 && lat < 0) {
    // Careful - this could be Indonesia, Sri Lanka, etc.
    return { isOcean: false };
  }
  
  // Southern Ocean
  if (lat < -60) {
    return { isOcean: true };
  }
  
  // Arctic Ocean
  if (lat > 80) {
    return { isOcean: true };
  }

  return { isOcean: false };
}

/**
 * Check if this event type would naturally occur in marine areas
 */
function isMarineEventExpected(extraction: { location: ExtractionLocation } & Record<string, any>): boolean {
  const eventType = extraction.event_type?.primary;
  const marineTypes = ['coral_bleaching', 'oil_spill', 'pollution'];
  
  if (marineTypes.includes(eventType)) {
    return true;
  }
  
  // Check if location mentions marine keywords
  const locationText = [
    extraction.location.admin.locality,
    extraction.location.description,
  ].filter(Boolean).join(' ').toLowerCase();
  
  const marineKeywords = ['ocean', 'sea', 'reef', 'marine', 'coast', 'island', 'bay', 'gulf'];
  return marineKeywords.some(kw => locationText.includes(kw));
}

/**
 * Get centroid coordinates for a country (fallback)
 */
export function getCountryCentroid(country: string): [number, number] | null {
  const CENTROIDS: Record<string, [number, number]> = {
    'united states': [-98.5795, 39.8283],
    'usa': [-98.5795, 39.8283],
    'canada': [-106.3468, 56.1304],
    'brazil': [-51.9253, -14.2350],
    'australia': [133.7751, -25.2744],
    'china': [104.1954, 35.8617],
    'india': [78.9629, 20.5937],
    'russia': [105.3188, 61.5240],
    'united kingdom': [-3.4360, 55.3781],
    'france': [2.2137, 46.2276],
    'germany': [10.4515, 51.1657],
    'japan': [138.2529, 36.2048],
    'indonesia': [113.9213, -0.7893],
    'mexico': [-102.5528, 23.6345],
    // Add more as needed
  };

  return CENTROIDS[country.toLowerCase().trim()] ?? null;
}
