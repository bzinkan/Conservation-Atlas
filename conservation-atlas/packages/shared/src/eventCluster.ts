// packages/api/src/services/deduplication/eventCluster.ts

/**
 * Event Deduplication & Clustering Service
 * 
 * Handles:
 * 1. Finding duplicate/similar events
 * 2. Merging events that describe the same incident
 * 3. Multi-source confirmation (boosting confidence)
 * 4. Maintaining event clusters
 */

import { prisma } from '../../db';
import { logger } from '../../utils/logger';

export interface ClusterResult {
  action: 'new' | 'merged' | 'duplicate';
  primaryEventId: string;
  mergedEventIds: string[];
  confidenceBoost: number;
  sourceCount: number;
}

interface EventForClustering {
  id: string;
  title: string;
  eventTypePrimary: string;
  severityLevel: number;
  confidenceExtraction: number;
  country: string;
  admin1: string | null;
  latitude: number | null;
  longitude: number | null;
  startDate: Date | null;
  summaryShort: string;
  sourceCount: number;
  createdAt: Date;
}

// Configuration
const CLUSTER_CONFIG = {
  // Time window for considering events as potentially related
  TIME_WINDOW_HOURS: 72,
  
  // Maximum distance (km) for events to be considered same location
  MAX_DISTANCE_KM: 50,
  
  // Minimum similarity score to consider merging
  MIN_SIMILARITY_SCORE: 0.7,
  
  // Confidence boost per additional source
  SOURCE_CONFIDENCE_BOOST: 0.05,
  
  // Maximum confidence after boosting
  MAX_BOOSTED_CONFIDENCE: 0.98,
};

/**
 * Main clustering function - called after extraction
 */
export async function clusterEvent(eventId: string): Promise<ClusterResult> {
  logger.info({ eventId }, 'Starting event clustering');
  
  // 1. Load the new event
  const newEvent = await loadEventForClustering(eventId);
  if (!newEvent) {
    throw new Error(`Event not found: ${eventId}`);
  }
  
  // 2. Find candidate matches
  const candidates = await findCandidateMatches(newEvent);
  
  if (candidates.length === 0) {
    // No potential duplicates - this is a new unique event
    logger.info({ eventId }, 'No duplicates found - event is unique');
    return {
      action: 'new',
      primaryEventId: eventId,
      mergedEventIds: [],
      confidenceBoost: 0,
      sourceCount: newEvent.sourceCount,
    };
  }
  
  // 3. Score similarity with each candidate
  const scoredCandidates = candidates.map(candidate => ({
    candidate,
    similarity: calculateSimilarity(newEvent, candidate),
  }));
  
  // 4. Find best match above threshold
  const bestMatch = scoredCandidates
    .filter(sc => sc.similarity >= CLUSTER_CONFIG.MIN_SIMILARITY_SCORE)
    .sort((a, b) => b.similarity - a.similarity)[0];
  
  if (!bestMatch) {
    // Candidates found but none similar enough
    logger.info({ eventId, candidateCount: candidates.length }, 'Candidates found but similarity too low');
    return {
      action: 'new',
      primaryEventId: eventId,
      mergedEventIds: [],
      confidenceBoost: 0,
      sourceCount: newEvent.sourceCount,
    };
  }
  
  // 5. Decide: merge into existing or mark as duplicate
  const existingEvent = bestMatch.candidate;
  
  logger.info({
    eventId,
    existingEventId: existingEvent.id,
    similarity: bestMatch.similarity,
  }, 'Found matching event - merging');
  
  // 6. Merge: update existing event with additional source info
  const mergeResult = await mergeEvents(existingEvent, newEvent);
  
  return mergeResult;
}

/**
 * Find events that could potentially be duplicates
 */
async function findCandidateMatches(event: EventForClustering): Promise<EventForClustering[]> {
  const timeWindowStart = new Date(event.createdAt);
  timeWindowStart.setHours(timeWindowStart.getHours() - CLUSTER_CONFIG.TIME_WINDOW_HOURS);
  
  // Query for potential matches
  // Criteria: same event type, same country, within time window
  const candidates = await prisma.event.findMany({
    where: {
      id: { not: event.id },
      eventTypePrimary: event.eventTypePrimary,
      country: event.country,
      createdAt: { gte: timeWindowStart },
    },
    orderBy: { createdAt: 'desc' },
    take: 50, // Limit candidates
  });
  
  // Filter by geographic proximity if coordinates available
  if (event.latitude && event.longitude) {
    return candidates.filter(c => {
      if (!c.latitude || !c.longitude) return true; // Include if no coords
      
      const distance = calculateDistanceKm(
        event.latitude!, event.longitude!,
        parseFloat(c.latitude.toString()), parseFloat(c.longitude.toString())
      );
      
      return distance <= CLUSTER_CONFIG.MAX_DISTANCE_KM;
    }).map(mapToClusteringEvent);
  }
  
  // If no coordinates, filter by admin1 region
  return candidates
    .filter(c => c.admin1 === event.admin1 || !event.admin1 || !c.admin1)
    .map(mapToClusteringEvent);
}

/**
 * Calculate similarity score between two events (0-1)
 */
function calculateSimilarity(event1: EventForClustering, event2: EventForClustering): number {
  let score = 0;
  let weights = 0;
  
  // 1. Event type match (required - already filtered, but weight it)
  if (event1.eventTypePrimary === event2.eventTypePrimary) {
    score += 0.2;
  }
  weights += 0.2;
  
  // 2. Geographic similarity
  if (event1.latitude && event1.longitude && event2.latitude && event2.longitude) {
    const distance = calculateDistanceKm(
      event1.latitude, event1.longitude,
      event2.latitude, event2.longitude
    );
    
    // Closer = higher score (0-0.3)
    const geoScore = Math.max(0, 1 - distance / CLUSTER_CONFIG.MAX_DISTANCE_KM) * 0.3;
    score += geoScore;
    weights += 0.3;
  } else if (event1.admin1 && event2.admin1 && event1.admin1 === event2.admin1) {
    // Same admin1 region
    score += 0.2;
    weights += 0.3;
  } else {
    weights += 0.3;
  }
  
  // 3. Title similarity (Jaccard on words)
  const titleSimilarity = calculateTextSimilarity(event1.title, event2.title);
  score += titleSimilarity * 0.25;
  weights += 0.25;
  
  // 4. Summary similarity
  const summarySimilarity = calculateTextSimilarity(event1.summaryShort, event2.summaryShort);
  score += summarySimilarity * 0.15;
  weights += 0.15;
  
  // 5. Temporal proximity
  if (event1.startDate && event2.startDate) {
    const daysDiff = Math.abs(
      (event1.startDate.getTime() - event2.startDate.getTime()) / (1000 * 60 * 60 * 24)
    );
    
    // Closer in time = higher score
    const temporalScore = Math.max(0, 1 - daysDiff / 7) * 0.1; // Within a week
    score += temporalScore;
    weights += 0.1;
  } else {
    weights += 0.1;
  }
  
  return weights > 0 ? score / weights : 0;
}

/**
 * Calculate text similarity using Jaccard index on words
 */
function calculateTextSimilarity(text1: string, text2: string): number {
  const words1 = new Set(tokenize(text1));
  const words2 = new Set(tokenize(text2));
  
  if (words1.size === 0 && words2.size === 0) return 1;
  if (words1.size === 0 || words2.size === 0) return 0;
  
  const intersection = new Set([...words1].filter(w => words2.has(w)));
  const union = new Set([...words1, ...words2]);
  
  return intersection.size / union.size;
}

/**
 * Tokenize text into words for comparison
 */
function tokenize(text: string): string[] {
  return text
    .toLowerCase()
    .replace(/[^\w\s]/g, ' ')
    .split(/\s+/)
    .filter(w => w.length > 2) // Skip short words
    .filter(w => !STOP_WORDS.has(w));
}

const STOP_WORDS = new Set([
  'the', 'a', 'an', 'and', 'or', 'but', 'in', 'on', 'at', 'to', 'for',
  'of', 'with', 'by', 'from', 'as', 'is', 'was', 'are', 'were', 'been',
  'be', 'have', 'has', 'had', 'do', 'does', 'did', 'will', 'would', 'could',
  'should', 'may', 'might', 'must', 'shall', 'can', 'need', 'this', 'that',
  'these', 'those', 'than', 'when', 'where', 'which', 'who', 'whom', 'whose',
]);

/**
 * Calculate distance between two coordinates in km (Haversine)
 */
function calculateDistanceKm(lat1: number, lon1: number, lat2: number, lon2: number): number {
  const R = 6371; // Earth radius in km
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  
  const a = Math.sin(dLat / 2) ** 2 +
            Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) *
            Math.sin(dLon / 2) ** 2;
  
  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function toRad(deg: number): number {
  return deg * (Math.PI / 180);
}

/**
 * Merge new event into existing event
 */
async function mergeEvents(
  existing: EventForClustering,
  newEvent: EventForClustering
): Promise<ClusterResult> {
  // Calculate confidence boost from additional source
  const newSourceCount = existing.sourceCount + newEvent.sourceCount;
  const additionalSources = newEvent.sourceCount;
  
  let confidenceBoost = additionalSources * CLUSTER_CONFIG.SOURCE_CONFIDENCE_BOOST;
  const newConfidence = Math.min(
    existing.confidenceExtraction + confidenceBoost,
    CLUSTER_CONFIG.MAX_BOOSTED_CONFIDENCE
  );
  
  await prisma.$transaction(async (tx) => {
    // 1. Update existing event
    await tx.event.update({
      where: { id: existing.id },
      data: {
        confidenceExtraction: newConfidence,
        sourceCount: newSourceCount,
        updatedAt: new Date(),
        // Optionally update severity if new event has higher severity
        ...(newEvent.severityLevel > existing.severityLevel ? {
          severityLevel: newEvent.severityLevel,
        } : {}),
      },
    });
    
    // 2. Transfer sources from new event to existing
    await tx.eventSource.updateMany({
      where: { eventId: newEvent.id },
      data: { eventId: existing.id },
    });
    
    // 3. Mark new event as merged/duplicate
    await tx.event.update({
      where: { id: newEvent.id },
      data: {
        mergedIntoId: existing.id,
        status: 'merged',
      },
    });
    
    // 4. Create merge record for audit trail
    await tx.$executeRaw`
      INSERT INTO event_merges (primary_event_id, merged_event_id, similarity_score, merged_at)
      VALUES (${existing.id}, ${newEvent.id}, ${0}, NOW())
    `;
  });
  
  logger.info({
    primaryEventId: existing.id,
    mergedEventId: newEvent.id,
    newSourceCount,
    confidenceBoost,
  }, 'Events merged successfully');
  
  return {
    action: 'merged',
    primaryEventId: existing.id,
    mergedEventIds: [newEvent.id],
    confidenceBoost,
    sourceCount: newSourceCount,
  };
}

/**
 * Map Prisma event to clustering interface
 */
function mapToClusteringEvent(event: any): EventForClustering {
  return {
    id: event.id,
    title: event.title,
    eventTypePrimary: event.eventTypePrimary,
    severityLevel: event.severityLevel,
    confidenceExtraction: parseFloat(event.confidenceExtraction?.toString() ?? '0.5'),
    country: event.country,
    admin1: event.admin1,
    latitude: event.latitude ? parseFloat(event.latitude.toString()) : null,
    longitude: event.longitude ? parseFloat(event.longitude.toString()) : null,
    startDate: event.startDate,
    summaryShort: event.summaryShort,
    sourceCount: event.sourceCount,
    createdAt: event.createdAt,
  };
}

/**
 * Load event for clustering
 */
async function loadEventForClustering(eventId: string): Promise<EventForClustering | null> {
  const event = await prisma.event.findUnique({
    where: { id: eventId },
  });
  
  if (!event) return null;
  return mapToClusteringEvent(event);
}

/**
 * Find all events in a cluster
 */
export async function getEventCluster(eventId: string): Promise<EventForClustering[]> {
  // Find the primary event (might be this one or one this was merged into)
  const event = await prisma.event.findUnique({
    where: { id: eventId },
    select: { id: true, mergedIntoId: true },
  });
  
  if (!event) return [];
  
  const primaryId = event.mergedIntoId ?? event.id;
  
  // Get primary + all merged events
  const clusterEvents = await prisma.event.findMany({
    where: {
      OR: [
        { id: primaryId },
        { mergedIntoId: primaryId },
      ],
    },
  });
  
  return clusterEvents.map(mapToClusteringEvent);
}
