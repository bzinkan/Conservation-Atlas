// packages/api/src/utils/organizationExtractor.ts
//
// Utilities for extracting and normalizing organization mentions from events

import { prisma } from '../db';
import { logger } from './logger';

// ============================================
// Types
// ============================================

export interface ExtractedOrganization {
  name: string;
  role: OrganizationRole;
  confidence: number;
  evidence?: string;
}

export type OrganizationRole = 
  | 'implementing'
  | 'reporting'
  | 'funding'
  | 'managing'
  | 'researching'
  | 'enforcing'
  | 'responding'
  | 'mentioned'
  | 'unknown';

export interface OrganizationMatch {
  id: number;
  name: string;
  normalized_name: string;
  org_type: string | null;
  confidence: number;
  is_new: boolean;
}

// ============================================
// Known Organizations (for better matching)
// ============================================

const KNOWN_ORGS: Record<string, { type: string; aliases: string[] }> = {
  'world wildlife fund': { type: 'ngo', aliases: ['wwf', 'world wide fund for nature'] },
  'the nature conservancy': { type: 'ngo', aliases: ['tnc', 'nature conservancy'] },
  'conservation international': { type: 'ngo', aliases: ['ci'] },
  'wildlife conservation society': { type: 'ngo', aliases: ['wcs'] },
  'iucn': { type: 'intergov', aliases: ['international union for conservation of nature'] },
  'greenpeace': { type: 'ngo', aliases: [] },
  'sierra club': { type: 'ngo', aliases: [] },
  'audubon society': { type: 'ngo', aliases: ['national audubon society', 'audubon'] },
  'noaa': { type: 'government', aliases: ['national oceanic and atmospheric administration'] },
  'epa': { type: 'government', aliases: ['environmental protection agency', 'us epa'] },
  'usgs': { type: 'government', aliases: ['united states geological survey', 'us geological survey'] },
  'usfws': { type: 'government', aliases: ['us fish and wildlife service', 'fish and wildlife service'] },
  'national park service': { type: 'government', aliases: ['nps'] },
  'usda': { type: 'government', aliases: ['department of agriculture'] },
  'united nations environment programme': { type: 'intergov', aliases: ['unep', 'un environment'] },
};

// ============================================
// Normalization
// ============================================

/**
 * Normalize an organization name for deduplication
 */
export function normalizeOrgName(name: string): string {
  return name
    .toLowerCase()
    .trim()
    // Remove common suffixes
    .replace(/\s*(inc\.?|llc|ltd\.?|corp\.?|co\.?|foundation|org\.?)$/i, '')
    // Remove punctuation
    .replace(/[.,'"]/g, '')
    // Collapse whitespace
    .replace(/\s+/g, ' ')
    .trim();
}

/**
 * Check if an organization name matches a known org
 */
export function matchKnownOrg(name: string): { 
  canonicalName: string; 
  type: string 
} | null {
  const normalized = normalizeOrgName(name);
  
  for (const [canonical, info] of Object.entries(KNOWN_ORGS)) {
    if (normalized === canonical || info.aliases.includes(normalized)) {
      return { canonicalName: canonical, type: info.type };
    }
  }
  
  return null;
}

/**
 * Infer organization type from name
 */
export function inferOrgType(name: string): string {
  const lower = name.toLowerCase();
  
  if (lower.includes('department') || lower.includes('ministry') || 
      lower.includes('agency') || lower.includes('bureau') ||
      lower.includes('service') || lower.includes('administration')) {
    return 'government';
  }
  
  if (lower.includes('university') || lower.includes('institute') ||
      lower.includes('college') || lower.includes('research')) {
    return 'academic';
  }
  
  if (lower.includes('foundation') || lower.includes('trust') ||
      lower.includes('society') || lower.includes('alliance') ||
      lower.includes('conservation') || lower.includes('wildlife')) {
    return 'ngo';
  }
  
  if (lower.includes('united nations') || lower.includes('un ') ||
      lower.includes('international')) {
    return 'intergov';
  }
  
  if (lower.includes('community') || lower.includes('local') ||
      lower.includes('village') || lower.includes('tribal')) {
    return 'community';
  }
  
  return 'unknown';
}

// ============================================
// Database Operations
// ============================================

/**
 * Find or create an organization by name
 */
export async function findOrCreateOrganization(
  name: string,
  options?: {
    orgType?: string;
    website?: string;
    confidence?: number;
  }
): Promise<OrganizationMatch> {
  const normalized = normalizeOrgName(name);
  
  // Check for known org match first
  const known = matchKnownOrg(name);
  const canonicalName = known?.canonicalName 
    ? name // Keep original casing but use canonical for matching
    : name;
  const orgType = options?.orgType || known?.type || inferOrgType(name);
  
  // Try to find existing
  const existing = await prisma.$queryRaw<any[]>`
    SELECT id, name, normalized_name, org_type, confidence
    FROM organizations
    WHERE normalized_name = ${normalized}
    LIMIT 1
  `;
  
  if (existing.length > 0) {
    return {
      id: parseInt(existing[0].id),
      name: existing[0].name,
      normalized_name: existing[0].normalized_name,
      org_type: existing[0].org_type,
      confidence: parseFloat(existing[0].confidence) || 0.7,
      is_new: false,
    };
  }
  
  // Create new organization
  const result = await prisma.$queryRaw<any[]>`
    INSERT INTO organizations (name, normalized_name, org_type, website, confidence)
    VALUES (${canonicalName}, ${normalized}, ${orgType}, ${options?.website ?? null}, ${options?.confidence ?? 0.7})
    RETURNING id, name, normalized_name, org_type, confidence
  `;
  
  logger.info({ name: canonicalName, org_type: orgType }, 'Created new organization');
  
  return {
    id: parseInt(result[0].id),
    name: result[0].name,
    normalized_name: result[0].normalized_name,
    org_type: result[0].org_type,
    confidence: parseFloat(result[0].confidence) || 0.7,
    is_new: true,
  };
}

/**
 * Link organizations to an event
 */
export async function linkOrganizationsToEvent(
  eventId: number,
  organizations: ExtractedOrganization[]
): Promise<void> {
  for (const org of organizations) {
    try {
      // Find or create the organization
      const orgMatch = await findOrCreateOrganization(org.name, {
        confidence: org.confidence,
      });
      
      // Create the link
      await prisma.$executeRaw`
        INSERT INTO event_organizations (event_id, organization_id, role, involvement_confidence, evidence_snippet)
        VALUES (${eventId}, ${orgMatch.id}, ${org.role}, ${org.confidence}, ${org.evidence ?? null})
        ON CONFLICT (event_id, organization_id, role) 
        DO UPDATE SET 
          involvement_confidence = GREATEST(event_organizations.involvement_confidence, EXCLUDED.involvement_confidence),
          evidence_snippet = COALESCE(EXCLUDED.evidence_snippet, event_organizations.evidence_snippet)
      `;
      
      // Update organization stats
      await prisma.$executeRaw`
        UPDATE organizations 
        SET event_count = event_count + 1, last_seen_at = NOW()
        WHERE id = ${orgMatch.id}
      `;
      
    } catch (error) {
      logger.warn({ eventId, org: org.name, error }, 'Failed to link organization to event');
    }
  }
}

/**
 * Extract organization mentions from LLM extraction output
 */
export function extractOrganizationsFromExtraction(
  extraction: any
): ExtractedOrganization[] {
  const organizations: ExtractedOrganization[] = [];
  
  // Check entities.organizations if present
  if (extraction.entities?.organizations) {
    for (const name of extraction.entities.organizations) {
      if (typeof name === 'string' && name.length > 2) {
        organizations.push({
          name,
          role: 'mentioned',
          confidence: 0.7,
        });
      }
    }
  }
  
  // Check actors.organizations if present (more detailed format)
  if (extraction.actors?.organizations) {
    for (const actor of extraction.actors.organizations) {
      if (typeof actor === 'string') {
        organizations.push({
          name: actor,
          role: 'mentioned',
          confidence: 0.7,
        });
      } else if (actor.name) {
        organizations.push({
          name: actor.name,
          role: mapRole(actor.role),
          confidence: actor.confidence ?? 0.7,
          evidence: actor.evidence,
        });
      }
    }
  }
  
  // Check source publisher as potential organization
  if (extraction.source_ref?.publisher) {
    const publisher = extraction.source_ref.publisher;
    // Only add if it looks like an org name (not generic news)
    if (!isGenericPublisher(publisher)) {
      organizations.push({
        name: publisher,
        role: 'reporting',
        confidence: 0.9,
      });
    }
  }
  
  // Deduplicate by normalized name
  const seen = new Set<string>();
  return organizations.filter(org => {
    const normalized = normalizeOrgName(org.name);
    if (seen.has(normalized)) return false;
    seen.add(normalized);
    return true;
  });
}

function mapRole(role: string | undefined): OrganizationRole {
  if (!role) return 'mentioned';
  
  const normalized = role.toLowerCase();
  const roleMap: Record<string, OrganizationRole> = {
    'implementing': 'implementing',
    'implementer': 'implementing',
    'reporting': 'reporting',
    'reporter': 'reporting',
    'funding': 'funding',
    'funder': 'funding',
    'managing': 'managing',
    'manager': 'managing',
    'researching': 'researching',
    'researcher': 'researching',
    'research': 'researching',
    'enforcing': 'enforcing',
    'enforcement': 'enforcing',
    'responding': 'responding',
    'responder': 'responding',
    'mentioned': 'mentioned',
  };
  
  return roleMap[normalized] || 'unknown';
}

function isGenericPublisher(publisher: string): boolean {
  const genericPublishers = [
    'reuters', 'associated press', 'ap', 'afp',
    'bbc', 'cnn', 'nbc', 'abc', 'cbs', 'fox',
    'new york times', 'washington post', 'guardian',
    'local news', 'staff reporter', 'wire service',
  ];
  
  const normalized = publisher.toLowerCase();
  return genericPublishers.some(g => normalized.includes(g));
}

// ============================================
// Batch Operations
// ============================================

/**
 * Process organizations for multiple events (batch)
 */
export async function processOrganizationsBatch(
  events: Array<{ eventId: number; organizations: ExtractedOrganization[] }>
): Promise<void> {
  for (const event of events) {
    await linkOrganizationsToEvent(event.eventId, event.organizations);
  }
}

/**
 * Merge duplicate organizations
 */
export async function mergeOrganizations(
  primaryId: number,
  duplicateIds: number[]
): Promise<void> {
  await prisma.$transaction(async (tx) => {
    // Update all event_organizations to point to primary
    for (const dupId of duplicateIds) {
      await tx.$executeRaw`
        UPDATE event_organizations 
        SET organization_id = ${primaryId}
        WHERE organization_id = ${dupId}
        AND NOT EXISTS (
          SELECT 1 FROM event_organizations eo2 
          WHERE eo2.event_id = event_organizations.event_id 
          AND eo2.organization_id = ${primaryId}
          AND eo2.role = event_organizations.role
        )
      `;
      
      // Delete conflicts (primary already has this link)
      await tx.$executeRaw`
        DELETE FROM event_organizations 
        WHERE organization_id = ${dupId}
      `;
      
      // Delete the duplicate org
      await tx.$executeRaw`
        DELETE FROM organizations WHERE id = ${dupId}
      `;
    }
    
    // Update event_count on primary
    await tx.$executeRaw`
      UPDATE organizations 
      SET event_count = (
        SELECT COUNT(DISTINCT event_id) FROM event_organizations WHERE organization_id = ${primaryId}
      )
      WHERE id = ${primaryId}
    `;
  });
  
  logger.info({ primaryId, duplicateIds }, 'Merged organizations');
}
