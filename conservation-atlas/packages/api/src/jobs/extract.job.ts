// packages/api/src/jobs/extract.job.ts

import Ajv, { ValidateFunction } from "ajv";
import addFormats from "ajv-formats";
import { OpenAI } from "openai";

import type { ExtractJobMessage, ClusterJobMessage } from "@conservation-atlas/shared/types/queueMessages";
import { createJobMessage } from "@conservation-atlas/shared/types/queueMessages";
import eventSchema from "@conservation-atlas/shared/schemas/event_extraction_v1.schema.json";

import { logger } from "../utils/logger";
import { prisma } from "../db";
import { enqueueJob } from "../services/queue/sqsService";
import { validateGeolocation, type GeoValidationResult } from "../utils/geoValidation";
import { calculateSourceQualityScore } from "../utils/sourceQuality";

// ============================================
// Configuration
// ============================================
const OPENAI_MODEL = process.env.OPENAI_EXTRACT_MODEL || "gpt-4-turbo";
const MAX_SOURCE_CHARS = 20_000;
const MIN_SOURCE_CHARS = 200;

// ============================================
// AJV Validator Setup
// ============================================
const ajv = new Ajv({ allErrors: true, strict: false });
addFormats(ajv);
const validateEventSchema = ajv.compile(eventSchema) as ValidateFunction;

// ============================================
// OpenAI Client
// ============================================
const openai = new OpenAI({ apiKey: process.env.OPENAI_API_KEY });

// ============================================
// Types
// ============================================
interface SourceRow {
  id: number;
  url: string;
  publisher: string | null;
  title: string | null;
  sourceType: string;
  publishedAt: Date | null;
  scrapedAt: Date;
  language: string | null;
  snippet: string | null;
  credibilityScore: number | null;
}

interface ExtractionResult {
  eventId: string;
  confidence: number;
  geoValidation: GeoValidationResult;
}

// ============================================
// Main Job Handler
// ============================================
export async function runExtractJob(msg: ExtractJobMessage): Promise<ExtractionResult | null> {
  const { source_id, force_reextract, model_hint } = msg.payload;

  logger.info({ job_id: msg.job_id, source_id }, "EXTRACT job started");

  // 1. Load source record
  const source = await loadSource(source_id);
  if (!source) {
    throw new Error(`Source not found: ${source_id}`);
  }

  // 2. Check for existing extraction (idempotency)
  if (!force_reextract) {
    const existing = await hasExistingExtraction(source_id);
    if (existing) {
      logger.info({ source_id }, "Extraction already exists; skipping");
      return null;
    }
  }

  // 3. Validate source text
  const rawText = truncateText(source.snippet ?? "", MAX_SOURCE_CHARS);
  if (!rawText || rawText.trim().length < MIN_SOURCE_CHARS) {
    logger.warn({ source_id, length: rawText.length }, "Source text too short");
    await markSourceStatus(source_id, "too_short");
    return null;
  }

  // 4. Calculate source quality score
  const qualityScore = calculateSourceQualityScore({
    sourceType: source.sourceType,
    publisher: source.publisher,
    credibilityScore: source.credibilityScore,
  });

  // 5. Call OpenAI for extraction
  const systemPrompt = buildSystemPrompt();
  
  // First attempt
  let extraction = await callOpenAIForExtraction({
    system: systemPrompt,
    user: buildUserPrompt(source, rawText, false),
  });
  
  let valid = validateEventSchema(extraction);

  // Retry with stricter prompt if validation failed
  if (!valid) {
    logger.warn(
      { source_id, errors: validateEventSchema.errors?.slice(0, 3) },
      "Validation failed on attempt 1; retrying with strict prompt"
    );

    extraction = await callOpenAIForExtraction({
      system: systemPrompt,
      user: buildUserPrompt(source, rawText, true),
    });
    
    valid = validateEventSchema(extraction);
  }

  if (!valid) {
    const errors = validateEventSchema.errors ?? [];
    logger.error({ source_id, errors: errors.slice(0, 5) }, "Extraction invalid after retries");
    await markSourceStatus(source_id, "extraction_failed");
    throw new Error(`Invalid event_extraction_v1 JSON for source_id=${source_id}`);
  }

  // 6. Validate geolocation
  const geoValidation = validateGeolocation(extraction);
  
  if (!geoValidation.valid) {
    logger.warn({ source_id, geoValidation }, "Geolocation validation issues");
    // Downgrade confidence but don't fail
    extraction.confidence.geolocation = geoValidation.adjustedConfidence;
    
    if (geoValidation.shouldNullifyCoords) {
      extraction.location.geometry = null;
    }
  }

  // 7. Save extraction result
  const { eventId } = await saveExtractionResult({
    sourceId: source_id,
    extraction,
    model: model_hint === "anthropic" ? "claude" : OPENAI_MODEL,
    qualityScore,
    geoValidation,
  });

  logger.info({ source_id, eventId }, "Extraction saved successfully");

  // 8. Enqueue next stage (CLUSTER for deduplication)
  const clusterMsg = createJobMessage<ClusterJobMessage>(
    "CLUSTER",
    { event_id: parseInt(eventId) },
    msg.correlation_id
  );
  
  await enqueueJob("CLUSTER", clusterMsg);
  logger.info({ source_id, eventId }, "CLUSTER job enqueued");

  return {
    eventId,
    confidence: extraction.confidence.extraction,
    geoValidation,
  };
}

// ============================================
// Database Helpers
// ============================================
async function loadSource(sourceId: number): Promise<SourceRow | null> {
  const source = await prisma.eventSource.findUnique({
    where: { id: sourceId.toString() },
  });
  
  if (!source) return null;
  
  return {
    id: parseInt(source.id),
    url: source.url,
    publisher: source.publisher,
    title: source.title,
    sourceType: source.sourceType,
    publishedAt: source.publishedAt,
    scrapedAt: source.scrapedAt,
    language: source.language,
    snippet: source.snippet,
    credibilityScore: source.credibilityScore ? parseFloat(source.credibilityScore.toString()) : null,
  };
}

async function hasExistingExtraction(sourceId: number): Promise<boolean> {
  const count = await prisma.$queryRaw<[{ count: bigint }]>`
    SELECT COUNT(*) as count FROM source_extractions WHERE source_id = ${sourceId}
  `;
  return count[0].count > 0n;
}

async function markSourceStatus(sourceId: number, status: string): Promise<void> {
  // Update source with extraction status
  await prisma.$executeRaw`
    UPDATE event_sources 
    SET extraction_status = ${status}, updated_at = NOW()
    WHERE id = ${sourceId.toString()}
  `;
}

async function saveExtractionResult(args: {
  sourceId: number;
  extraction: any;
  model: string;
  qualityScore: number;
  geoValidation: GeoValidationResult;
}): Promise<{ eventId: string }> {
  const { sourceId, extraction, model, qualityScore, geoValidation } = args;
  
  // Use transaction to create event and link to source
  const result = await prisma.$transaction(async (tx) => {
    // Create the event
    const event = await tx.event.create({
      data: {
        title: extraction.title,
        eventTypePrimary: extraction.event_type.primary,
        eventTypeSecondary: extraction.event_type.secondary,
        severityLevel: extraction.severity.level,
        confidenceExtraction: extraction.confidence.extraction,
        
        // Geography
        country: extraction.location.admin.country,
        admin1: extraction.location.admin.admin1,
        admin2: extraction.location.admin.admin2,
        locationName: extraction.location.admin.locality,
        latitude: extraction.location.geometry?.coordinates?.[1],
        longitude: extraction.location.geometry?.coordinates?.[0],
        
        // Content
        summaryShort: extraction.summary.short,
        summaryDetailed: extraction.summary.detailed,
        
        // Classification
        isClassroomSafe: extraction.classification?.is_classroom_safe ?? false,
        classroomTopicTags: extraction.classification?.classroom_topic_tags ?? [],
        
        // Temporal
        startDate: extraction.temporal?.start_date ? new Date(extraction.temporal.start_date) : null,
        endDate: extraction.temporal?.end_date ? new Date(extraction.temporal.end_date) : null,
        isOngoing: extraction.temporal?.is_ongoing ?? true,
        
        // Metadata
        sourceCount: 1,
        primarySourceType: extraction.source_ref.publisher ? 
          categorizePublisher(extraction.source_ref.publisher) : 'news',
        episodeEligible: shouldMarkEpisodeEligible(extraction, qualityScore),
      },
    });

    // Link source to event
    await tx.eventSource.update({
      where: { id: sourceId.toString() },
      data: { eventId: event.id },
    });

    // Store raw extraction for debugging/auditing
    await tx.$executeRaw`
      INSERT INTO source_extractions (source_id, event_id, schema_version, model, extraction_json, quality_score, geo_validation, created_at)
      VALUES (${sourceId}, ${event.id}, 'event_extraction_v1', ${model}, ${JSON.stringify(extraction)}::jsonb, ${qualityScore}, ${JSON.stringify(geoValidation)}::jsonb, NOW())
    `;

    return { eventId: event.id };
  });

  return result;
}

// ============================================
// OpenAI Helpers
// ============================================
async function callOpenAIForExtraction(args: { 
  system: string; 
  user: string 
}): Promise<any> {
  const response = await openai.chat.completions.create({
    model: OPENAI_MODEL,
    temperature: 0.2,
    max_tokens: 4000,
    messages: [
      { role: "system", content: args.system },
      { role: "user", content: args.user },
    ],
    response_format: { type: "json_object" },
  });

  const content = response.choices?.[0]?.message?.content;
  if (!content) {
    throw new Error("OpenAI returned empty content");
  }

  try {
    return JSON.parse(content);
  } catch (e) {
    throw new Error(`Failed to parse JSON from OpenAI: ${(e as Error).message}`);
  }
}

function buildSystemPrompt(): string {
  return `You are a conservation event data extraction engine.

TASK: Extract structured event data from news articles and reports about conservation, environmental events, and wildlife.

OUTPUT: Return ONLY valid JSON matching the event_extraction_v1 schema. No markdown fences or explanations.

RULES:
1. Extract exactly ONE event per source (the most significant/newsworthy one)
2. Coordinates must be [longitude, latitude] (GeoJSON order)
3. All dates must be ISO 8601 format (YYYY-MM-DD or full datetime)
4. Use null for unknown fields, not empty strings
5. severity.level: 1=minimal, 2=low, 3=moderate, 4=high, 5=severe/crisis
6. confidence.extraction: Your confidence in the overall extraction (0-1)
7. is_classroom_safe: False if contains graphic violence, animal death details, or disturbing content

EVENT TYPES (pick most appropriate):
- wildfire, deforestation, illegal_logging, poaching
- pollution, oil_spill, disease_outbreak, coral_bleaching
- invasive_species, habitat_loss, climate_impact
- policy_change, restoration, conservation_win, species_discovery
- other (only if nothing else fits)

SEVERITY GUIDELINES:
- 1: Minor, localized, quickly resolved
- 2: Limited scope, manageable impact
- 3: Regional concern, moderate damage
- 4: Significant damage, widespread impact
- 5: Major disaster, crisis-level, international concern`;
}

function buildUserPrompt(source: SourceRow, rawText: string, strict: boolean): string {
  const metadata = {
    url: source.url,
    publisher: source.publisher,
    title: source.title,
    published_at: source.publishedAt?.toISOString(),
    language: source.language,
  };

  return `Extract a conservation event from this source.

SOURCE METADATA:
${JSON.stringify(metadata, null, 2)}

SOURCE TEXT:
${rawText}

${strict ? `
IMPORTANT: Your previous response was invalid. This time:
- Include ALL required fields (schema_version, title, event_type, severity, confidence, location, temporal, summary, source_ref)
- schema_version MUST be exactly "event_extraction_v1"
- coordinates MUST be [longitude, latitude] if provided
- severity.level MUST be an integer 1-5
- Output ONLY the JSON object, nothing else` : ''}`;
}

// ============================================
// Utility Functions
// ============================================
function truncateText(text: string, maxChars: number): string {
  if (text.length <= maxChars) return text;
  return text.slice(0, maxChars) + "\n\n[TRUNCATED]";
}

function categorizePublisher(publisher: string): string {
  const lower = publisher.toLowerCase();
  if (lower.includes('gov') || lower.includes('department') || lower.includes('ministry')) {
    return 'gov';
  }
  if (lower.includes('wwf') || lower.includes('conservation') || lower.includes('wildlife')) {
    return 'ngo';
  }
  if (lower.includes('university') || lower.includes('journal') || lower.includes('science')) {
    return 'academic';
  }
  return 'news';
}

function shouldMarkEpisodeEligible(extraction: any, qualityScore: number): boolean {
  return (
    extraction.severity.level >= 4 &&
    extraction.confidence.extraction >= 0.75 &&
    extraction.classification?.is_classroom_safe === true &&
    qualityScore >= 0.6
  );
}

export { validateEventSchema };
