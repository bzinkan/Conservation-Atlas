// packages/shared/src/types/queueMessages.ts

export const JOB_TYPES = [
  "INGEST",
  "EXTRACT",
  "CLUSTER",
  "GEO_JOIN",
  "ALERTS",
  "VIDEO_BRIEF",
  "CLASSROOM_EPISODE",
  "SMOKE_TEST",
] as const;

export type JobType = (typeof JOB_TYPES)[number];

export type ISODateTime = string;

/**
 * Base envelope carried in every SQS message.
 * Keep it small. Put big payloads in DB/S3 and reference IDs here.
 */
export interface BaseJobMessage {
  version: "job_v1";
  job_type: JobType;

  /** Unique identifier for this job message (use ULID/UUID). */
  job_id: string;

  /** For tracing across pipeline stages. */
  correlation_id: string;

  /** When this job was enqueued. */
  enqueued_at: ISODateTime;

  /** Optional: attempt count managed by your worker logic. */
  attempt?: number;

  /** Optional: caller/service metadata. */
  meta?: Record<string, string | number | boolean | null>;
}

// ============================================
// INGEST: Pull new items for a source group
// ============================================
export interface IngestJobMessage extends BaseJobMessage {
  job_type: "INGEST";
  payload: {
    source_group: "global" | "pilot" | string;
    max_items?: number;
  };
}

// ============================================
// EXTRACT: Run LLM extraction for a source
// ============================================
export interface ExtractJobMessage extends BaseJobMessage {
  job_type: "EXTRACT";
  payload: {
    source_id: number;              // DB id for the ingested source record
    force_reextract?: boolean;      // ignore existing extraction if true
    model_hint?: "openai" | "anthropic" | "gemini"; // optional routing
  };
}

// ============================================
// CLUSTER: Dedupe/merge events
// ============================================
export interface ClusterJobMessage extends BaseJobMessage {
  job_type: "CLUSTER";
  payload: {
    event_id?: number;              // cluster around a specific event
    window_hours?: number;          // or cluster recent events
  };
}

// ============================================
// GEO_JOIN: Compute protected area overlap
// ============================================
export interface GeoJoinJobMessage extends BaseJobMessage {
  job_type: "GEO_JOIN";
  payload: {
    event_id: number;
  };
}

// ============================================
// ALERTS: Dispatch alerts for events/digests
// ============================================
export interface AlertsJobMessage extends BaseJobMessage {
  job_type: "ALERTS";
  payload: {
    mode: "event" | "digest_daily" | "digest_weekly";
    event_id?: number;
    digest_date?: string; // YYYY-MM-DD
  };
}

// ============================================
// VIDEO_BRIEF: Generate 30-90s briefing video
// ============================================
export interface VideoBriefJobMessage extends BaseJobMessage {
  job_type: "VIDEO_BRIEF";
  payload: {
    event_id: number;
    runtime_seconds?: number; // default 60
  };
}

// ============================================
// CLASSROOM_EPISODE: Generate weekly episode
// ============================================
export interface ClassroomEpisodeJobMessage extends BaseJobMessage {
  job_type: "CLASSROOM_EPISODE";
  payload: {
    grade_band: "K_2" | "3_5" | "6_8" | "9_12";
    window_days?: number; // default 7
    region_scope?: "global" | "us" | string;
  };
}

// ============================================
// SMOKE_TEST: Pipeline health checks
// ============================================
export interface SmokeTestJobMessage extends BaseJobMessage {
  job_type: "SMOKE_TEST";
  payload: {
    checks?: Array<
      | "scraperbee"
      | "openai_json"
      | "db_rw"
      | "s3_rw"
      | "pictory_auth"
      | "maps_static"
    >;
  };
}

// ============================================
// Union type for all job messages
// ============================================
export type JobMessage =
  | IngestJobMessage
  | ExtractJobMessage
  | ClusterJobMessage
  | GeoJoinJobMessage
  | AlertsJobMessage
  | VideoBriefJobMessage
  | ClassroomEpisodeJobMessage
  | SmokeTestJobMessage;

/**
 * Type guard for validating job messages
 */
export function isJobMessage(x: unknown): x is JobMessage {
  if (!x || typeof x !== 'object') return false;
  const msg = x as Record<string, unknown>;
  return (
    msg.version === "job_v1" &&
    typeof msg.job_type === "string" &&
    JOB_TYPES.includes(msg.job_type as JobType) &&
    typeof msg.job_id === "string" &&
    typeof msg.correlation_id === "string" &&
    typeof msg.enqueued_at === "string" &&
    msg.payload != null
  );
}

/**
 * Helper to create a new job message with defaults
 */
export function createJobMessage<T extends JobMessage>(
  jobType: T['job_type'],
  payload: T['payload'],
  correlationId?: string
): T {
  return {
    version: "job_v1",
    job_type: jobType,
    job_id: generateJobId(),
    correlation_id: correlationId ?? generateCorrelationId(),
    enqueued_at: new Date().toISOString(),
    payload,
  } as T;
}

function generateJobId(): string {
  return `job_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}

function generateCorrelationId(): string {
  return `corr_${Date.now().toString(36)}_${Math.random().toString(36).slice(2, 10)}`;
}
