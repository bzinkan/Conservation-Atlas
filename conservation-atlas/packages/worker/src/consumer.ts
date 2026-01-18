// packages/api/src/workers/consumer.ts
//
// Production-friendly SQS consumer loop:
// - Receives messages from configured queues
// - Routes to the correct job handler
// - Deletes on success
// - Requeues (via visibility timeout) on retryable failure
// - Lets SQS redrive policy send to DLQ after maxReceiveCount
// - Deletes poison messages (invalid JSON/envelope)

import type { JobMessage } from "@conservation-atlas/shared/types/queueMessages";
import { isJobMessage } from "@conservation-atlas/shared/types/queueMessages";

import {
  receiveMessages,
  deleteMessage,
  requeueSoon,
  handlePoisonMessage,
  type QueueName,
} from "../services/queue/sqsService";

import { logger, log } from "../utils/logger";

// Job handlers
import { runExtractJob } from "../jobs/extract.job";
// import { runIngestJob } from "../jobs/ingest.job";
// import { runClusterJob } from "../jobs/cluster.job";
// import { runGeoJoinJob } from "../jobs/geojoin.job";
// import { runAlertsJob } from "../jobs/alerts.job";
// import { runVideoBriefJob } from "../jobs/videoBrief.job";
// import { runClassroomEpisodeJob } from "../jobs/classroomEpisode.job";
// import { runSmokeTestJob } from "../jobs/smokeTest.job";

// ============================================
// Configuration
// ============================================

/**
 * Which queues this worker should consume.
 * Run multiple task definitions for separate worker pools per queue.
 */
const QUEUES_TO_CONSUME: QueueName[] = [
  "q_ingest",
  "q_extract",
  "q_cluster",
  "q_geojoin",
  "q_alerts",
  "q_video",
  "q_classroom",
  "q_smoketest",
];

// SQS long polling settings
const MAX_MESSAGES_PER_POLL = Number(process.env.WORKER_MAX_MESSAGES ?? 5);
const WAIT_SECONDS = Number(process.env.WORKER_WAIT_SECONDS ?? 20);
const VISIBILITY_TIMEOUT_SECONDS = Number(process.env.WORKER_VISIBILITY_TIMEOUT ?? 120);

// Retry policy (app-level). DLQ is handled by SQS redrive policy.
const MAX_APP_RETRIES = Number(process.env.WORKER_MAX_APP_RETRIES ?? 2);
const BASE_RETRY_DELAY_SECONDS = Number(process.env.WORKER_RETRY_BASE_DELAY ?? 30);
const SHUTDOWN_GRACE_MS = Number(process.env.WORKER_SHUTDOWN_GRACE_MS ?? 10_000);

// Worker state
let shuttingDown = false;
let activeJobs = 0;

// ============================================
// Handler Registry
// ============================================

type Handler = (msg: any) => Promise<void>;

// Placeholder handlers for jobs not yet implemented
const notImplementedHandler = async (msg: JobMessage) => {
  logger.warn({ job_type: msg.job_type }, "Job handler not yet implemented");
};

const handlers: Record<JobMessage["job_type"], Handler> = {
  INGEST: notImplementedHandler,
  EXTRACT: runExtractJob as Handler,
  CLUSTER: notImplementedHandler,
  GEO_JOIN: notImplementedHandler,
  ALERTS: notImplementedHandler,
  VIDEO_BRIEF: notImplementedHandler,
  CLASSROOM_EPISODE: notImplementedHandler,
  SMOKE_TEST: notImplementedHandler,
};

// ============================================
// Retry Logic
// ============================================

function computeBackoffSeconds(attempt: number): number {
  // Exponential backoff with jitter, capped at 15 minutes
  const base = BASE_RETRY_DELAY_SECONDS * Math.pow(2, Math.max(0, attempt - 1));
  const jitter = Math.random() * 0.2 * base; // 0-20% jitter
  return Math.min(base + jitter, 15 * 60);
}

function isRetryableError(err: unknown): boolean {
  const msg = (err as any)?.message?.toLowerCase?.() ?? "";
  const code = (err as any)?.code ?? (err as any)?.name ?? "";

  // Rate limits and throttling
  if (msg.includes("rate limit") || msg.includes("throttl")) return true;
  if (String(code).includes("Throttl")) return true;
  
  // Timeouts and network errors
  if (msg.includes("timeout") || msg.includes("timed out")) return true;
  if (msg.includes("econnreset") || msg.includes("econnrefused")) return true;
  if (String(code).includes("ECONNRESET") || String(code).includes("ETIMEDOUT")) return true;
  
  // Temporary/transient errors
  if (msg.includes("temporar") || msg.includes("try again")) return true;
  
  // 5xx errors from APIs
  if (msg.includes("500") || msg.includes("502") || msg.includes("503") || msg.includes("504")) return true;
  
  // OpenAI specific
  if (msg.includes("overloaded") || msg.includes("capacity")) return true;
  
  // Validation errors are NOT retryable
  if (msg.includes("validation") || msg.includes("invalid")) return false;
  
  // Default: treat unknown errors as retryable once
  return true;
}

// ============================================
// Message Processing
// ============================================

async function handleMessage(
  queue: QueueName, 
  receiptHandle: string, 
  rawBody: string, 
  parsed?: JobMessage,
  receiveCount?: number
): Promise<void> {
  // Poison message handling (invalid JSON or wrong envelope)
  if (!parsed) {
    await handlePoisonMessage(queue, receiptHandle, rawBody);
    return;
  }

  const handler = handlers[parsed.job_type];
  if (!handler) {
    logger.error({ job_type: parsed.job_type }, "Unknown job type; deleting message");
    await deleteMessage(queue, receiptHandle);
    return;
  }

  const attempt = receiveCount ?? (parsed.attempt ?? 0) + 1;
  const startTime = Date.now();

  try {
    activeJobs++;
    
    log.jobStart(parsed.job_id, parsed.job_type, {
      queue,
      attempt,
      correlation_id: parsed.correlation_id,
    });

    // Run handler
    await handler(parsed);

    // Success => delete message
    await deleteMessage(queue, receiptHandle);

    const durationMs = Date.now() - startTime;
    log.jobComplete(parsed.job_id, parsed.job_type, durationMs, {
      queue,
      attempt,
    });

  } catch (err) {
    const durationMs = Date.now() - startTime;
    const error = err as Error;
    const retryable = isRetryableError(error);

    log.jobFailed(parsed.job_id, parsed.job_type, error, {
      queue,
      attempt,
      retryable,
      durationMs,
    });

    if (!retryable) {
      // Non-retryable errors: delete immediately
      // Could also send to a "manual-review" queue
      logger.warn({ job_id: parsed.job_id }, "Non-retryable error; deleting message");
      await deleteMessage(queue, receiptHandle);
      return;
    }

    if (attempt >= MAX_APP_RETRIES) {
      // Let SQS handle redrive to DLQ via maxReceiveCount
      // Make visible soon so it can be retried by SQS
      const delay = computeBackoffSeconds(attempt);
      logger.warn({ job_id: parsed.job_id, delay }, "Max retries reached; letting SQS handle");
      await requeueSoon(queue, receiptHandle, delay);
      return;
    }

    // App-level retry: change visibility timeout
    const delay = computeBackoffSeconds(attempt);
    logger.info({ job_id: parsed.job_id, delay, attempt }, "Scheduling retry");
    await requeueSoon(queue, receiptHandle, delay);
    
  } finally {
    activeJobs--;
  }
}

async function pollQueue(queue: QueueName): Promise<void> {
  const messages = await receiveMessages(queue, {
    maxMessages: MAX_MESSAGES_PER_POLL,
    waitSeconds: WAIT_SECONDS,
    visibilityTimeoutSeconds: VISIBILITY_TIMEOUT_SECONDS,
  });

  if (!messages.length) return;

  logger.debug({ queue, count: messages.length }, "Received messages");

  // Process sequentially for simplicity
  // For parallelization, use a concurrency limiter like p-limit
  for (const m of messages) {
    if (shuttingDown) {
      logger.info("Shutdown requested; stopping message processing");
      return;
    }

    await handleMessage(
      queue, 
      m.receiptHandle, 
      m.rawBody, 
      m.parsed,
      m.approximateReceiveCount
    );
  }
}

// ============================================
// Main Loop
// ============================================

async function mainLoop(): Promise<void> {
  logger.info({ 
    queues: QUEUES_TO_CONSUME,
    maxMessages: MAX_MESSAGES_PER_POLL,
    waitSeconds: WAIT_SECONDS,
    visibilityTimeout: VISIBILITY_TIMEOUT_SECONDS,
  }, "Worker consumer started");

  while (!shuttingDown) {
    // Round-robin polling across queues
    // For priority queues, reorder QUEUES_TO_CONSUME
    for (const q of QUEUES_TO_CONSUME) {
      if (shuttingDown) break;

      try {
        await pollQueue(q);
      } catch (err) {
        logger.error({ 
          queue: q, 
          error: (err as Error).message 
        }, "Queue poll error");
        
        // Brief pause to avoid tight error loop
        await sleep(1000);
      }
    }
  }

  // Wait for active jobs to complete
  if (activeJobs > 0) {
    logger.info({ activeJobs }, "Waiting for active jobs to complete");
    const deadline = Date.now() + SHUTDOWN_GRACE_MS;
    
    while (activeJobs > 0 && Date.now() < deadline) {
      await sleep(100);
    }
    
    if (activeJobs > 0) {
      logger.warn({ activeJobs }, "Shutdown deadline reached with active jobs");
    }
  }

  logger.info("Worker consumer shut down complete");
}

function sleep(ms: number): Promise<void> {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

// ============================================
// Graceful Shutdown
// ============================================

function handleShutdown(signal: string): void {
  logger.warn({ signal }, "Shutdown signal received");
  shuttingDown = true;
  
  // Give time for graceful shutdown
  setTimeout(() => {
    logger.error("Forced shutdown after grace period");
    process.exit(1);
  }, SHUTDOWN_GRACE_MS + 5000).unref();
}

process.on("SIGTERM", () => handleShutdown("SIGTERM"));
process.on("SIGINT", () => handleShutdown("SIGINT"));

// ============================================
// Exports
// ============================================

export { mainLoop, shuttingDown };

// Start consumer if run directly
if (require.main === module) {
  mainLoop().catch((err) => {
    logger.error({ error: (err as Error).message }, "Fatal worker error");
    process.exit(1);
  });
}
