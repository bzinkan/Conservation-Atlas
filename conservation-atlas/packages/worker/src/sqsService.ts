// packages/api/src/services/queue/sqsService.ts
//
// SQS wrapper with typed routing for all pipeline queues

import {
  SQSClient,
  SendMessageCommand,
  SendMessageCommandInput,
  ReceiveMessageCommand,
  DeleteMessageCommand,
  ChangeMessageVisibilityCommand,
} from "@aws-sdk/client-sqs";

import type { JobMessage, JobType } from "@conservation-atlas/shared/types/queueMessages";
import { isJobMessage } from "@conservation-atlas/shared/types/queueMessages";
import { logger } from "../../utils/logger";

// ============================================
// Types
// ============================================
export type QueueName =
  | "q_ingest"
  | "q_extract"
  | "q_cluster"
  | "q_geojoin"
  | "q_alerts"
  | "q_video"
  | "q_classroom"
  | "q_smoketest";

type QueueConfig = Record<QueueName, { url: string }>;

// ============================================
// Configuration
// ============================================
const region = process.env.AWS_REGION || "us-east-1";
const sqs = new SQSClient({ region });

function getEnvOrDefault(name: string, defaultValue?: string): string {
  const v = process.env[name];
  if (!v) {
    if (defaultValue !== undefined) return defaultValue;
    throw new Error(`Missing env var: ${name}`);
  }
  return v;
}

/**
 * Map logical queue names -> URLs
 * Set these in your ECS task environment or .env file
 */
export const QUEUES: QueueConfig = {
  q_ingest: { url: getEnvOrDefault("SQS_QUEUE_URL_INGEST", "http://localhost:4566/000000000000/ingest") },
  q_extract: { url: getEnvOrDefault("SQS_QUEUE_URL_EXTRACT", "http://localhost:4566/000000000000/extract") },
  q_cluster: { url: getEnvOrDefault("SQS_QUEUE_URL_CLUSTER", "http://localhost:4566/000000000000/cluster") },
  q_geojoin: { url: getEnvOrDefault("SQS_QUEUE_URL_GEOJOIN", "http://localhost:4566/000000000000/geojoin") },
  q_alerts: { url: getEnvOrDefault("SQS_QUEUE_URL_ALERTS", "http://localhost:4566/000000000000/alerts") },
  q_video: { url: getEnvOrDefault("SQS_QUEUE_URL_VIDEO", "http://localhost:4566/000000000000/video") },
  q_classroom: { url: getEnvOrDefault("SQS_QUEUE_URL_CLASSROOM", "http://localhost:4566/000000000000/classroom") },
  q_smoketest: { url: getEnvOrDefault("SQS_QUEUE_URL_SMOKETEST", "http://localhost:4566/000000000000/smoketest") },
};

/**
 * Typed routing: for each JobType, which queue should it go to?
 */
export const ROUTE_BY_JOBTYPE: Record<JobType, QueueName> = {
  INGEST: "q_ingest",
  EXTRACT: "q_extract",
  CLUSTER: "q_cluster",
  GEO_JOIN: "q_geojoin",
  ALERTS: "q_alerts",
  VIDEO_BRIEF: "q_video",
  CLASSROOM_EPISODE: "q_classroom",
  SMOKE_TEST: "q_smoketest",
};

// ============================================
// Send Messages
// ============================================

/**
 * Enqueue a JobMessage onto a specific queue
 */
export async function enqueueJob(queue: QueueName, msg: JobMessage): Promise<string> {
  const body = JSON.stringify(msg);

  const input: SendMessageCommandInput = {
    QueueUrl: QUEUES[queue].url,
    MessageBody: body,
    MessageAttributes: {
      job_type: { DataType: "String", StringValue: msg.job_type },
      correlation_id: { DataType: "String", StringValue: msg.correlation_id },
    },
  };

  const result = await sqs.send(new SendMessageCommand(input));
  
  logger.debug({
    queue,
    job_type: msg.job_type,
    job_id: msg.job_id,
    message_id: result.MessageId,
  }, "Message enqueued");

  return result.MessageId ?? msg.job_id;
}

/**
 * Enqueue by automatic routing based on job_type
 */
export async function enqueueRouted(msg: JobMessage): Promise<string> {
  const queue = ROUTE_BY_JOBTYPE[msg.job_type];
  return enqueueJob(queue, msg);
}

/**
 * Enqueue with delay (for scheduled retries)
 */
export async function enqueueDelayed(
  queue: QueueName, 
  msg: JobMessage, 
  delaySeconds: number
): Promise<string> {
  const body = JSON.stringify(msg);

  const input: SendMessageCommandInput = {
    QueueUrl: QUEUES[queue].url,
    MessageBody: body,
    DelaySeconds: Math.min(delaySeconds, 900), // Max 15 minutes
    MessageAttributes: {
      job_type: { DataType: "String", StringValue: msg.job_type },
      correlation_id: { DataType: "String", StringValue: msg.correlation_id },
    },
  };

  const result = await sqs.send(new SendMessageCommand(input));
  return result.MessageId ?? msg.job_id;
}

// ============================================
// Receive Messages
// ============================================

export interface ReceivedMessage {
  receiptHandle: string;
  rawBody: string;
  parsed?: JobMessage;
  messageId?: string;
  approximateReceiveCount?: number;
}

/**
 * Receive up to max messages from a queue (long polling)
 */
export async function receiveMessages(
  queue: QueueName, 
  opts?: {
    maxMessages?: number;           // 1..10
    waitSeconds?: number;           // 0..20 (long poll)
    visibilityTimeoutSeconds?: number;
  }
): Promise<ReceivedMessage[]> {
  const maxMessages = opts?.maxMessages ?? 5;
  const waitSeconds = opts?.waitSeconds ?? 20;
  const visibilityTimeoutSeconds = opts?.visibilityTimeoutSeconds ?? 60;

  const resp = await sqs.send(
    new ReceiveMessageCommand({
      QueueUrl: QUEUES[queue].url,
      MaxNumberOfMessages: maxMessages,
      WaitTimeSeconds: waitSeconds,
      VisibilityTimeout: visibilityTimeoutSeconds,
      MessageAttributeNames: ["All"],
      AttributeNames: ["ApproximateReceiveCount"],
    })
  );

  const out: ReceivedMessage[] = [];

  for (const m of resp.Messages ?? []) {
    if (!m.ReceiptHandle || !m.Body) continue;

    let parsed: JobMessage | undefined;
    try {
      const candidate = JSON.parse(m.Body);
      if (isJobMessage(candidate)) parsed = candidate;
    } catch {
      // Leave parsed undefined; will be treated as poison message
    }

    out.push({
      receiptHandle: m.ReceiptHandle,
      rawBody: m.Body,
      parsed,
      messageId: m.MessageId,
      approximateReceiveCount: m.Attributes?.ApproximateReceiveCount 
        ? parseInt(m.Attributes.ApproximateReceiveCount) 
        : undefined,
    });
  }

  return out;
}

// ============================================
// Delete / Visibility Management
// ============================================

/**
 * Delete a message after successful processing
 */
export async function deleteMessage(queue: QueueName, receiptHandle: string): Promise<void> {
  await sqs.send(
    new DeleteMessageCommand({
      QueueUrl: QUEUES[queue].url,
      ReceiptHandle: receiptHandle,
    })
  );
}

/**
 * Change visibility timeout for retry
 */
export async function requeueSoon(
  queue: QueueName, 
  receiptHandle: string, 
  delaySeconds: number
): Promise<void> {
  await sqs.send(
    new ChangeMessageVisibilityCommand({
      QueueUrl: QUEUES[queue].url,
      ReceiptHandle: receiptHandle,
      VisibilityTimeout: Math.max(0, Math.min(delaySeconds, 12 * 60 * 60)), // Cap at 12h
    })
  );
}

/**
 * Handle poison messages (bad JSON / invalid envelope)
 * Best practice: log and delete to avoid infinite loop
 * Your DLQ policy will catch repeated failures for valid messages
 */
export async function handlePoisonMessage(
  queue: QueueName, 
  receiptHandle: string, 
  rawBody: string
): Promise<void> {
  logger.error({ queue, rawBody: rawBody.slice(0, 500) }, "Poison SQS message (invalid JSON/envelope); deleting");
  await deleteMessage(queue, receiptHandle);
}

// ============================================
// Batch Operations
// ============================================

/**
 * Send multiple messages in a batch (up to 10)
 */
export async function enqueueBatch(
  queue: QueueName, 
  messages: JobMessage[]
): Promise<{ successful: string[]; failed: string[] }> {
  const { SendMessageBatchCommand } = await import("@aws-sdk/client-sqs");
  
  const successful: string[] = [];
  const failed: string[] = [];
  
  // Process in chunks of 10 (SQS limit)
  for (let i = 0; i < messages.length; i += 10) {
    const batch = messages.slice(i, i + 10);
    
    const result = await sqs.send(new SendMessageBatchCommand({
      QueueUrl: QUEUES[queue].url,
      Entries: batch.map((msg, idx) => ({
        Id: `${i + idx}`,
        MessageBody: JSON.stringify(msg),
        MessageAttributes: {
          job_type: { DataType: "String", StringValue: msg.job_type },
          correlation_id: { DataType: "String", StringValue: msg.correlation_id },
        },
      })),
    }));
    
    for (const s of result.Successful ?? []) {
      successful.push(batch[parseInt(s.Id!)].job_id);
    }
    
    for (const f of result.Failed ?? []) {
      failed.push(batch[parseInt(f.Id!)].job_id);
      logger.error({ error: f }, "Batch send failed for message");
    }
  }
  
  return { successful, failed };
}
