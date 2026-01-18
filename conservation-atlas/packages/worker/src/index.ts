// packages/worker/src/index.ts
import dotenv from 'dotenv';
import { Pool } from 'pg';
import { createLogger } from 'winston';

import { createQueueConsumer } from './consumer';
import { SQSService } from './sqsService';

dotenv.config();

const logger = createLogger({
  // Configure as needed
});

async function main() {
  const pg = new Pool({
    connectionString: process.env.DATABASE_URL,
  });

  const sqsService = new SQSService({
    region: process.env.AWS_REGION || 'us-east-1',
  });

  // Define queue URLs
  const queues = {
    ingest: process.env.SQS_INGEST_QUEUE_URL!,
    extract: process.env.SQS_EXTRACT_QUEUE_URL!,
    video: process.env.SQS_VIDEO_QUEUE_URL!,
    alerts: process.env.SQS_ALERTS_QUEUE_URL!,
  };

  // Create and start consumer
  const consumer = createQueueConsumer({
    pg,
    sqsService,
    queues,
    logger,
  });

  // Handle shutdown
  process.on('SIGTERM', async () => {
    logger.info('Received SIGTERM, shutting down...');
    await consumer.stop();
    await pg.end();
    process.exit(0);
  });

  process.on('SIGINT', async () => {
    logger.info('Received SIGINT, shutting down...');
    await consumer.stop();
    await pg.end();
    process.exit(0);
  });

  // Start consuming
  logger.info('Worker started, polling queues...');
  await consumer.start();
}

main().catch((err) => {
  console.error('Worker failed to start:', err);
  process.exit(1);
});
