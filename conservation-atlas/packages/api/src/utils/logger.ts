// packages/api/src/utils/logger.ts

import winston from 'winston';

const { combine, timestamp, printf, colorize, json } = winston.format;

// Custom format for development
const devFormat = printf(({ level, message, timestamp, ...metadata }) => {
  let msg = `${timestamp} [${level}]: ${message}`;
  
  if (Object.keys(metadata).length > 0) {
    msg += ` ${JSON.stringify(metadata)}`;
  }
  
  return msg;
});

// Determine log level from environment
const level = process.env.LOG_LEVEL || (process.env.NODE_ENV === 'production' ? 'info' : 'debug');

// Create logger instance
export const logger = winston.createLogger({
  level,
  format: combine(
    timestamp({ format: 'YYYY-MM-DD HH:mm:ss' }),
    process.env.NODE_ENV === 'production'
      ? json()
      : combine(colorize(), devFormat)
  ),
  defaultMeta: { service: 'conservation-atlas' },
  transports: [
    new winston.transports.Console(),
  ],
});

// Add file transports in production
if (process.env.NODE_ENV === 'production') {
  logger.add(new winston.transports.File({ 
    filename: 'logs/error.log', 
    level: 'error' 
  }));
  logger.add(new winston.transports.File({ 
    filename: 'logs/combined.log' 
  }));
}

// Create child logger for specific modules
export function createModuleLogger(moduleName: string) {
  return logger.child({ module: moduleName });
}

// Convenience methods for structured logging
export const log = {
  info: (msg: string, meta?: Record<string, any>) => logger.info(msg, meta),
  warn: (msg: string, meta?: Record<string, any>) => logger.warn(msg, meta),
  error: (msg: string, meta?: Record<string, any>) => logger.error(msg, meta),
  debug: (msg: string, meta?: Record<string, any>) => logger.debug(msg, meta),
  
  // Job-specific logging
  jobStart: (jobId: string, jobType: string, meta?: Record<string, any>) => {
    logger.info(`Job started: ${jobType}`, { jobId, jobType, ...meta });
  },
  
  jobComplete: (jobId: string, jobType: string, durationMs: number, meta?: Record<string, any>) => {
    logger.info(`Job completed: ${jobType}`, { jobId, jobType, durationMs, ...meta });
  },
  
  jobFailed: (jobId: string, jobType: string, error: Error, meta?: Record<string, any>) => {
    logger.error(`Job failed: ${jobType}`, { 
      jobId, 
      jobType, 
      error: error.message,
      stack: error.stack,
      ...meta 
    });
  },
};

export default logger;
