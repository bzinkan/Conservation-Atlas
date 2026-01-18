// packages/api/src/index.ts
import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { Pool } from 'pg';
import dotenv from 'dotenv';

import { registerPlaceBriefRoute } from './routes/placeBrief.route';
import { logger } from './utils/logger';

dotenv.config();

const app = express();
const port = process.env.PORT || 3000;

// Middleware
app.use(helmet());
app.use(cors());
app.use(express.json());

// Database connection
const pg = new Pool({
  connectionString: process.env.DATABASE_URL,
});

// Health check
app.get('/health', (req, res) => {
  res.json({ status: 'ok', timestamp: new Date().toISOString() });
});

// Register routes
const router = express.Router();
registerPlaceBriefRoute(router, { pg });
app.use('/api', router);

// Error handler
app.use((err: Error, req: express.Request, res: express.Response, next: express.NextFunction) => {
  logger.error({ error: err.message, stack: err.stack }, 'Unhandled error');
  res.status(500).json({ error: 'Internal server error' });
});

// Start server
app.listen(port, () => {
  logger.info({ port }, 'API server started');
});

export default app;
