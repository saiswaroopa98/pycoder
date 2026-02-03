import { ApiClient } from './api-client';
import { Database } from './database';
import { IngestionOrchestrator } from './orchestrator';
import { Submitter } from './submitter';
import { logger } from './logger';
import * as dotenv from 'dotenv';

dotenv.config();

const config = {
  databaseUrl: process.env.DATABASE_URL || 'postgresql://postgres:postgres@postgres:5432/ingestion',
  apiBaseUrl: process.env.API_BASE_URL || '',
  apiKey: process.env.API_KEY || '',
  githubRepo: process.env.GITHUB_REPO_URL || '',
};

async function main(): Promise<void> {
  logger.info('Starting ingestion service');

  if (!config.apiKey) {
    logger.error('API_KEY is required');
    process.exit(1);
  }

  const db = new Database(config.databaseUrl);
  
  let retries = 30;
  while (retries > 0) {
    try {
      await db.initialize();
      break;
    } catch (error) {
      retries--;
      logger.warn({ retriesLeft: retries }, 'Waiting for database...');
      await new Promise(resolve => setTimeout(resolve, 2000));
    }
  }

  if (retries === 0) {
    logger.error('Failed to connect to database');
    process.exit(1);
  }

  const api = new ApiClient(config.apiBaseUrl, config.apiKey);
  const orchestrator = new IngestionOrchestrator(api, db);

  try {
    // Run ingestion
    await orchestrator.start();
    
    // Export event IDs
    const outputPath = '/app/output/event_ids.txt';
    await db.exportEventIds(outputPath);
    logger.info({ path: outputPath }, 'Event IDs exported');
    
    // Submit if GitHub repo URL is provided
    if (config.githubRepo) {
      const submitter = new Submitter(config.apiBaseUrl, config.apiKey, config.githubRepo);
      await submitter.submit(outputPath);
    } else {
      logger.warn('GITHUB_REPO_URL not set, skipping submission');
      logger.info('To submit manually, run:');
      logger.info(`curl -X POST -H "X-API-Key: ${config.apiKey}" -H "Content-Type: text/plain" --data-binary @output/event_ids.txt "${config.apiBaseUrl}/api/v1/submissions?github_repo=YOUR_GITHUB_URL"`);
    }
    
  } catch (error: any) {
    logger.error({ error: error.message }, 'Fatal error');
    process.exit(1);
  } finally {
    await db.close();
  }
}

main();