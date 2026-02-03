import { ApiClient } from './api-client';
import { Database } from './database';
import { logger } from './logger';

export class IngestionOrchestrator {
  private api: ApiClient;
  private db: Database;
  private totalEvents: number = 0;
  private totalRequests: number = 0;
  private startTime: number = Date.now();

  constructor(api: ApiClient, db: Database) {
    this.api = api;
    this.db = db;
  }

  private normalizeTimestamp(timestamp: any): string {
    if (!timestamp) return new Date().toISOString();
    
    if (typeof timestamp === 'string' && timestamp.includes('T')) {
      return timestamp;
    }
    
    const ts = typeof timestamp === 'string' ? parseInt(timestamp, 10) : timestamp;
    
    if (ts > 1e12) {
      return new Date(ts).toISOString();
    } else {
      return new Date(ts * 1000).toISOString();
    }
  }

  private normalizeEvents(events: any[]): any[] {
    return events.map((event: any) => {
      const normalized = {
        id: event.id,
        timestamp: this.normalizeTimestamp(event.timestamp),
        eventType: event.eventType || event.event_type || event.type || null,
        userId: event.userId || event.user_id || null,
        sessionId: event.sessionId || event.session_id || null,
        properties: event.properties || {}
      };
      
      return normalized;
    });
  }

  async start(): Promise<void> {
    logger.info('Starting ingestion...');

    // Check for existing progress
    const savedProgress = await this.db.getProgress();
    let cursor: string | undefined = savedProgress?.cursor || undefined;
    let iterationCount = 0;
    const batchSize = 1000;

    if (savedProgress) {
      this.totalEvents = savedProgress.totalIngested;
      logger.info({
        resumingFrom: cursor,
        eventsAlreadyIngested: this.totalEvents
      }, 'Resuming from saved progress');
    }

    while (true) {
      try {
        iterationCount++;
        
        logger.info({
          iteration: iterationCount,
          cursor: cursor || 'initial',
          totalEventsSoFar: this.totalEvents
        }, 'Fetching batch...');

        const response = await this.api.fetchEvents(cursor, batchSize);
        this.totalRequests++;

        if (!response.data || response.data.length === 0) {
          logger.info('No more events received, stopping');
          break;
        }

        logger.info({
          iteration: iterationCount,
          eventsInBatch: response.data.length,
          totalEventsSoFar: this.totalEvents + response.data.length,
        }, 'Batch received');

        const normalizedEvents = this.normalizeEvents(response.data);
        await this.db.saveEvents(normalizedEvents);
        this.totalEvents += normalizedEvents.length;

        // Get the next cursor from pagination object
        const nextCursor = response.pagination?.nextCursor || response.nextCursor;
        
        if (nextCursor) {
          cursor = nextCursor;
          // Save progress after each batch
          await this.db.saveProgress(cursor, this.totalEvents);
          logger.info({ cursor, totalEvents: this.totalEvents }, 'Progress saved');
        }

        // Check if there's more data
        const hasMore = response.pagination?.hasMore || response.hasMore;
        
        if (!hasMore) {
          logger.info('API indicates no more data');
          break;
        }

        // If we got fewer events than requested, we might be done
        if (response.data.length < batchSize && !hasMore) {
          logger.info({
            received: response.data.length,
            expected: batchSize
          }, 'Received fewer events and hasMore is false, stopping');
          break;
        }

        // Progress logging
        if (this.totalRequests % 10 === 0) {
          const elapsed = (Date.now() - this.startTime) / 1000;
          logger.info({
            totalEvents: this.totalEvents,
            totalRequests: this.totalRequests,
            eventsPerSecond: (this.totalEvents / elapsed).toFixed(2),
          }, 'Progress update');
        }

        // Delay between requests to respect rate limits
        await new Promise(resolve => setTimeout(resolve, 200));

      } catch (error: any) {
        logger.error({ 
          error: error.message,
          iteration: iterationCount,
          cursor: cursor,
          totalEventsSoFar: this.totalEvents
        }, 'Error during ingestion - progress has been saved');
        throw error;
      }
    }

    logger.info('ingestion complete');
    
    const finalCount = await this.db.getEventCount();
    const totalTime = (Date.now() - this.startTime) / 1000;
    
    logger.info({
      totalEvents: finalCount,
      totalRequests: this.totalRequests,
      iterations: iterationCount,
      totalTimeSeconds: totalTime.toFixed(2),
      totalTimeMinutes: (totalTime / 60).toFixed(2),
      eventsPerSecond: (finalCount / totalTime).toFixed(2),
    }, 'Ingestion completed successfully');
  }
}