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

    let cursor: string | undefined = undefined;
    let hasMore: boolean = true;

    while (hasMore) {
      try {
        const response = await this.api.fetchEvents(cursor, 1000);
        this.totalRequests++;

        if (response.data && response.data.length > 0) {
          const normalizedEvents = this.normalizeEvents(response.data);
          await this.db.saveEvents(normalizedEvents);
          this.totalEvents += normalizedEvents.length;
        }

        cursor = response.nextCursor;
        hasMore = response.hasMore;

        if (this.totalRequests % 10 === 0) {
          const elapsed = (Date.now() - this.startTime) / 1000;
          logger.info({
            totalEvents: this.totalEvents,
            totalRequests: this.totalRequests,
            eventsPerSecond: (this.totalEvents / elapsed).toFixed(2),
          }, 'Progress update');
        }

        await new Promise(resolve => setTimeout(resolve, 50));

      } catch (error: any) {
        logger.error({ error: error.message }, 'Error during ingestion');
        throw error;
      }
    }

    logger.info('ingestion complete');
    
    const finalCount = await this.db.getEventCount();
    const totalTime = (Date.now() - this.startTime) / 1000;
    
    logger.info({
      totalEvents: finalCount,
      totalRequests: this.totalRequests,
      totalTimeSeconds: totalTime.toFixed(2),
      eventsPerSecond: (finalCount / totalTime).toFixed(2),
    }, 'Ingestion completed');
  }
}