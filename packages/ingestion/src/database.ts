import { Pool, PoolClient } from 'pg';
import { logger } from './logger';
import * as fs from 'fs';

export class Database {
  private pool: Pool;

  constructor(connectionString: string) {
    this.pool = new Pool({
      connectionString,
      max: 20,
      idleTimeoutMillis: 30000,
      connectionTimeoutMillis: 10000,
    });

    this.pool.on('error', (err: Error) => {
      logger.error({ err }, 'Unexpected database pool error');
    });
  }

  async initialize(): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      logger.info('Initializing database schema...');
      
      await client.query(`
        CREATE TABLE IF NOT EXISTS ingested_events (
          id TEXT PRIMARY KEY,
          event_type TEXT,
          timestamp TIMESTAMPTZ NOT NULL,
          user_id TEXT,
          session_id TEXT,
          properties JSONB,
          created_at TIMESTAMPTZ DEFAULT NOW()
        );
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_events_timestamp 
        ON ingested_events(timestamp);
      `);

      await client.query(`
        CREATE INDEX IF NOT EXISTS idx_events_type 
        ON ingested_events(event_type);
      `);

      logger.info('Database schema initialized');
    } finally {
      client.release();
    }
  }

  async saveEvents(events: any[]): Promise<void> {
    if (events.length === 0) return;

    const client: PoolClient = await this.pool.connect();
    try {
      await client.query('BEGIN');

      for (const event of events) {
        const eventType = event.eventType || event.event_type || event.type || null;
        const userId = event.userId || event.user_id || null;
        const sessionId = event.sessionId || event.session_id || null;
        
        await client.query(
          `INSERT INTO ingested_events (id, event_type, timestamp, user_id, session_id, properties)
           VALUES ($1, $2, $3, $4, $5, $6)
           ON CONFLICT (id) DO NOTHING`,
          [
            event.id,
            eventType,
            event.timestamp,
            userId,
            sessionId,
            JSON.stringify(event.properties || event)
          ]
        );
      }

      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async getEventCount(): Promise<number> {
    const client: PoolClient = await this.pool.connect();
    try {
      const result = await client.query('SELECT COUNT(*) as count FROM ingested_events');
      return parseInt(result.rows[0].count, 10);
    } finally {
      client.release();
    }
  }

  async exportEventIds(outputPath: string): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      logger.info('Exporting event IDs...');
      const result = await client.query('SELECT id FROM ingested_events ORDER BY id');
      
      const ids = result.rows.map((row: any) => row.id).join('\n');
      fs.writeFileSync(outputPath, ids);
      
      logger.info({ count: result.rows.length, path: outputPath }, 'Event IDs exported');
    } finally {
      client.release();
    }
  }

  async close(): Promise<void> {
    await this.pool.end();
  }
}