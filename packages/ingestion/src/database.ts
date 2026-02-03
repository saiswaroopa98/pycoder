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

      // Progress tracking table
      await client.query(`
        CREATE TABLE IF NOT EXISTS ingestion_progress (
          id INTEGER PRIMARY KEY DEFAULT 1,
          last_cursor TEXT,
          total_ingested INTEGER DEFAULT 0,
          last_updated TIMESTAMPTZ DEFAULT NOW(),
          CONSTRAINT single_row CHECK (id = 1)
        );
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

      // Batch insert - much faster than individual inserts
      const values: any[] = [];
      const placeholders: string[] = [];
      
      events.forEach((event, idx) => {
        const baseIdx = idx * 6;
        placeholders.push(
          `($${baseIdx + 1}, $${baseIdx + 2}, $${baseIdx + 3}, $${baseIdx + 4}, $${baseIdx + 5}, $${baseIdx + 6})`
        );
        
        const eventType = event.eventType || event.event_type || event.type || null;
        const userId = event.userId || event.user_id || null;
        const sessionId = event.sessionId || event.session_id || null;
        
        values.push(
          event.id,
          eventType,
          event.timestamp,
          userId,
          sessionId,
          JSON.stringify(event.properties || event)
        );
      });

      const query = `
        INSERT INTO ingested_events (id, event_type, timestamp, user_id, session_id, properties)
        VALUES ${placeholders.join(', ')}
        ON CONFLICT (id) DO NOTHING
      `;

      await client.query(query, values);
      await client.query('COMMIT');
    } catch (error) {
      await client.query('ROLLBACK');
      throw error;
    } finally {
      client.release();
    }
  }

  async saveProgress(cursor: string, totalIngested: number): Promise<void> {
    const client: PoolClient = await this.pool.connect();
    try {
      await client.query(
        `INSERT INTO ingestion_progress (id, last_cursor, total_ingested, last_updated)
         VALUES (1, $1, $2, NOW())
         ON CONFLICT (id) DO UPDATE 
         SET last_cursor = $1, total_ingested = $2, last_updated = NOW()`,
        [cursor, totalIngested]
      );
    } finally {
      client.release();
    }
  }

  async getProgress(): Promise<{ cursor: string | null; totalIngested: number } | null> {
    const client: PoolClient = await this.pool.connect();
    try {
      const result = await client.query(
        'SELECT last_cursor, total_ingested FROM ingestion_progress WHERE id = 1'
      );
      
      if (result.rows.length === 0) {
        return null;
      }

      return {
        cursor: result.rows[0].last_cursor,
        totalIngested: parseInt(result.rows[0].total_ingested, 10)
      };
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
    
    // Get count
    const countResult = await client.query('SELECT COUNT(*) as count FROM ingested_events');
    const totalCount = parseInt(countResult.rows[0].count, 10);
    logger.info({ totalCount }, 'Total events to export');
    
    // Export in batches to avoid memory issues
    const batchSize = 50000;
    const writeStream = fs.createWriteStream(outputPath, { flags: 'w' });
    
    let offset = 0;
    let exported = 0;
    
    while (offset < totalCount) {
      const result = await client.query(
        'SELECT id FROM ingested_events ORDER BY id LIMIT $1 OFFSET $2',
        [batchSize, offset]
      );
      
      for (let i = 0; i < result.rows.length; i++) {
        if (offset > 0 || i > 0) {
          writeStream.write('\n');
        }
        writeStream.write(result.rows[i].id);
      }
      
      exported += result.rows.length;
      offset += batchSize;
      
      logger.info({
        exported,
        total: totalCount,
        progress: `${((exported / totalCount) * 100).toFixed(1)}%`
      }, 'Export progress');
    }
    
    writeStream.end();
    
    // Wait for write to finish
    await new Promise<void>((resolve) => {
      writeStream.on('finish', () => resolve());
    });
    
    // Verify the export
    const fileContent = fs.readFileSync(outputPath, 'utf-8');
    const lineCount = fileContent.split('\n').filter((line: string) => line.trim()).length;
    
    logger.info({ 
      exported: lineCount,
      expected: totalCount,
      path: outputPath,
      fileSize: `${(fs.statSync(outputPath).size / 1024 / 1024).toFixed(2)} MB`,
      match: lineCount === totalCount ? 'YES ✓' : 'NO ✗'
    }, 'Event IDs exported');
    
    if (lineCount !== totalCount) {
      throw new Error(`Export verification failed: exported ${lineCount} but expected ${totalCount}`);
    }
    
  } finally {
    client.release();
  }
}

  async close(): Promise<void> {
    await this.pool.end();
  }
}