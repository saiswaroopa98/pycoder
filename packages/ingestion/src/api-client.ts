import axios, { AxiosInstance } from 'axios';
import { logger } from './logger';

interface EventsResponse {
  data: any[];
  hasMore: boolean;
  nextCursor?: string;
  cursor?: string;
  pagination?: {
    hasMore: boolean;
    nextCursor?: string;
    limit?: number;
  };
  meta?: any;
}

export class ApiClient {
  private client: AxiosInstance;
  private requestCount = 0;

  constructor(baseUrl: string, apiKey: string) {
    this.client = axios.create({
      baseURL: baseUrl,
      timeout: 60000,
      headers: {
        'X-API-Key': apiKey,
        'Accept': 'application/json',
      },
    });
  }

  async fetchEvents(cursor?: string, limit: number = 1000): Promise<EventsResponse> {
    try {
      const params: any = { limit };
      
      if (cursor) {
        params.cursor = cursor;
      }

      this.requestCount++;
      const response = await this.client.get('/api/v1/events', { params });

      const paginationHasMore = response.data.pagination?.hasMore === true;
      const nextCursor = response.data.pagination?.nextCursor || response.data.nextCursor;

      logger.info({
        cursor: cursor || 'initial',
        eventsReceived: response.data.data?.length || 0,
        hasMore: paginationHasMore,
        nextCursor: nextCursor ? nextCursor.substring(0, 20) + '...' : 'none',
        requestCount: this.requestCount,
        totalInAPI: response.data.meta?.total
      }, 'Fetched events');

      return {
        data: response.data.data || [],
        hasMore: paginationHasMore,
        nextCursor: nextCursor,
        pagination: response.data.pagination,
        meta: response.data.meta
      };
    } catch (error: any) {
      if (error.response?.status === 429) {
        const retryAfter = error.response.headers['retry-after'] || 60;
        const waitTime = parseInt(retryAfter, 10) * 1000;
        
        logger.warn({ waitTimeMs: waitTime }, 'Rate limited, waiting...');
        await new Promise(resolve => setTimeout(resolve, waitTime));
        return this.fetchEvents(cursor, limit);
      }
      
      // Handle 502/503/504 errors with retry
      if (error.response?.status >= 502 && error.response?.status <= 504) {
        logger.warn({ status: error.response.status }, 'Server error, retrying after 5s...');
        await new Promise(resolve => setTimeout(resolve, 5000));
        return this.fetchEvents(cursor, limit);
      }
      
      logger.error({
        error: error.message,
        status: error.response?.status,
        data: error.response?.data
      }, 'API request failed');
      
      throw error;
    }
  }
}