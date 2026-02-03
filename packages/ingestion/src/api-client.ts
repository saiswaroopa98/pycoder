import axios, { AxiosInstance, AxiosError } from 'axios';
import { logger } from './logger';

interface EventsResponse {
  data: any[];
  hasMore: boolean;
  nextCursor?: string;
  cursor?: string;
  total?: number;
}

interface RateLimitInfo {
  limit: number;
  remaining: number;
  reset: number;
}

export class ApiClient {
  private client: AxiosInstance;
  private apiKey: string;
  private baseUrl: string;
  private rateLimitInfo: RateLimitInfo | null = null;
  private requestCount = 0;
  private startTime = Date.now();

  constructor(baseUrl: string, apiKey: string) {
    this.baseUrl = baseUrl;
    this.apiKey = apiKey;
    
    this.client = axios.create({
      baseURL: baseUrl,
      timeout: 30000,
      headers: {
        'X-API-Key': apiKey,
        'Accept': 'application/json',
      },
    });

    // Add response interceptor to track rate limits
    this.client.interceptors.response.use(
      (response) => {
        this.updateRateLimitInfo(response.headers);
        return response;
      },
      (error) => {
        if (error.response) {
          this.updateRateLimitInfo(error.response.headers);
        }
        return Promise.reject(error);
      }
    );
  }

  private updateRateLimitInfo(headers: any): void {
    const limit = headers['x-ratelimit-limit'];
    const remaining = headers['x-ratelimit-remaining'];
    const reset = headers['x-ratelimit-reset'];

    if (limit && remaining && reset) {
      this.rateLimitInfo = {
        limit: parseInt(limit, 10),
        remaining: parseInt(remaining, 10),
        reset: parseInt(reset, 10),
      };
    }
  }

  private async handleRateLimit(): Promise<void> {
    if (!this.rateLimitInfo) return;

    const { remaining, reset } = this.rateLimitInfo;

    // If we're close to the limit, wait
    if (remaining < 5) {
      const waitTime = Math.max(0, reset - Date.now());
      if (waitTime > 0) {
        logger.warn(
          { waitTimeMs: waitTime, remaining },
          'Rate limit approaching, waiting...'
        );
        await new Promise((resolve) => setTimeout(resolve, waitTime));
      }
    }
  }

  async fetchEvents(
    cursor?: string,
    limit: number = 1000
  ): Promise<EventsResponse> {
    await this.handleRateLimit();

    try {
      const params: any = { limit };
      if (cursor) {
        params.cursor = cursor;
      }

      this.requestCount++;
      const response = await this.client.get('/api/v1/events', { params });

      const elapsed = Date.now() - this.startTime;
      const requestsPerSecond = (this.requestCount / elapsed) * 1000;

      logger.info({
        cursor: cursor || 'initial',
        eventsReceived: response.data.data?.length || 0,
        hasMore: response.data.hasMore,
        requestCount: this.requestCount,
        requestsPerSecond: requestsPerSecond.toFixed(2),
        rateLimit: this.rateLimitInfo,
      }, 'Fetched events from API');

      return response.data;
    } catch (error) {
      if (axios.isAxiosError(error)) {
        const axiosError = error as AxiosError;
        
        // Handle rate limit errors
        if (axiosError.response?.status === 429) {
          const retryAfter = axiosError.response.headers['retry-after'];
          const waitTime = retryAfter ? parseInt(retryAfter, 10) * 1000 : 5000;
          
          logger.warn(
            { waitTimeMs: waitTime },
            'Rate limited by server, waiting...'
          );
          
          await new Promise((resolve) => setTimeout(resolve, waitTime));
          return this.fetchEvents(cursor, limit);
        }

        // Handle other HTTP errors
        if (axiosError.response) {
          logger.error({
            status: axiosError.response.status,
            data: axiosError.response.data,
            cursor,
          }, 'API request failed');
        }
      }

      throw error;
    }
  }

  async discoverBatchEndpoint(): Promise<boolean> {
    try {
      // Try to discover if there's a batch endpoint
      logger.info('Attempting to discover batch endpoint...');
      
      const batchResponse = await this.client.get('/api/v1/events/batch', {
        params: { limit: 100 },
      }).catch(() => null);

      if (batchResponse && batchResponse.status === 200) {
        logger.info('Batch endpoint discovered!');
        return true;
      }

      // Try alternative endpoints
      const bulkResponse = await this.client.get('/api/v1/events/bulk', {
        params: { limit: 100 },
      }).catch(() => null);

      if (bulkResponse && bulkResponse.status === 200) {
        logger.info('Bulk endpoint discovered!');
        return true;
      }

      logger.info('No batch endpoints found, using standard pagination');
      return false;
    } catch (error) {
      logger.debug('Batch endpoint discovery failed');
      return false;
    }
  }

  getRateLimitInfo(): RateLimitInfo | null {
    return this.rateLimitInfo;
  }

  getRequestCount(): number {
    return this.requestCount;
  }

  getRequestsPerSecond(): number {
    const elapsed = Date.now() - this.startTime;
    return (this.requestCount / elapsed) * 1000;
  }
}