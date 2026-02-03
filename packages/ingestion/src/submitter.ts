import axios, { AxiosInstance } from 'axios';
import { logger } from './logger';
import * as fs from 'fs';

export class Submitter {
  private client: AxiosInstance;
  private githubRepo: string;

  constructor(baseUrl: string, apiKey: string, githubRepo: string) {
    this.client = axios.create({
      baseURL: baseUrl,
      timeout: 120000, // 2 minutes timeout for large file upload
      headers: {
        'X-API-Key': apiKey,
      },
    });
    this.githubRepo = githubRepo;
  }

  async submit(eventIdsFilePath: string): Promise<void> {
    try {
      logger.info('Reading event IDs file...');
      const eventIds = fs.readFileSync(eventIdsFilePath, 'utf-8');
      const eventCount = eventIds.split('\n').filter(id => id.trim()).length;

      logger.info({
        eventCount,
        fileSize: `${(eventIds.length / 1024 / 1024).toFixed(2)} MB`,
        githubRepo: this.githubRepo
      }, 'Submitting results...');

      const response = await this.client.post(
        `/api/v1/submissions?github_repo=${encodeURIComponent(this.githubRepo)}`,
        eventIds,
        {
          headers: {
            'Content-Type': 'text/plain',
          },
        }
      );

      logger.info({
        success: response.data.success,
        submissionId: response.data.data?.submissionId,
        eventCount: response.data.data?.eventCount,
        timeToSubmit: response.data.data?.timeToSubmit,
        submissionNumber: response.data.data?.submissionNumber,
        remainingSubmissions: response.data.data?.remainingSubmissions,
        message: response.data.message
      }, 'Submission successful!');

      logger.info('==============================================');
      logger.info('SUBMISSION COMPLETE!');
      logger.info(`Submission ID: ${response.data.data?.submissionId}`);
      logger.info(`Events submitted: ${response.data.data?.eventCount?.toLocaleString()}`);
      logger.info(`Time taken: ${response.data.data?.timeToSubmit?.formatted}`);
      logger.info(`Submission #${response.data.data?.submissionNumber} of 5`);
      logger.info('==============================================');

    } catch (error: any) {
      logger.error({
        error: error.message,
        status: error.response?.status,
        data: error.response?.data
      }, 'Submission failed');
      
      if (error.response?.status === 400) {
        logger.error('Bad request - check if event IDs format is correct');
      } else if (error.response?.status === 429) {
        logger.error('Too many submissions - you may have exceeded the 5 submission limit');
      }
      
      throw error;
    }
  }

  async checkSubmissions(): Promise<void> {
    try {
      const response = await this.client.get('/api/v1/submissions');
      
      logger.info({
        submissions: response.data
      }, 'Your submissions');

    } catch (error: any) {
      logger.error({
        error: error.message,
        status: error.response?.status
      }, 'Failed to check submissions');
    }
  }
}