import { Request, Response, NextFunction } from 'express';
import { redisClient } from '../config/redis';

export const rateLimiter = async (req: Request, res: Response, next: NextFunction): Promise<any> => {
    // In production behind a load balancer, use req.headers['x-forwarded-for']
    const ip = req.ip || req.socket.remoteAddress || 'unknown-ip';
    const key = `rate_limit:${ip}`;
    
    const MAX_REQUESTS = 5; // Allow 5 requests...
    const WINDOW_SECONDS = 1; // ...per 1 second

    try {
        // Atomically increment the request count for this IP
        const requestCount = await redisClient.incr(key);
        
        // If this is the first request in the window, set the expiration timer
        if (requestCount === 1) {
            await redisClient.expire(key, WINDOW_SECONDS);
        }

        if (requestCount > MAX_REQUESTS) {
            return res.status(429).json({ 
                status: 'error', 
                message: 'Too many requests. Please slow down.' 
            });
        }
        
        next();
    } catch (error) {
        console.error('Rate limiter failed (Redis down?), bypassing...', error);
        next(); // Fail open: if Redis crashes, don't break the whole API
    }
};