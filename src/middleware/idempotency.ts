import type { Request, Response, NextFunction } from 'express';
import { redisClient } from '../config/redis.js';

export const idempotencyCheck = async (req: Request, res: Response, next: NextFunction) => {
    // 1. Extract the key from the incoming request headers
    const idempotencyKey = req.headers['x-idempotency-key'];

    if (!idempotencyKey || typeof idempotencyKey !== 'string') {
        return res.status(400).json({ error: 'x-idempotency-key header is required' });
    }

    const cacheKey = `idempotency:${idempotencyKey}`;

    try {
        // 2. Atomic lock: Set the key ONLY if it does not exist (NX), expiring in 24 hours (EX 86400)
        const isNewRequest = await redisClient.set(cacheKey, 'QUEUED', {
            NX: true,
            EX: 86400
        });

        // 3. If Redis returns null, the key already exists. Reject the duplicate.
        if (!isNewRequest) {
            return res.status(409).json({ 
                status: 'duplicate', 
                message: 'This exact order is already being processed. Please wait.' 
            });
        }

        // 4. If successful, pass the request to the next step
        next();
    } catch (error) {
        console.error('Idempotency check failed (Redis issue?), bypassing...', error);
        next(); // Fail open: do not block the flash sale if Redis temporarily disconnects
    }
};