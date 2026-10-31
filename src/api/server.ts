import express from 'express';
import cors from 'cors';
import dotenv from 'dotenv';
import { db } from '../config/db.js';
import { OrderRequestSchema } from '../schema/orderSchema.js';
import { rateLimiter } from '../middleware/rateLimiter.js';
import '../config/redis.js';
import { producer, connectKafkaProducer } from '../config/kafka.js';

dotenv.config();

const app = express();
app.use(cors());
app.use(express.json());

app.get('/health', async (req, res) => {
    try {
        const result = await db.query('SELECT NOW()');
        res.status(200).json({ status: 'ok', service: 'flashdrop-api', dbTime: result.rows[0].now });
    } catch (error) {
        console.error('Database connection error:', error);
        res.status(500).json({ status: 'error', message: 'Database connection failed' });
    }
});

app.post('/api/orders', rateLimiter, async (req, res) => {
    try {
        // 1. Validate payload
        const validatedData = OrderRequestSchema.parse(req.body);
        
        // 2. Publish event to Kafka
        await producer.send({
            topic: 'order-requests',
            messages: [
                { 
                    key: validatedData.userId, // Ensures orders from the same user are processed in order
                    value: JSON.stringify(validatedData) 
                },
            ],
        });

        // 3. Immediately return success to the user
        res.status(202).json({ 
            status: 'pending', 
            message: 'Order queued for processing.',
            data: validatedData 
        });

    } catch (error: any) {
        if (error.name === 'ZodError') {
            return res.status(400).json({ error: error.errors });
        }
        console.error('Order ingestion error:', error);
        res.status(500).json({ error: 'Internal Server Error' });
    }
});

const PORT = process.env.PORT || 3000;

app.listen(PORT, async () => {
    await connectKafkaProducer(); // Boot Kafka producer when server starts
    console.log(`🚀 API Gateway running on port ${PORT}`);
});