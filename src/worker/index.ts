import { Kafka } from 'kafkajs';
import { db } from '../config/db.js';
import type { OrderRequest } from '../schema/orderSchema.js';

const kafka = new Kafka({
    clientId: 'flashdrop-worker',
    brokers: ['localhost:9092'],
});

const consumer = kafka.consumer({ groupId: 'order-processing-group' });

const processOrder = async (order: OrderRequest) => {
    const client = await db.connect();
    
    try {
        // 1. Begin ACID Transaction
        await client.query('BEGIN');

        // 2. ROW-LEVEL LOCK: Prevent any other concurrent transaction from modifying this product
        const productRes = await client.query(
            'SELECT stock FROM products WHERE id = $1 FOR UPDATE', 
            [order.productId]
        );

        const currentStock = productRes.rows[0]?.stock;

        // 3. Evaluate Stock
        if (currentStock === undefined) {
            throw new Error('Product not found');
        }

        if (currentStock < order.quantity) {
            console.log(`❌ Order failed for user ${order.userId}: OUT OF STOCK`);
            await client.query('ROLLBACK');
            return;
        }

        // 4. Deduct Inventory & Create Order Record
        await client.query(
            'UPDATE products SET stock = stock - $1 WHERE id = $2',
            [order.quantity, order.productId]
        );

        await client.query(
            'INSERT INTO orders (user_id, product_id, quantity) VALUES ($1, $2, $3)',
            [order.userId, order.productId, order.quantity]
        );

        // 5. Commit Transaction
        await client.query('COMMIT');
        console.log(`✅ Order processed for user ${order.userId}. Stock remaining: ${currentStock - order.quantity}`);

    } catch (error) {
        await client.query('ROLLBACK');
        console.error('Transaction failed, rolled back.', error);
    } finally {
        client.release();
    }
};

const runWorker = async () => {
    await consumer.connect();
    console.log('✅ Kafka Consumer connected');
    
    await consumer.subscribe({ topic: 'order-requests', fromBeginning: true });

    await consumer.run({
        eachMessage: async ({ message }) => {
            if (message.value) {
                const order: OrderRequest = JSON.parse(message.value.toString());
                await processOrder(order);
            }
        },
    });
};

runWorker().catch(console.error);