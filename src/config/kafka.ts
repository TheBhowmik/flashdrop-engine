import { Kafka } from 'kafkajs';

const kafka = new Kafka({
    clientId: 'flashdrop-api',
    brokers: ['localhost:9092'], // Connects to our Dockerized Kafka
});

export const producer = kafka.producer();

export const connectKafkaProducer = async () => {
    try {
        await producer.connect();
        console.log('✅ Connected to Kafka Producer');
    } catch (error) {
        console.error('❌ Failed to connect to Kafka', error);
        process.exit(1);
    }
};