import { Pool } from 'pg';
import dotenv from 'dotenv';

dotenv.config();

export const db = new Pool({
    user: process.env.POSTGRES_USER || 'flash_user',
    password: process.env.POSTGRES_PASSWORD || 'flash_password',
    host: process.env.POSTGRES_HOST || '127.0.0.1', // <-- Changed here
    port: parseInt(process.env.POSTGRES_PORT || '5432'),
    database: process.env.POSTGRES_DB || 'flashdrop',
    
    max: 20, 
    idleTimeoutMillis: 30000,
    connectionTimeoutMillis: 2000,
});

db.on('connect', () => {
    console.log('✅ Connected to PostgreSQL');
});

db.on('error', (err) => {
    console.error('❌ Unexpected error on idle client', err);
    process.exit(-1);
});