import { db } from './db';

const setup = async () => {
    try {
        await db.query(`
            CREATE TABLE IF NOT EXISTS users (
                id UUID PRIMARY KEY,
                name VARCHAR(100)
            );
            
            CREATE TABLE IF NOT EXISTS products (
                id UUID PRIMARY KEY,
                name VARCHAR(100),
                stock INT NOT NULL CHECK (stock >= 0)
            );
            
            CREATE TABLE IF NOT EXISTS orders (
                id SERIAL PRIMARY KEY,
                user_id UUID REFERENCES users(id),
                product_id UUID REFERENCES products(id),
                quantity INT NOT NULL,
                status VARCHAR(20) DEFAULT 'success',
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        `);

        // Insert Seed Data (Ignoring conflicts if they already exist)
        await db.query(`
            INSERT INTO users (id, name) 
            VALUES ('123e4567-e89b-12d3-a456-426614174000', 'Test User')
            ON CONFLICT DO NOTHING;
        `);

        await db.query(`
            INSERT INTO products (id, name, stock) 
            VALUES ('123e4567-e89b-12d3-a456-426614174001', 'Limited Edition Sneaker', 1000)
            ON CONFLICT DO NOTHING;
        `);

        console.log('✅ Database schemas created and seeded successfully.');
        process.exit(0);
    } catch (error) {
        console.error('❌ Database setup failed:', error);
        process.exit(1);
    }
};

setup();