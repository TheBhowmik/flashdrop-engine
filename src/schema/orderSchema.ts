import { z } from 'zod';

export const OrderRequestSchema = z.object({
    userId: z.string().uuid("Invalid User ID format"),
    productId: z.string().uuid("Invalid Product ID format"),
    quantity: z.number()
        .int("Quantity must be an integer")
        .positive("Quantity must be greater than zero")
        .max(2, "Limit of 2 items per customer for flash sales"),
});

// Extract the TypeScript type from the Zod schema
export type OrderRequest = z.infer<typeof OrderRequestSchema>;