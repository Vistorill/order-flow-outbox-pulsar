ALTER TABLE "idempotency_keys" ADD COLUMN "duplicate_requests" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "idempotency_keys" ADD COLUMN "last_duplicate_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "inbound_webhooks" ADD COLUMN "duplicates" integer DEFAULT 0 NOT NULL;--> statement-breakpoint
ALTER TABLE "processed_events" ADD COLUMN "duplicates" integer DEFAULT 0 NOT NULL;