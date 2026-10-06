CREATE TYPE "public"."shipment_status" AS ENUM('REQUESTED', 'ACCEPTED', 'DELIVERED');--> statement-breakpoint
ALTER TYPE "public"."order_status" ADD VALUE 'SHIPPED';--> statement-breakpoint
ALTER TYPE "public"."order_status" ADD VALUE 'DELIVERED';--> statement-breakpoint
CREATE TABLE "inbound_webhooks" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"source" text NOT NULL,
	"external_event_id" text NOT NULL,
	"event_type" text NOT NULL,
	"headers" jsonb NOT NULL,
	"payload" jsonb NOT NULL,
	"received_at" timestamp with time zone DEFAULT now() NOT NULL
);
--> statement-breakpoint
CREATE TABLE "shipments" (
	"id" uuid PRIMARY KEY DEFAULT gen_random_uuid() NOT NULL,
	"order_id" uuid NOT NULL,
	"carrier" text NOT NULL,
	"status" "shipment_status" DEFAULT 'REQUESTED' NOT NULL,
	"external_id" text,
	"tracking_code" text,
	"attempts" integer DEFAULT 0 NOT NULL,
	"request_body" jsonb,
	"last_status_code" integer,
	"last_response" jsonb,
	"last_error" text,
	"requested_at" timestamp with time zone DEFAULT now() NOT NULL,
	"accepted_at" timestamp with time zone,
	"delivered_at" timestamp with time zone,
	CONSTRAINT "shipments_order_id_unique" UNIQUE("order_id")
);
--> statement-breakpoint
ALTER TABLE "orders" ADD COLUMN "shipped_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "orders" ADD COLUMN "delivered_at" timestamp with time zone;--> statement-breakpoint
ALTER TABLE "shipments" ADD CONSTRAINT "shipments_order_id_orders_id_fk" FOREIGN KEY ("order_id") REFERENCES "public"."orders"("id") ON DELETE cascade ON UPDATE no action;--> statement-breakpoint
CREATE UNIQUE INDEX "ux_inbound_webhooks_source_event" ON "inbound_webhooks" USING btree ("source","external_event_id");