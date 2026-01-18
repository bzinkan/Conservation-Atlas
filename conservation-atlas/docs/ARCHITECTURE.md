# Conservation Atlas Architecture

## System Overview

Conservation Atlas is a real-time conservation monitoring platform with three main components:

1. **Explore Mode** — Interactive global map for researchers and professionals
2. **Place Brief** — Click-anywhere intelligence feature
3. **Classroom Mode** — Educational content for K-12

## Data Flow

```
┌─────────────────────────────────────────────────────────────────────────────┐
│                              DATA PIPELINE                                   │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
    │  ScraperBee  │────▶│    OpenAI    │────▶│   Postgres   │
    │   (Ingest)   │     │  (Extract)   │     │   (Store)    │
    └──────────────┘     └──────────────┘     └──────────────┘

┌─────────────────────────────────────────────────────────────────────────────┐
│                              USER FEATURES                                   │
└─────────────────────────────────────────────────────────────────────────────┘

    ┌──────────────┐     ┌──────────────┐     ┌──────────────┐
    │  Global Map  │     │ Place Brief  │     │   Classroom  │
    │  (Explore)   │     │ (Click-Any)  │     │   (Videos)   │
    └──────────────┘     └──────────────┘     └──────────────┘
```

## Core Components

### 1. Ingestion Pipeline
**ScraperBee** scrapes conservation news sources globally.

### 2. Extraction Pipeline
**OpenAI GPT-4** extracts structured events with AJV validation.

### 3. Organization Extraction
Organizations automatically extracted, normalized, and typed.

### 4. Place Brief Feature
Click anywhere → instant intelligence via PostGIS queries.

### 5. Video Generation
**Pictory** generates videos from scripts.

## Database Schema

### Core Tables
- `events` — Conservation events
- `sources` — News articles, reports
- `organizations` — WWF, NOAA, etc.
- `protected_areas` — WDPA import

## AWS Infrastructure

- ECS Fargate (API + Worker)
- RDS PostgreSQL + PostGIS
- SQS queues with DLQs
- EventBridge schedules
- S3 for assets
- SES for alerts
