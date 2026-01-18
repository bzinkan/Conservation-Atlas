# 🌍 Conservation Atlas

A real-time global conservation monitoring platform that aggregates environmental news, extracts structured events using AI, and presents them on an interactive map.

## Features

- **Explore Mode** — Interactive global map with conservation events
- **Place Brief** — Click anywhere on Earth, get instant intelligence on that location
- **Classroom Mode** — Educational content for K-12 with auto-generated video episodes
- **Alerts** — Subscribe to regions, threat types, or severity levels

## Tech Stack

| Layer | Technology |
|-------|------------|
| Frontend | React, Mapbox/Google Maps |
| API | Node.js, Express, TypeScript |
| Database | PostgreSQL + PostGIS |
| Queue | AWS SQS |
| AI | OpenAI GPT-4, Claude, Gemini |
| Scraping | ScraperBee |
| Video | Pictory |
| Hosting | AWS ECS Fargate |

## Project Structure

\`\`\`
conservation-atlas/
├── packages/
│   ├── api/              # Express API server
│   │   └── src/
│   │       ├── routes/   # API endpoints
│   │       ├── services/ # Business logic
│   │       ├── jobs/     # Background job handlers
│   │       └── utils/    # Shared utilities
│   ├── worker/           # Queue consumer service
│   │   └── src/
│   └── shared/           # Shared code & schemas
│       ├── src/
│       │   ├── types/    # TypeScript types
│       │   └── validators/
│       └── db/           # Database schemas
├── tools/                # CLI utilities (WDPA import)
├── docs/                 # Documentation
└── .github/workflows/    # CI/CD
\`\`\`

## Quick Start

\`\`\`bash
# Clone
git clone https://github.com/yourorg/conservation-atlas.git
cd conservation-atlas

# Install dependencies
npm install

# Setup database
createdb conservation_atlas
psql conservation_atlas -f packages/shared/db/schema.sql
psql conservation_atlas -f packages/shared/db/protected_areas.sql

# Configure environment
cp .env.example .env

# Start development
npm run dev
\`\`\`

## Documentation

- [Architecture](docs/ARCHITECTURE.md) - System design and data flow
- [Setup Guide](docs/SETUP.md) - Detailed installation instructions

## License

MIT
