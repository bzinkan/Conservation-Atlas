// packages/api/src/utils/sourceQuality.ts

/**
 * Source Quality Scoring
 * 
 * Assigns quality scores to sources based on:
 * - Source type (gov/ngo/academic vs news/blog)
 * - Publisher reputation
 * - Content characteristics
 */

export interface SourceQualityInput {
  sourceType: string;
  publisher: string | null;
  credibilityScore: number | null;
  
  // Optional content-based signals
  hasAuthor?: boolean;
  hasPublishDate?: boolean;
  wordCount?: number;
  hasCitations?: boolean;
}

export interface SourceQualityResult {
  score: number;          // 0-1 overall quality score
  tier: 'high' | 'medium' | 'low';
  factors: {
    sourceType: number;
    publisher: number;
    content: number;
  };
  boost: number;          // Multiplier for confidence scores
}

// Publisher trust levels (0-1)
const PUBLISHER_TRUST: Record<string, number> = {
  // Government sources
  'noaa': 0.95,
  'epa': 0.95,
  'usgs': 0.95,
  'nasa': 0.95,
  'usda': 0.90,
  'usfws': 0.95,
  'nps': 0.90,
  'environment canada': 0.90,
  'defra': 0.90,
  'european environment agency': 0.90,
  
  // Major NGOs
  'wwf': 0.85,
  'world wildlife fund': 0.85,
  'conservation international': 0.85,
  'the nature conservancy': 0.85,
  'wildlife conservation society': 0.85,
  'iucn': 0.90,
  'greenpeace': 0.70,
  'sierra club': 0.75,
  'audubon': 0.85,
  'oceana': 0.80,
  
  // Academic/Research
  'nature': 0.95,
  'science': 0.95,
  'pnas': 0.90,
  'conservation biology': 0.90,
  'biological conservation': 0.90,
  'plos one': 0.80,
  'frontiers': 0.75,
  
  // Quality news
  'reuters': 0.85,
  'associated press': 0.85,
  'ap news': 0.85,
  'bbc': 0.85,
  'the guardian': 0.80,
  'new york times': 0.80,
  'washington post': 0.80,
  'national geographic': 0.85,
  
  // Conservation-focused news
  'mongabay': 0.80,
  'conservation news': 0.80,
  'carbon brief': 0.85,
  'climate home': 0.75,
  'ensia': 0.75,
  'grist': 0.70,
  
  // Regional quality sources
  'abc australia': 0.80,
  'cbc': 0.80,
  'al jazeera': 0.75,
  
  // Lower quality (not bad, just less rigorous)
  'huffpost': 0.60,
  'vice': 0.55,
  'daily mail': 0.40,
  'fox news': 0.50,
  'breitbart': 0.30,
};

// Source type base scores
const SOURCE_TYPE_SCORES: Record<string, number> = {
  'gov': 0.90,
  'government': 0.90,
  'ngo': 0.80,
  'academic': 0.85,
  'news': 0.60,
  'wire': 0.70,
  'blog': 0.40,
  'social': 0.30,
  'unknown': 0.50,
};

/**
 * Calculate overall source quality score
 */
export function calculateSourceQualityScore(input: SourceQualityInput): number {
  const result = getSourceQuality(input);
  return result.score;
}

/**
 * Get detailed source quality analysis
 */
export function getSourceQuality(input: SourceQualityInput): SourceQualityResult {
  // 1. Source type score (40% weight)
  const sourceTypeScore = SOURCE_TYPE_SCORES[input.sourceType.toLowerCase()] ?? 0.5;
  
  // 2. Publisher score (40% weight)
  let publisherScore = 0.5; // Default unknown
  
  if (input.publisher) {
    const normalizedPublisher = input.publisher.toLowerCase().trim();
    
    // Check for exact match
    if (PUBLISHER_TRUST[normalizedPublisher]) {
      publisherScore = PUBLISHER_TRUST[normalizedPublisher];
    } else {
      // Check for partial matches
      for (const [known, score] of Object.entries(PUBLISHER_TRUST)) {
        if (normalizedPublisher.includes(known) || known.includes(normalizedPublisher)) {
          publisherScore = score;
          break;
        }
      }
    }
  }
  
  // Use existing credibility score if available
  if (input.credibilityScore !== null && input.credibilityScore !== undefined) {
    // Blend with our calculation
    publisherScore = (publisherScore + input.credibilityScore) / 2;
  }
  
  // 3. Content quality signals (20% weight)
  let contentScore = 0.5;
  
  if (input.hasAuthor !== undefined || input.hasPublishDate !== undefined || input.wordCount !== undefined) {
    let contentFactors = 0;
    let contentSum = 0;
    
    if (input.hasAuthor !== undefined) {
      contentSum += input.hasAuthor ? 0.7 : 0.3;
      contentFactors++;
    }
    
    if (input.hasPublishDate !== undefined) {
      contentSum += input.hasPublishDate ? 0.8 : 0.4;
      contentFactors++;
    }
    
    if (input.wordCount !== undefined) {
      // Longer, more detailed articles generally better
      if (input.wordCount > 1000) contentSum += 0.8;
      else if (input.wordCount > 500) contentSum += 0.6;
      else if (input.wordCount > 200) contentSum += 0.5;
      else contentSum += 0.3;
      contentFactors++;
    }
    
    if (input.hasCitations !== undefined) {
      contentSum += input.hasCitations ? 0.9 : 0.5;
      contentFactors++;
    }
    
    contentScore = contentFactors > 0 ? contentSum / contentFactors : 0.5;
  }
  
  // Weighted combination
  const weights = {
    sourceType: 0.40,
    publisher: 0.40,
    content: 0.20,
  };
  
  const overallScore = (
    sourceTypeScore * weights.sourceType +
    publisherScore * weights.publisher +
    contentScore * weights.content
  );
  
  // Determine tier
  let tier: 'high' | 'medium' | 'low';
  if (overallScore >= 0.75) tier = 'high';
  else if (overallScore >= 0.50) tier = 'medium';
  else tier = 'low';
  
  // Calculate confidence boost/penalty
  const boost = tier === 'high' ? 1.1 : tier === 'medium' ? 1.0 : 0.8;
  
  return {
    score: Math.round(overallScore * 100) / 100,
    tier,
    factors: {
      sourceType: Math.round(sourceTypeScore * 100) / 100,
      publisher: Math.round(publisherScore * 100) / 100,
      content: Math.round(contentScore * 100) / 100,
    },
    boost,
  };
}

/**
 * Check if source meets minimum quality threshold for a use case
 */
export function meetsQualityThreshold(
  input: SourceQualityInput,
  useCase: 'extraction' | 'classroom' | 'alert' | 'featured'
): boolean {
  const quality = getSourceQuality(input);
  
  const thresholds: Record<typeof useCase, number> = {
    extraction: 0.30,   // Low bar - we extract from anything
    classroom: 0.65,    // Higher bar for educational content
    alert: 0.50,        // Medium bar for alerts
    featured: 0.75,     // High bar for featured content
  };
  
  return quality.score >= thresholds[useCase];
}

/**
 * Get source type from publisher name heuristics
 */
export function inferSourceType(publisher: string | null, url: string | null): string {
  if (!publisher && !url) return 'unknown';
  
  const text = `${publisher ?? ''} ${url ?? ''}`.toLowerCase();
  
  // Government indicators
  if (text.includes('.gov') || 
      text.includes('government') ||
      text.includes('ministry') ||
      text.includes('department of')) {
    return 'gov';
  }
  
  // Academic indicators
  if (text.includes('.edu') ||
      text.includes('university') ||
      text.includes('journal') ||
      text.includes('research') ||
      text.includes('proceedings')) {
    return 'academic';
  }
  
  // NGO indicators
  if (text.includes('foundation') ||
      text.includes('conservation') ||
      text.includes('wildlife') ||
      text.includes('.org')) {
    return 'ngo';
  }
  
  // Wire services
  if (text.includes('reuters') ||
      text.includes('associated press') ||
      text.includes('afp')) {
    return 'wire';
  }
  
  // Social media
  if (text.includes('twitter') ||
      text.includes('facebook') ||
      text.includes('reddit') ||
      text.includes('instagram')) {
    return 'social';
  }
  
  // Blogs
  if (text.includes('blog') ||
      text.includes('medium.com') ||
      text.includes('substack')) {
    return 'blog';
  }
  
  return 'news';
}
