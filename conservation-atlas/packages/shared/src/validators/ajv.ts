// packages/shared/src/validators/ajv.ts
//
// AJV setup and validation helpers for all schemas

import Ajv, { ValidateFunction, ErrorObject } from "ajv";
import addFormats from "ajv-formats";

export type AjvError = ErrorObject;

export interface ValidationResult<T> {
  ok: boolean;
  value?: T;
  errors?: AjvError[];
  errorText?: string;
}

/**
 * Create a configured AJV instance
 */
export function createAjv(): Ajv {
  const ajv = new Ajv({
    allErrors: true,        // Report all errors, not just the first
    strict: false,          // Pragmatic for evolving schemas
    removeAdditional: false,
    allowUnionTypes: true,
    coerceTypes: false,     // Don't auto-coerce types
    useDefaults: true,      // Apply default values from schema
  });
  
  addFormats(ajv);
  
  return ajv;
}

/**
 * Validates `data` against an AJV validate function.
 * Returns a normalized result with human-readable error text.
 */
export function validateWithAjv<T>(
  validateFn: ValidateFunction, 
  data: unknown
): ValidationResult<T> {
  const ok = validateFn(data) as boolean;

  if (ok) {
    return { ok: true, value: data as T };
  }

  const errors = (validateFn.errors ?? []) as AjvError[];
  return {
    ok: false,
    errors,
    errorText: formatAjvErrors(errors),
  };
}

/**
 * Format AJV errors into a human-readable string
 */
export function formatAjvErrors(errors: AjvError[]): string {
  if (!errors.length) return "Unknown schema validation error";

  // Make a short, readable list: "path: message"
  const lines = errors.slice(0, 12).map((e) => {
    const path = e.instancePath || "(root)";
    const msg = e.message || "invalid";
    
    // Add extra context for common error types
    let detail = "";
    if (e.keyword === "enum" && e.params?.allowedValues) {
      detail = ` (allowed: ${e.params.allowedValues.slice(0, 5).join(", ")})`;
    }
    if (e.keyword === "type" && e.params?.type) {
      detail = ` (expected: ${e.params.type})`;
    }
    if (e.keyword === "minimum" || e.keyword === "maximum") {
      detail = ` (limit: ${e.params?.limit})`;
    }
    
    return `${path}: ${msg}${detail}`;
  });

  const more = errors.length > 12 ? ` (+${errors.length - 12} more)` : "";
  return lines.join("; ") + more;
}

/**
 * Validate and throw on failure (convenience function)
 */
export function validateOrThrow<T>(
  validateFn: ValidateFunction,
  data: unknown,
  schemaName: string = "data"
): T {
  const result = validateWithAjv<T>(validateFn, data);
  
  if (!result.ok) {
    throw new ValidationError(
      `Invalid ${schemaName}: ${result.errorText}`,
      result.errors ?? []
    );
  }
  
  return result.value!;
}

/**
 * Custom error class for validation failures
 */
export class ValidationError extends Error {
  public readonly errors: AjvError[];
  public readonly isValidationError = true;
  
  constructor(message: string, errors: AjvError[]) {
    super(message);
    this.name = "ValidationError";
    this.errors = errors;
    
    // Maintains proper stack trace in V8 environments
    if (Error.captureStackTrace) {
      Error.captureStackTrace(this, ValidationError);
    }
  }
  
  /**
   * Get errors for a specific path
   */
  errorsForPath(path: string): AjvError[] {
    return this.errors.filter(e => e.instancePath === path || e.instancePath.startsWith(path + "/"));
  }
  
  /**
   * Check if a specific field has errors
   */
  hasErrorAt(path: string): boolean {
    return this.errorsForPath(path).length > 0;
  }
}

/**
 * Type guard for ValidationError
 */
export function isValidationError(err: unknown): err is ValidationError {
  return (err as any)?.isValidationError === true;
}

// ============================================
// Pre-compiled validators singleton
// ============================================

let _ajv: Ajv | null = null;
const _validators: Map<string, ValidateFunction> = new Map();

/**
 * Get the shared AJV instance
 */
export function getAjv(): Ajv {
  if (!_ajv) {
    _ajv = createAjv();
  }
  return _ajv;
}

/**
 * Get or compile a validator for a schema
 */
export function getValidator(schemaId: string, schema: object): ValidateFunction {
  if (!_validators.has(schemaId)) {
    const ajv = getAjv();
    const validate = ajv.compile(schema);
    _validators.set(schemaId, validate);
  }
  return _validators.get(schemaId)!;
}

/**
 * Clear cached validators (useful for testing)
 */
export function clearValidatorCache(): void {
  _validators.clear();
  _ajv = null;
}
