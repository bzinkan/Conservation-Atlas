// tools/pick_wdpa_layer.ts
//
// Auto-detect the best WDPA layer in a GeoPackage/Shapefile.
// Scores layers by:
// - Presence of WDPAID + NAME (most important)
// - Geometry type (Polygon/MultiPolygon preferred for polygons file)
// - Field count (tie-breaker)
// - Name hints ("wdpa", "poly", "point")
//
// Usage:
//   npx ts-node tools/pick_wdpa_layer.ts --file ./WDPA_polygons.gpkg
//
// Output:
//   Prints chosen layer name to stdout (for scripting)
//   Writes report to stderr
//
// Example pipeline:
//   LAYER=$(npx ts-node tools/pick_wdpa_layer.ts --file WDPA_polygons.gpkg)
//   npx ts-node tools/generate_wdpa_mapping.ts --file WDPA_polygons.gpkg --layer "$LAYER" --release 2026-01

import { execSync } from "node:child_process";

type Args = { file: string };

function parseArgs(argv: string[]): Args {
  const out: any = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const n = argv[i + 1];
    if (a === "--file") out.file = n;
    if (a.startsWith("--")) i++;
  }
  if (!out.file) throw new Error("Missing --file");
  return out as Args;
}

function sh(cmd: string): string {
  return execSync(cmd, { stdio: ["ignore", "pipe", "pipe"] }).toString("utf8");
}

function normalizeFieldName(s: string): string {
  return s.replace(/\s+/g, "_").replace(/[^\w]/g, "_").toLowerCase();
}

function fieldSet(fields: string[]): Set<string> {
  return new Set(fields.map(normalizeFieldName));
}

function scoreLayer(layerName: string, geomType: string, fields: string[]): number {
  const fs = fieldSet(fields);

  const hasWdpaId = fs.has("wdpaid") || fs.has("wdpa_id");
  const hasName = fs.has("name") || fs.has("pa_name");

  let score = 0;
  if (hasWdpaId) score += 100;
  if (hasName) score += 100;

  // Geometry type bonus
  const g = (geomType || "").toLowerCase();
  if (g.includes("polygon")) score += 40;
  if (g.includes("multipolygon")) score += 40;
  if (g.includes("point")) score += 25;
  if (g.includes("multipoint")) score += 25;

  // Layer name hints
  const n = layerName.toLowerCase();
  if (n.includes("wdpa")) score += 10;
  if (n.includes("poly")) score += 8;
  if (n.includes("point")) score += 6;

  // Tie-breaker: more fields
  score += Math.min(fields.length, 80) * 0.1;

  // Penalty if missing critical fields
  if (!hasWdpaId || !hasName) score -= 150;

  return score;
}

function extractFields(layerObj: any): string[] {
  if (layerObj?.fields && typeof layerObj.fields === "object" && !Array.isArray(layerObj.fields)) {
    return Object.keys(layerObj.fields);
  }
  if (Array.isArray(layerObj?.fieldDefinitions)) {
    return layerObj.fieldDefinitions.map((x: any) => x?.name).filter((x: any) => typeof x === "string");
  }
  if (layerObj?.featureType?.properties && typeof layerObj.featureType.properties === "object") {
    return Object.keys(layerObj.featureType.properties);
  }
  return [];
}

function extractGeomType(layerObj: any): string {
  if (Array.isArray(layerObj?.geometryFields) && layerObj.geometryFields[0]?.type) {
    return String(layerObj.geometryFields[0].type);
  }
  if (layerObj?.geometryType) return String(layerObj.geometryType);
  if (layerObj?.geomType) return String(layerObj.geomType);
  return "";
}

function main() {
  const { file } = parseArgs(process.argv);

  const raw = sh(`ogrinfo -json ${JSON.stringify(file)}`);
  const j = JSON.parse(raw);

  const layers = Array.isArray(j.layers) ? j.layers : [];
  if (!layers.length) throw new Error("No layers found in file");

  const scored: Array<{
    name: string;
    geomType: string;
    fieldCount: number;
    score: number;
    hasCore: boolean;
  }> = [];

  for (const L of layers) {
    const name = L?.name;
    if (typeof name !== "string") continue;

    let layerJson: any = null;
    try {
      const layerRaw = sh(`ogrinfo -json ${JSON.stringify(file)} ${JSON.stringify(name)}`);
      const layerParsed = JSON.parse(layerRaw);
      layerJson =
        (Array.isArray(layerParsed.layers) && layerParsed.layers[0]) ||
        (Array.isArray(layerParsed.layer) && layerParsed.layer[0]) ||
        layerParsed;
    } catch {
      continue;
    }

    const fields = extractFields(layerJson);
    const geomType = extractGeomType(layerJson);

    const fs = fieldSet(fields);
    const hasWdpaId = fs.has("wdpaid") || fs.has("wdpa_id");
    const hasName = fs.has("name") || fs.has("pa_name");

    const score = scoreLayer(name, geomType, fields);
    scored.push({
      name,
      geomType,
      fieldCount: fields.length,
      score,
      hasCore: hasWdpaId && hasName,
    });
  }

  if (!scored.length) throw new Error("No inspectable layers found");

  scored.sort((a, b) => b.score - a.score);
  const best = scored[0];

  // Report to stderr
  const top = scored.slice(0, 8);
  console.error("(info) WDPA layer candidates:");
  for (const x of top) {
    console.error(
      `  - ${x.name} | geom=${x.geomType || "?"} | fields=${x.fieldCount} | score=${x.score.toFixed(1)} | core=${x.hasCore ? "yes" : "no"}`
    );
  }
  console.error(`(info) Selected: ${best.name}`);

  // Output only layer name to stdout
  process.stdout.write(best.name);
}

main();
