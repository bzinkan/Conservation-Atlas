// tools/generate_wdpa_mapping.ts
//
// WDPA import mapping generator:
// - Reads actual field names from a WDPA GeoPackage/Shapefile using `ogrinfo -json`
// - Auto-maps WDPA fields into canonical `protected_areas_staging` columns
// - Prints SQL to run after ogr2ogr loads data into staging
//
// Usage:
//   npx ts-node tools/generate_wdpa_mapping.ts \
//     --file ./WDPA_WDOECM_Jan2026_Public.gpkg \
//     --layer WDPA_poly_Jan2026 \
//     --release 2026-01
//
// Requirements:
// - GDAL installed and `ogrinfo` available in PATH
// - Staging table already created with protected_areas schema

import { execSync } from "node:child_process";

type Args = {
  file: string;
  layer?: string;
  release: string;
};

function parseArgs(argv: string[]): Args {
  const out: any = {};
  for (let i = 2; i < argv.length; i++) {
    const a = argv[i];
    const n = argv[i + 1];
    if (a === "--file") out.file = n;
    if (a === "--layer") out.layer = n;
    if (a === "--release") out.release = n;
    if (a.startsWith("--")) i++;
  }
  if (!out.file) throw new Error("Missing --file");
  if (!out.release) throw new Error("Missing --release (e.g. 2026-01)");
  return out as Args;
}

function sh(cmd: string): string {
  return execSync(cmd, { stdio: ["ignore", "pipe", "pipe"] }).toString("utf8");
}

function quoteIdent(ident: string): string {
  // Quote identifiers to preserve case / reserved words
  return `"${ident.replace(/"/g, '""')}"`;
}

function normalizeFieldName(s: string): string {
  return s.replace(/\s+/g, "_").replace(/[^\w]/g, "_").toLowerCase();
}

function buildFieldIndex(fields: string[]): Map<string, string> {
  const m = new Map<string, string>();
  for (const f of fields) m.set(normalizeFieldName(f), f);
  return m;
}

function pickField(fieldIndex: Map<string, string>, candidates: string[]): string | null {
  for (const c of candidates) {
    const found = fieldIndex.get(normalizeFieldName(c));
    if (found) return found;
  }
  return null;
}

function listLayers(file: string): string[] {
  const raw = sh(`ogrinfo -json ${JSON.stringify(file)}`);
  const j = JSON.parse(raw);
  const layers: string[] = [];
  if (Array.isArray(j.layers)) {
    for (const L of j.layers) {
      if (typeof L.name === "string") layers.push(L.name);
    }
  }
  return layers;
}

function getLayerFields(file: string, layer: string): string[] {
  const raw = sh(`ogrinfo -json ${JSON.stringify(file)} ${JSON.stringify(layer)}`);
  const j = JSON.parse(raw);

  const layerObj =
    (Array.isArray(j.layers) && j.layers[0]) ||
    (Array.isArray(j.layer) && j.layer[0]) ||
    j;

  const fieldsObj =
    layerObj?.fields ||
    layerObj?.fieldDefinitions ||
    layerObj?.featureType?.properties ||
    null;

  if (fieldsObj && typeof fieldsObj === "object" && !Array.isArray(fieldsObj)) {
    return Object.keys(fieldsObj);
  }

  if (Array.isArray(layerObj?.fieldDefinitions)) {
    return layerObj.fieldDefinitions
      .map((x: any) => x?.name)
      .filter((x: any) => typeof x === "string");
  }

  throw new Error(`Could not parse fields for layer ${layer}. Try: ogrinfo -so -al ${file}`);
}

function main() {
  const args = parseArgs(process.argv);
  const file = args.file;
  let layer = args.layer;

  if (!layer) {
    const layers = listLayers(file);
    if (!layers.length) throw new Error("No layers found. Provide --layer explicitly.");
    layer = layers[0];
    console.error(`(info) No --layer provided. Using first layer: ${layer}`);
  }

  const fields = getLayerFields(file, layer);
  const fieldIndex = buildFieldIndex(fields);

  // Canonical column -> possible WDPA field names (synonyms/case variants)
  const mappingCandidates: Record<string, string[]> = {
    wdpa_id: ["WDPAID", "WDPA_ID", "wdpaid", "wdpa_id"],
    wdpa_pid: ["WDPA_PID", "WDPA_PID_", "wdpa_pid"],
    name: ["NAME", "PA_NAME", "name"],
    designation: ["DESIG", "DESIGNATION", "desig"],
    designation_type: ["DESIG_TYPE", "DESIGTYPE", "designation_type"],
    iucn_category: ["IUCN_CAT", "IUCN_CATEGORY", "iucn_cat"],
    status: ["STATUS", "status"],
    status_year: ["STATUS_YR", "STATUS_YEAR", "status_yr"],
    gov_type: ["GOV_TYPE", "GOVTYPE", "gov_type"],
    own_type: ["OWN_TYPE", "OWNTYPE", "own_type"],
    mang_auth: ["MANG_AUTH", "MANGAUTH", "mang_auth"],
    mang_plan: ["MANG_PLAN", "MANGPLAN", "mang_plan"],
    verif: ["VERIF", "VERIFICATION", "verif"],
    iso3: ["ISO3", "ISO", "iso3"],
    parent_iso3: ["PARENT_ISO3", "PARENTISO3", "parent_iso3"],
    marine: ["MARINE", "marine"],
    reported_area_km2: ["REP_AREA", "REP_AREA_KM2", "reported_area", "rep_area"],
    gis_area_km2: ["GIS_AREA", "gis_area"],
    gis_m_area_km2: ["GIS_M_AREA", "GIS_MARINE_AREA", "gis_m_area"],
  };

  const resolved: Record<string, string | null> = {};
  const missing: string[] = [];

  for (const [canonical, candidates] of Object.entries(mappingCandidates)) {
    const picked = pickField(fieldIndex, candidates);
    resolved[canonical] = picked;
    if (!picked) missing.push(canonical);
  }

  // Build SQL SET clauses
  const setClauses: string[] = [];

  function setIfPresent(targetCol: string, sourceField: string | null, cast?: string) {
    if (!sourceField) return;
    const src = quoteIdent(sourceField);
    if (cast) setClauses.push(`${targetCol} = (${src})::${cast}`);
    else setClauses.push(`${targetCol} = ${src}`);
  }

  // Required
  setIfPresent("wdpa_id", resolved.wdpa_id, "bigint");
  setIfPresent("wdpa_pid", resolved.wdpa_pid);
  setIfPresent("name", resolved.name);

  // Classification
  setIfPresent("designation", resolved.designation);
  setIfPresent("designation_type", resolved.designation_type);
  setIfPresent("iucn_category", resolved.iucn_category);
  setIfPresent("status", resolved.status);
  setIfPresent("status_year", resolved.status_year, "int");
  setIfPresent("gov_type", resolved.gov_type);
  setIfPresent("own_type", resolved.own_type);
  setIfPresent("mang_auth", resolved.mang_auth);
  setIfPresent("mang_plan", resolved.mang_plan);
  setIfPresent("verif", resolved.verif);

  // Geography
  setIfPresent("iso3", resolved.iso3);
  setIfPresent("parent_iso3", resolved.parent_iso3);
  setIfPresent("marine", resolved.marine);

  // Areas
  setIfPresent("reported_area_km2", resolved.reported_area_km2, "double precision");
  setIfPresent("gis_area_km2", resolved.gis_area_km2, "double precision");
  setIfPresent("gis_m_area_km2", resolved.gis_m_area_km2, "double precision");

  // Always set these
  setClauses.push(`wdpa_release = ${JSON.stringify(args.release)}`);
  setClauses.push(`source = 'wdpa'`);
  setClauses.push(`centroid = ST_Centroid(geom)`);

  // Output SQL
  const header = [
    `-- WDPA mapping SQL for layer: ${layer}`,
    `-- Release: ${args.release}`,
    `-- Fields detected (${fields.length}): ${fields.join(", ")}`,
    ``,
  ].join("\n");

  const updateSql = [
    `-- 1) Map WDPA fields into canonical columns`,
    `UPDATE protected_areas_staging`,
    `SET`,
    `  ${setClauses.join(",\n  ")}`,
    `;`,
    ``,
    `-- 2) For point geometries, centroid should equal geom`,
    `UPDATE protected_areas_staging`,
    `SET centroid = geom`,
    `WHERE GeometryType(geom) = 'POINT' OR GeometryType(geom) = 'MULTIPOINT';`,
    ``,
    `-- 3) Sanity check`,
    `SELECT COUNT(*) AS rows, MIN(wdpa_id) AS min_id, MAX(wdpa_id) AS max_id FROM protected_areas_staging;`,
    ``,
  ].join("\n");

  console.log(header + updateSql);

  if (missing.length) {
    console.error(
      `(warn) Missing mappings for: ${missing.join(", ")}. May be absent in this release.`
    );
  }

  const criticalMissing = ["wdpa_id", "name"].filter((k) => !resolved[k]);
  if (criticalMissing.length) {
    console.error(
      `(error) Critical fields missing: ${criticalMissing.join(", ")}. Wrong layer or file?`
    );
    process.exitCode = 2;
  }
}

main();
