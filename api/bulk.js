// api/bulk.js
// Bulk: fetch many ArcGIS features by ID and return a zip containing
// one file per asset (KML / GeoJSON / Shapefile), plus mapbuddy_links.csv

import axios from "axios";
import AdmZip from "adm-zip";

// IMPORTANT: Node runtime (NOT Edge)
export const config = { runtime: "nodejs" };

// ---------- helpers ----------

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  if (!chunks.length) return {};
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8")); }
  catch { return {}; }
}

// mapshaper dynamic loader (works for both ESM/CJS shapes)
async function getApply() {
  const ms = await import("mapshaper");
  const apply = ms.applyCommands || (ms.default && ms.default.applyCommands) || null;
  if (!apply) throw new Error("Mapshaper API not available (applyCommands missing)");
  return apply;
}

const sleep = (ms) => new Promise(r => setTimeout(r, ms));
const isOk = (v) => v !== undefined && v !== null && v !== "";
const sanitizeName = (s) => String(s).replace(/[^\w\-]+/g, "_");

// super-light centroid to build a Google Maps link
function centroidOf(feature) {
  const g = feature?.geometry;
  if (!g) return null;
  const type = g.type;

  function avg(coords) {
    let sx = 0, sy = 0, n = 0;
    for (const [x, y] of coords) { sx += x; sy += y; n++; }
    return n ? [sx / n, sy / n] : null;
  }

  if (type === "Point") return { lng: g.coordinates[0], lat: g.coordinates[1] };

  if (type === "MultiPoint") {
    const c = avg(g.coordinates);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "LineString") {
    const c = avg(g.coordinates);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "MultiLineString") {
    const flat = g.coordinates.flat();
    const c = avg(flat);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "Polygon") {
    const ring = g.coordinates?.[0] || [];
    const c = avg(ring);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  if (type === "MultiPolygon") {
    const ring = g.coordinates?.[0]?.[0] || [];
    const c = avg(ring);
    return c ? { lng: c[0], lat: c[1] } : null;
  }

  return null;
}

function mapsUrl(lat, lng) {
  return `https://www.google.com/maps/search/?api=1&query=${lat},${lng}`;
}

// ---------- datasets ----------

const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities
  fairfax_bmps: {
    label: "Fairfax — Stormwater Facilities (FACILITY_ID)",
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },
  "Fairfax — Stormwater Facilities (FACILITY_ID)": {
    label: "Fairfax — Stormwater Facilities (FACILITY_ID)",
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape
  mdsha_landscape: {
    label: "MDOT SHA — Managed Landscape (LOD_ID)",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },
  "MDOT SHA — Managed Landscape (LOD_ID)": {
    label: "MDOT SHA — Managed Landscape (LOD_ID)",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // TMDL (MD SHA) - all keys included for both machine and label forms
  mdsha_tmdl_structures: {
    label: "TMDL — Stormwater Control Structures",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID", "ASSET_ID", "STRU_ID"],
  },
  "TMDL — Stormwater Control Structures": {
    label: "TMDL — Stormwater Control Structures",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID", "ASSET_ID", "STRU_ID"],
  },

  mdsha_tmdl_retrofits: {
    label: "TMDL — Retrofits",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID", "ASSET_ID", "STRU_ID"],
  },
  "TMDL — Retrofits": {
    label: "TMDL — Retrofits",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID", "ASSET_ID", "STRU_ID"],
  },

  mdsha_tmdl_tree_plantings: {
    label: "TMDL — Tree Plantings",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Tree Plantings": {
    label: "TMDL — Tree Plantings",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },

  mdsha_tmdl_pavement_removals: {
    label: "TMDL — Pavement Removals",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Pavement Removals": {
    label: "TMDL — Pavement Removals",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },

  mdsha_tmdl_stream_restorations: {
    label: "TMDL — Stream Restorations",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Stream Restorations": {
    label: "TMDL — Stream Restorations",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },

  mdsha_tmdl_outfall_stabilizations: {
    label: "TMDL — Outfall Stabilizations",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  "TMDL — Outfall Stabilizations": {
    label: "TMDL — Outfall Stabilizations",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },

  // -------------------------
  // Leesburg, VA — BMPs (ArcGIS Online)
  // Layer 51 under the LEESBURG FeatureServer
  // Accept OBJECTID first, but also try OBJECTID_1 and GlobalID just in case.
  leesburg_bmps: {
    label: "Leesburg — BMPs (OBJECTID)",
    base: "https://services1.arcgis.com/7owdfh5mgjEgbCSM/arcgis/rest/services/LEESBURG/FeatureServer/51",
    idFields: ["OBJECTID", "OBJECTID_1", "GlobalID"],
  },
  "Leesburg — BMPs (OBJECTID)": {
    label: "Leesburg — BMPs (OBJECTID)",
    base: "https://services1.arcgis.com/7owdfh5mgjEgbCSM/arcgis/rest/services/LEESBURG/FeatureServer/51",
    idFields: ["OBJECTID", "OBJECTID_1", "GlobalID"],
  }
  // -------------------------
};

// allow referencing by label or by key
function resolveDatasetKey(input) {
  if (DATASETS[input] && !DATASETS[input].alias) return input;
  // Try mapping machine keys to labels
  const machineToLabel = {
    mdsha_tmdl_tree_plantings: "TMDL — Tree Plantings",
    mdsha_tmdl_structures: "TMDL — Stormwater Control Structures",
    mdsha_tmdl_retrofits: "TMDL — Retrofits",
    mdsha_tmdl_pavement_removals: "TMDL — Pavement Removals",
    mdsha_tmdl_stream_restorations: "TMDL — Stream Restorations",
    mdsha_tmdl_outfall_stabilizations: "TMDL — Outfall Stabilizations",
    mdsha_landscape: "MDOT SHA — Managed Landscape (LOD_ID)",
    fairfax_bmps: "Fairfax — Stormwater Facilities (FACILITY_ID)",
    leesburg_bmps: "Leesburg — BMPs (OBJECTID)"
  };
  const mapped = machineToLabel[input];
  if (mapped && DATASETS[mapped]) return mapped;
  // match by label
  for (const [k, v] of Object.entries(DATASETS)) {
    if (!v.alias && (v.label === input)) return k;
  }
  return null;
}

// TMDL variant generator (as in single)
function tmdlVariants(assetId) {
  const s = String(assetId || "").trim().toUpperCase();
  const m = s.match(/^(\d+)\s*-?\s*([A-Z]{2,})$/);
  if (m) {
    const num = m[1], suf = m[2];
    return [ `${num}${suf}`, `${num}-${suf}`, `${num} ${suf}` ];
  }
  return [s, s.replace(/-/g,""), s.replace(/\s+/g,""), s.replace(/(\d+)([A-Za-z]+)/,'$1-$2'), s.replace(/(\d+)([A-Za-z]+)/,'$1 $2')];
}

// Try all id fields and variants, like single endpoint
async function fetchGeoJSONFeatureWithFallback(datasetKey, assetId) {
  const def = DATASETS[datasetKey];
  if (!def) return null;

  const useVariants = datasetKey.includes("TMDL");
  const variants = useVariants ? tmdlVariants(assetId) : [assetId];

  for (const fld of def.idFields || []) {
    for (const v of variants) {
      // Try f=geojson
      try {
        const p = new URLSearchParams({
          where: `${fld}='${v}'`,
          outFields: "*",
          returnGeometry: "true",
          outSR: "4326",
          f: "geojson",
        });
        const url = `${def.base}/query?${p.toString()}`;
        const r = await axios.get(url, { timeout: 15000 });
        const feats = r.data?.features || [];
        if (feats.length) return feats[0];
      } catch { /* fall through */ }

      // Fallback f=json
      try {
        const p = new URLSearchParams({
          where: `${fld}='${v}'`,
          outFields: "*",
          returnGeometry: "true",
          outSR: "4326",
          f: "json",
        });
        const url = `${def.base}/query?${p.toString()}`;
        const r = await axios.get(url, { timeout: 15000 });
        const feats = r.data?.features || [];
        if (feats.length) {
          const f = feats[0];
          const geom = f.geometry;
          let gj = null;
          if (geom?.x !== undefined && geom?.y !== undefined) {
            gj = { type: "Point", coordinates: [geom.x, geom.y] };
          } else if (geom?.rings) {
            gj = { type: "Polygon", coordinates: geom.rings };
          } else if (geom?.paths) {
            gj = { type: "LineString", coordinates: geom.paths[0] };
          }
          if (gj) {
            return {
              type: "Feature",
              properties: f.attributes || {},
              geometry: gj,
            };
          }
        }
      } catch { /* continue */ }
      await sleep(80);
    }
  }

  return null;
}

// ---------- converters ----------

async function featureToKmlBuffer(feature) {
  const apply = await getApply();
  const files = { "in.json": JSON.stringify(feature) };
  const out = await apply("-i in.json -o out.kml format=kml", files);
  return Buffer.from(out["out.kml"]);
}

async function featureToShapefileParts(feature, outBase) {
  const apply = await getApply();
  const files = { "in.json": JSON.stringify(feature) };
  const out = await apply("-i in.json -o out.shp format=shapefile", files);
  const parts = [];
  for (const [name, content] of Object.entries(out)) {
    const renamed = name.replace(/^out(\.|$)/, `${outBase}$1`);
    parts.push([renamed, Buffer.from(content)]);
  }
  return parts;
}

// ---------- main handler ----------

export default async function handler(req, res) {
  if (req.method !== "POST") {
    res.status(405).json({ error: "Use POST with JSON body { items, defaultDataset, format }" });
    return;
  }

  const body = await readJsonBody(req);
  const items = Array.isArray(body.items) ? body.items : [];
  let defDataset = body.defaultDataset;
  const format = String(body.format || "").toLowerCase();

  if (!items.length) {
    res.status(400).json({ error: "No items provided" });
    return;
  }

  if (defDataset) {
    defDataset = resolveDatasetKey(defDataset) || defDataset;
  }

  const zip = new AdmZip();
  const linkRows = [["assetId", "dataset", "lat", "lng", "googleMapsUrl", "status"]];
  let successCount = 0;

  for (const it of items) {
    const assetId = String(it.assetId || "").trim();
    if (!assetId) continue;

    let datasetKey = it.dataset ? (resolveDatasetKey(it.dataset) || it.dataset) : defDataset;
    if (!datasetKey) {
      datasetKey = assetId.endsWith("UT")
        ? "TMDL — Tree Plantings"
        : "Fairfax — Stormwater Facilities (FACILITY_ID)";
    }

    const ds = DATASETS[datasetKey];
    if (!ds || ds.alias) {
      linkRows.push([assetId, datasetKey || "", "", "", "", "unknown dataset"]);
      continue;
    }

    let feature = null;
    try {
      feature = await fetchGeoJSONFeatureWithFallback(datasetKey, assetId);
    } catch (e) {
      // swallow, will be handled below
    }

    if (!feature) {
      linkRows.push([assetId, datasetKey, "", "", "", "not found"]);
      continue;
    }

    const c = centroidOf(feature);
    if (c) {
      linkRows.push([assetId, datasetKey, String(c.lat), String(c.lng), mapsUrl(c.lat, c.lng), "ok"]);
    } else {
      linkRows.push([assetId, datasetKey, "", "", "", "ok (no centroid)"]);
    }

    const base = sanitizeName(assetId);

    try {
      if (format === "kmlzip") {
        const buf = await featureToKmlBuffer(feature);
        zip.addFile(`${base}.kml`, buf);
      } else if (format === "geojsonzip") {
        zip.addFile(`${base}.geojson`, Buffer.from(JSON.stringify(feature)));
      } else if (format === "shpzip") {
        const parts = await featureToShapefileParts(feature, base);
        for (const [name, buf] of parts] zip.addFile(name, buf);
      } else if (format === "kml" || format === "geojson") {
        // merged single file (non-zip) — not supported in bulk
      } else {
        const buf = await featureToKmlBuffer(feature);
        zip.addFile(`${base}.kml`, buf);
      }

      successCount++;
    } catch (e) {
      linkRows.push([assetId, datasetKey, "", "", "", "conversion error"]);
    }
  }

  if (format === "kml" || format === "geojson") {
    res.status(400).json({ error: "For bulk, use kmlzip, geojsonzip or shpzip." });
    return;
  }

  const csv = linkRows.map(r => r.map(v => /[",\n]/.test(v) ? `"${String(v).replace(/"/g, '""')}"` : v).join(",")).join("\n");
  zip.addFile("mapbuddy_links.csv", Buffer.from(csv, "utf8"));

  if (!successCount) {
    res.status(404).json({ error: "No features found for supplied items." });
    return;
  }

  const zipName =
    format === "geojsonzip" ? "mapbuddy_geojson.zip" :
    format === "shpzip"     ? "mapbuddy_shapefile.zip" :
                              "mapbuddy_kml.zip";

  res.setHeader("Content-Type", "application/zip");
  res.setHeader("Content-Disposition", `attachment; filename=${zipName}`);
  res.status(200).send(zip.toBuffer());
}
