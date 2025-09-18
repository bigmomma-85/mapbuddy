// api/convert.js
// Convert a single ArcGIS feature (by ID or supplied GeoJSON) to KML using Mapshaper

import axios from "axios";

// IMPORTANT: ensure we run on the Node runtime (NOT Edge)
export const config = { runtime: "nodejs" };

// ---------- small utils ----------

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

// Try both ESM/CJS shapes for mapshaper so we always have a callable function
async function getMapshaperApply() {
  const ms = await import("mapshaper");
  const mod = ms.default || ms;
  const apply =
    mod.applyCommands ||
    mod.runCommands ||
    (ms.applyCommands || ms.runCommands) ||
    null;

  if (!apply) {
    throw new Error("Mapshaper API not available (apply/runCommands missing)");
  }
  return { mod, apply };
}

function kmlFromMapshaperOutput(output) {
  // Mapshaper returns an object keyed by filenames
  const known = ["out.kml", "output.kml"];
  for (const k of known) if (output[k]) return output[k];
  // otherwise find any .kml
  for (const [k, v] of Object.entries(output)) {
    if (k.toLowerCase().endsWith(".kml")) return v;
  }
  throw new Error("KML not found in Mapshaper output");
}

// ---------- datasets (server side) ----------

const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape (Layer 0)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // TMDL layers
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"],
  },
};

// order used by the "TMDL (Auto-detect)" option
const TMDL_KEYS = [
  "mdsha_tmdl_structures",
  "mdsha_tmdl_retrofits",
  "mdsha_tmdl_tree_plantings",
  "mdsha_tmdl_pavement_removals",
  "mdsha_tmdl_stream_restorations",
  "mdsha_tmdl_outfall_stabilizations",
];

function arcgisGeojsonUrl(base, field, value) {
  const where = encodeURIComponent(`${field}='${value}'`);
  return `${base}/query?where=${where}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
}

async function fetchOneFeatureAsGeoJSON(datasetKey, assetId) {
  const cfg = DATASETS[datasetKey];
  if (!cfg) return null;

  for (const f of cfg.idFields) {
    const url = arcgisGeojsonUrl(cfg.base, f, assetId);
    try {
      const r = await axios.get(url, { timeout: 15000, validateStatus: () => true });
      if (r.status === 200 && r.data && r.data.features && r.data.features.length) {
        // return one feature as a FeatureCollection
        return { type: "FeatureCollection", features: [r.data.features[0]] };
      }
    } catch { /* try next idField */ }
  }
  return null;
}

// ---------- main ----------

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset, geojson? }" });
      return;
    }

    const body = await readJson(req);
    const assetId = body?.assetId?.trim();
    const dataset = body?.dataset?.trim();
    let fc = body?.geojson || null; // browser-first path may send us GeoJSON directly

    // Fallback: fetch from ArcGIS on the server if we don't have FC yet
    if (!fc) {
      if (!assetId || !dataset) {
        res.status(400).json({ error: "Missing assetId/dataset or geojson" });
        return;
      }
      if (dataset === "mdsha_tmdl_any") {
        for (const key of TMDL_KEYS) {
          fc = await fetchOneFeatureAsGeoJSON(key, assetId);
          if (fc) break;
        }
      } else {
        fc = await fetchOneFeatureAsGeoJSON(dataset, assetId);
      }
      if (!fc) {
        res.status(404).json({ error: `No feature found for ${assetId} in ${dataset}` });
        return;
      }
    }

    // Mapshaper → KML
    const { mod, apply } = await getMapshaperApply();
    const files = { "in.json": JSON.stringify(fc) };
    const cmds = `-i in.json -o format=kml out.kml`;

    // Support callback AND promise styles
    const output = await (mod.applyCommands
      ? new Promise((resolve, reject) => {
          mod.applyCommands(cmds, files, (err, out) => (err ? reject(err) : resolve(out)));
        })
      : mod.runCommands(cmds, files)
    );

    const kmlText = kmlFromMapshaperOutput(output);
    const kmlBuffer = Buffer.from(kmlText, "utf8");

    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    const name = assetId ? assetId.replace(/[^\w.-]+/g, "_") : "feature";
    res.setHeader("Content-Disposition", `attachment; filename="${name}.kml"`);
    res.status(200).send(kmlBuffer);
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    res.status(500).json({ error: `Conversion failed: ${msg}` });
  }
}
