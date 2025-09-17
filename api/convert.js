// api/convert.js
// Converts one feature (by ID) from the selected dataset into a KML download.
// Forces Node runtime to avoid Edge bundle issues with mapshaper.
export const config = { runtime: "nodejs18.x" };

import axios from "axios";

// IMPORTANT: dynamic import keeps Vercel bundling happy
async function getMapshaper() {
  // returns { applyCommands } from the ESM build
  const m = await import("mapshaper");
  // some bundles export default, some named — normalize
  return m.applyCommands ? m : (m.default || m);
}

// Fallback converter: GeoJSON -> KML (no styling), ensures you still get a file
import tokml from "tokml";

// ---------- DATASETS ----------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) | field: FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },

  // MDOT SHA Managed Landscape (Layer 0) | field: LOD_ID  (VPN required)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"]
  },

  // -------- MDOT SHA — TMDL (VPN required) --------
  // Auto-detect (search all TMDL layers by their ID fields)
  mdsha_tmdl_any: {
    // virtual dataset: we’ll try each concrete layer below
    any: true,
    layers: [
      "mdsha_tmdl_structures",
      "mdsha_tmdl_retrofits",
      "mdsha_tmdl_tree_plantings",
      "mdsha_tmdl_pavement_removals",
      "mdsha_tmdl_stream_restorations",
      "mdsha_tmdl_outfall_stabilizations"
    ]
  },

  // 0: Stormwater Control Structures
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },

  // 1: Retrofits
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },

  // 2: Tree Plantings
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"]
  },

  // 3: Pavement Removals
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"]
  },

  // 4: Stream Restorations
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"]
  },

  // 5: Outfall Stabilizations
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"]
  }
};

// Build a WHERE clause that tries multiple possible ID fields for the value
function buildWhere(idFields, value) {
  const esc = value.replace(/'/g, "''");
  return idFields.map(f => `${f}= '${esc}'`).join(" OR ");
}

// Query a single ArcGIS layer for a single assetId, returning GeoJSON FeatureCollection
async function queryLayerAsGeoJSON(base, idFields, assetId) {
  const url =
    `${base}/query` +
    `?where=${encodeURIComponent(buildWhere(idFields, assetId))}` +
    `&outFields=*` +
    `&returnGeometry=true` +
    `&outSR=4326` +
    `&f=geojson`;

  const { data } = await axios.get(url, { timeout: 20000 });
  if (!data || !data.features || !data.features.length) return null;

  // If multiple, keep first (you can change to union later if useful)
  return {
    type: "FeatureCollection",
    features: [data.features[0]]
  };
}

// Try a dataset key; if mdsha_tmdl_any, try all concrete TMDL layers in order
async function getFeatureCollection(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);

  if (ds.any) {
    for (const key of ds.layers) {
      const target = DATASETS[key];
      const fc = await queryLayerAsGeoJSON(target.base, target.idFields, assetId);
      if (fc) return fc;
    }
    return null;
  }

  return queryLayerAsGeoJSON(ds.base, ds.idFields, assetId);
}

// Turn a FeatureCollection into a KML Buffer using mapshaper; fallback to tokml
async function featureCollectionToKml(fc) {
  // Try mapshaper first
  try {
    const ms = await getMapshaper();

    const cmds = [
      "-i in.json",
      // ensure WGS84
      "-proj wgs84",
      // give a layer name and write kml
      "-o format=kml precision=0.000001 out.kml"
    ].join(" ");

    const input = { "in.json": Buffer.from(JSON.stringify(fc)) };
    const files = await ms.applyCommands(cmds, { "input-files": input });

    const out = files["out.kml"] || files["out.kmz"]; // depending on version
    if (!out) throw new Error("mapshaper output missing");
    return out; // Buffer
  } catch (err) {
    // Fallback: tokml (no styling, but reliable)
    const kmlText = tokml(fc, { name: "name" });
    return Buffer.from(kmlText, "utf8");
  }
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const text = Buffer.concat(chunks).toString("utf8");
  return JSON.parse(text || "{}");
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJsonBody(req);
    const assetId = (body.assetId || "").trim();
    const dataset = (body.dataset || "").trim();

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const fc = await getFeatureCollection(dataset, assetId);
    if (!fc) {
      res.status(404).json({ error: `No feature found for '${assetId}' in '${dataset}'` });
      return;
    }

    const kmlBuf = await featureCollectionToKml(fc);

    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}.kml"`);
    res.status(200).send(kmlBuf);
  } catch (e) {
    const msg = e?.message || String(e);
    res.status(500).json({ error: "Conversion failed", details: msg });
  }
}
