// api/convert.js
// Node/ESM (Vercel Node.js runtime)
// Converts ArcGIS features (GeoJSON or Esri JSON) to a downloadable KML using MapShaper.

import axios from "axios";
import * as mapshaper from "mapshaper";

/** -----------------------------
 * Dataset registry
 * ------------------------------
 * - Each dataset has a base MapServer layer URL (no trailing /query)
 * - We don't hardcode a field; we auto-detect available ID fields from the layer metadata
 * - "mdsha_tmdl_any" tries all 6 TMDL layers in order until one matches
 */
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7)
  fairfax_bmps: {
    label: "Fairfax — Stormwater Facilities",
    type: "single",
    layers: [
      {
        base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
      },
    ],
  },

  // MDOT SHA — TMDL Bay Restoration (Layers 0..5)
  // 0: Structures, 1: Retrofits, 2: Tree Plantings, 3: Pavement Removals, 4: Stream Restorations, 5: Outfall Stabilizations
  mdsha_tmdl_structures: {
    label: "MD SHA — TMDL Structures",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0" }],
  },
  mdsha_tmdl_retrofits: {
    label: "MD SHA — TMDL Retrofits",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1" }],
  },
  mdsha_tmdl_tree_plantings: {
    label: "MD SHA — TMDL Tree Plantings",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2" }],
  },
  mdsha_tmdl_pavement_removals: {
    label: "MD SHA — TMDL Pavement Removals",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3" }],
  },
  mdsha_tmdl_stream_restorations: {
    label: "MD SHA — TMDL Stream Restorations",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4" }],
  },
  mdsha_tmdl_outfall_stabilizations: {
    label: "MD SHA — TMDL Outfall Stabilizations",
    type: "single",
    layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5" }],
  },

  // Tries all 6 TMDL layers in order; stops at the first match
  mdsha_tmdl_any: {
    label: "MD SHA — TMDL (Auto-detect)",
    type: "multi",
    layers: [
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0" },
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1" },
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2" },
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3" },
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4" },
      { base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5" },
    ],
  },
};

// Candidate fields we’ll try to match against (we’ll filter to what's actually on the layer)
const CANDIDATE_ID_FIELDS = [
  "FACILITY_ID",
  "LOD_ID",
  "STRU_ID",
  "STRUCTURE_ID",
  "ASSET_ID",
  "SWM_FAC_NO",
  "NAME",
  "PROJECT_ID",
];

// Small helper: URL query building
const q = (params) =>
  Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");

// Fetch layer metadata to discover field names (so we build a valid WHERE)
async function fetchLayerMeta(base) {
  const url = `${base}?f=pjson`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data; // contains .fields[]
}

// Build a robust WHERE across available fields on the layer
function buildWhereClause(id, metaFields) {
  const available = new Set(metaFields.map((f) => f.name.toUpperCase()));
  const fields = CANDIDATE_ID_FIELDS.filter((f) => available.has(f));
  if (fields.length === 0) return null;

  const esc = id.replace(/'/g, "''");
  const equals = fields.map((f) => `${f}='${esc}'`);
  const likes = fields.map((f) => `UPPER(${f}) LIKE '%${esc.toUpperCase()}%'`);
  return `(${equals.join(" OR ")}) OR (${likes.join(" OR ")})`;
}

// Try GeoJSON first (some servers support it), then fallback to Esri JSON
async function fetchFeatureAsGeoJSON(base, where) {
  // 1) try f=geojson
  const gjUrl = `${base}/query?${q({
    where,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "geojson",
  })}`;

  try {
    const { data } = await axios.get(gjUrl, { timeout: 25000 });
    if (data && data.type === "FeatureCollection" && data.features?.length) {
      return { geojson: data, format: "geojson" };
    }
  } catch {
    // ignore and fallback
  }

  // 2) fallback to Esri JSON (f=json) and convert to GeoJSON
  const esriUrl = `${base}/query?${q({
    where,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "json",
  })}`;

  const { data: esri } = await axios.get(esriUrl, { timeout: 25000 });
  if (!esri || !esri.features || !esri.features.length) {
    return { geojson: null, format: "esri-json" };
  }

  const geojson = esriToGeoJSON(esri);
  return { geojson, format: "esri-json" };
}

// Minimal Esri JSON → GeoJSON converter (points, lines, polygons)
function esriToGeoJSON(esri) {
  const fc = {
    type: "FeatureCollection",
    features: [],
  };

  const gType = (esri.geometryType || "").toLowerCase();
  const toFeat = (esriGeom, attrs) => {
    let geometry = null;

    if (gType.includes("point")) {
      geometry = {
        type: "Point",
        coordinates: [esriGeom.x, esriGeom.y],
      };
    } else if (gType.includes("polyline")) {
      // paths: [ [ [x,y], [x,y], ... ], ... ]
      geometry = {
        type: "MultiLineString",
        coordinates: esriGeom.paths,
      };
      // if single path, simplify to LineString
      if (esriGeom.paths?.length === 1) {
        geometry = { type: "LineString", coordinates: esriGeom.paths[0] };
      }
    } else if (gType.includes("polygon")) {
      // rings: [ [ [x,y], [x,y], ... (closed) ], ... ]
      geometry = {
        type: "MultiPolygon",
        coordinates: esriGeom.rings.map((ring) => [ring]),
      };
      // if only one ring, simplify to Polygon
      if (esriGeom.rings?.length === 1) {
        geometry = { type: "Polygon", coordinates: [esriGeom.rings[0]] };
      }
    }

    return { type: "Feature", properties: attrs || {}, geometry };
  };

  for (const f of esri.features) {
    const geom = f.geometry || {};
    fc.features.push(toFeat(geom, f.attributes || {}));
  }
  return fc;
}

// Convert GeoJSON → KML using MapShaper (in-memory)
async function geojsonToKml(geojson) {
  const input = [{ name: "in.geojson", content: JSON.stringify(geojson) }];
  const cmd = "-i in.geojson -o format=kml precision=0.000001 out.kml";
  const outputs = await mapshaper.applyCommands(cmd, input);
  const kml = outputs["out.kml"]; // Uint8Array
  return Buffer.from(kml);
}

// Try a single layer; return {kml, usedLayer} or null
async function tryLayer(base, assetId) {
  const meta = await fetchLayerMeta(base);
  const where = buildWhereClause(assetId, meta.fields || []);
  if (!where) return null;

  const { geojson } = await fetchFeatureAsGeoJSON(base, where);
  if (!geojson || !geojson.features?.length) return null;

  const kml = await geojsonToKml(geojson);
  return { kml, usedLayer: base, featureCount: geojson.features.length };
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const { assetId, dataset } = req.body || {};
    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const ds = DATASETS[dataset];
    if (!ds) {
      res.status(400).json({ error: `Unknown dataset '${dataset}'` });
      return;
    }

    let result = null;

    if (ds.type === "single") {
      result = await tryLayer(ds.layers[0].base, assetId);
    } else if (ds.type === "multi") {
      for (const lyr of ds.layers) {
        result = await tryLayer(lyr.base, assetId);
        if (result) break;
      }
    }

    if (!result) {
      // Helpful VPN hint if SHA hostname is present
      const vpnHint = ds.layers.some((l) => l.base.includes("maps.roads.maryland.gov"))
        ? " (If you are on MDOT SHA datasets, make sure VPN is connected.)"
        : "";
      res.status(404).json({ error: `No feature found for '${assetId}' in ${dataset}.${vpnHint}` });
      return;
    }

    const filename = `${assetId}.kml`;
    res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
    res.setHeader("Content-Disposition", `attachment; filename="${filename}"`);
    res.setHeader("X-Dataset-Used", result.usedLayer);
    res.send(result.kml);
  } catch (err) {
    // MapShaper and Axios errors can be noisy; trim the message
    const msg = err?.response?.data?.error?.message || err?.message || "Unknown error";
    res.status(500).json({ error: `Conversion failed: ${msg}` });
  }
}
