// api/locate.js
// Return centroid + Google Maps link for a feature (by ID or supplied GeoJSON)

import axios from "axios";
export const config = { runtime: "nodejs" };

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8");
  try { return JSON.parse(raw || "{}"); } catch { return {}; }
}

// server-side datasets
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },
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
      if (r.status === 200 && r.data?.features?.length) {
        return r.data.features[0]; // return a single Feature
      }
    } catch {}
  }
  return null;
}

// light-weight centroid (bbox center)
function getCentroid(feature) {
  const g = feature?.geometry;
  if (!g) return null;

  const push = (arr, x, y) => { arr[0] = Math.min(arr[0], x); arr[1] = Math.min(arr[1], y); arr[2] = Math.max(arr[2], x); arr[3] = Math.max(arr[3], y); };
  const bbox = [Infinity, Infinity, -Infinity, -Infinity];

  const walk = (coords) => {
    if (typeof coords[0] === "number") {
      push(bbox, coords[0], coords[1]);
    } else {
      for (const c of coords) walk(c);
    }
  };

  if (g.type === "Point") push(bbox, g.coordinates[0], g.coordinates[1]);
  else walk(g.coordinates);

  const [minx, miny, maxx, maxy] = bbox;
  return { lat: (miny + maxy) / 2, lng: (minx + maxx) / 2 };
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset, geojson? }" });
      return;
    }
    const body = await readJson(req);
    const assetId = body?.assetId?.trim();
    const dataset = body?.dataset?.trim();

    // If the browser already fetched GeoJSON and sent it along, use it
    let feature = body?.geojson?.features?.[0] || body?.geojson || null;

    if (!feature) {
      if (!assetId || !dataset) {
        res.status(400).json({ error: "Missing assetId/dataset or geojson" });
        return;
      }
      if (dataset === "mdsha_tmdl_any") {
        for (const key of TMDL_KEYS) {
          feature = await fetchOneFeatureAsGeoJSON(key, assetId);
          if (feature) break;
        }
      } else {
        feature = await fetchOneFeatureAsGeoJSON(dataset, assetId);
      }
    }

    if (!feature) {
      res.status(404).json({ error: `No feature found for ${assetId} in ${dataset}` });
      return;
    }

    const ctr = getCentroid(feature);
    const url = ctr ? `https://www.google.com/maps?q=${ctr.lat},${ctr.lng}` : null;

    res.status(200).json({ centroid: ctr, googleMapsUrl: url });
  } catch (err) {
    const msg = (err && err.message) ? err.message : String(err);
    res.status(500).json({ error: msg });
  }
}
