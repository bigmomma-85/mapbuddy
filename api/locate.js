// api/locate.js
// Looks up an asset/BMP ID in the chosen dataset, finds the geometry centroid,
// and returns a Google Maps link. (Node runtime is required on Vercel.)
export const config = { runtime: "nodejs18.x" };

import axios from "axios";

/* --------------------------- DATASETS (same keys as convert.js) --------------------------- */
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) | field: FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"],
  },

  // MDOT SHA Managed Landscape (Layer 0) | field: LOD_ID  (VPN may be required)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"],
  },

  // -------- MDOT SHA — TMDL (VPN may be required) --------
  // Virtual dataset that tries all TMDL layers until it finds a match
  mdsha_tmdl_any: {
    any: true,
    layers: [
      "mdsha_tmdl_structures",
      "mdsha_tmdl_retrofits",
      "mdsha_tmdl_tree_plantings",
      "mdsha_tmdl_pavement_removals",
      "mdsha_tmdl_stream_restorations",
      "mdsha_tmdl_outfall_stabilizations",
    ],
  },

  // 0: Stormwater Control Structures
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },

  // 1: Retrofits
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"],
  },

  // 2: Tree Plantings
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 3: Pavement Removals
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 4: Stream Restorations
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },

  // 5: Outfall Stabilizations
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID"],
  },
};
/* ----------------------------------------------------------------------------------------- */

// Build a WHERE clause that tries multiple fields for a single value
function buildWhere(idFields, value) {
  const esc = String(value).replace(/'/g, "''");
  return idFields.map((f) => `${f}= '${esc}'`).join(" OR ");
}

// GET a single GeoJSON feature (first match) from one layer
async function querySingleFeatureGeoJSON(base, idFields, assetId) {
  const url =
    `${base}/query` +
    `?where=${encodeURIComponent(buildWhere(idFields, assetId))}` +
    `&outFields=*` +
    `&returnGeometry=true` +
    `&outSR=4326` +
    `&f=geojson`;

  const { data } = await axios.get(url, { timeout: 20000 });
  if (!data || !data.features || !data.features.length) return null;

  // Return the first match with some metadata about which field hit, if we can detect it
  const feat = data.features[0];
  const matchedField = idFields.find((f) => {
    const v = feat.properties?.[f];
    return v != null && String(v).toUpperCase() === String(assetId).toUpperCase();
  }) || idFields[0];

  return { feature: feat, matchedField };
}

// Try a specific dataset key; if "any", iterate through concrete TMDL layers
async function findFeature(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);

  if (ds.any) {
    for (const key of ds.layers) {
      const concrete = DATASETS[key];
      const hit = await querySingleFeatureGeoJSON(concrete.base, concrete.idFields, assetId);
      if (hit) return { ...hit, datasetUsed: key };
    }
    return null;
  }

  const hit = await querySingleFeatureGeoJSON(ds.base, ds.idFields, assetId);
  return hit ? { ...hit, datasetUsed: datasetKey } : null;
}

/* ------------------------------ Geometry → centroid helpers ------------------------------ */

function centroidOfCoords(coords) {
  // coords = [[x,y], [x,y], ...]
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const [x, y] of coords) {
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  }
  return [(minX + maxX) / 2, (minY + maxY) / 2];
}

function getCentroidFromGeometry(geom) {
  if (!geom) return null;

  const { type, coordinates } = geom;

  if (type === "Point") {
    // [lng, lat]
    return { lng: coordinates[0], lat: coordinates[1] };
  }

  if (type === "MultiPoint" || type === "LineString") {
    const [lng, lat] = centroidOfCoords(coordinates);
    return { lng, lat };
  }

  if (type === "MultiLineString") {
    const flat = coordinates.flat();
    const [lng, lat] = centroidOfCoords(flat);
    return { lng, lat };
  }

  if (type === "Polygon") {
    // outer ring
    const ring = coordinates[0] || [];
    const [lng, lat] = centroidOfCoords(ring);
    return { lng, lat };
  }

  if (type === "MultiPolygon") {
    // take first polygon’s outer ring for a quick, robust centroid
    const ring = coordinates[0]?.[0] || [];
    const [lng, lat] = centroidOfCoords(ring);
    return { lng, lat };
  }

  return null;
}

/* ------------------------------------ HTTP handler --------------------------------------- */

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const text = Buffer.concat(chunks).toString("utf8");
  return JSON.parse(text || "{}");
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res
        .status(405)
        .json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJsonBody(req);
    const assetId = (body.assetId || "").trim();
    const dataset = (body.dataset || "").trim();

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const hit = await findFeature(dataset, assetId);
    if (!hit) {
      res
        .status(404)
        .json({ error: `No feature found for '${assetId}' in '${dataset}'` });
      return;
    }

    const centroid = getCentroidFromGeometry(hit.feature.geometry);
    if (!centroid) {
      res.status(500).json({ error: "Could not compute centroid" });
      return;
    }

    const googleMapsUrl = `https://www.google.com/maps?q=${centroid.lat},${centroid.lng}`;

    res.status(200).json({
      centroid,
      googleMapsUrl,
      datasetUsed: hit.datasetUsed,
      matchedField: hit.matchedField,
    });
  } catch (e) {
    const msg = e?.message || String(e);
    // When the SHA services require VPN and it’s unreachable you may see timeouts here.
    res.status(500).json({ error: "Locate failed", details: msg });
  }
}
