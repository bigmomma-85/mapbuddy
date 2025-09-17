// api/locate.js
// Returns a centroid + Google Maps link for a given assetId & dataset.
// Shares the same dataset registry logic as convert.js (keep them in sync).

import axios from "axios";

const DATASETS = {
  fairfax_bmps: {
    type: "single",
    layers: [{ base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7" }],
  },
  mdsha_tmdl_structures: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0" }] },
  mdsha_tmdl_retrofits: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1" }] },
  mdsha_tmdl_tree_plantings: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2" }] },
  mdsha_tmdl_pavement_removals: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3" }] },
  mdsha_tmdl_stream_restorations: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4" }] },
  mdsha_tmdl_outfall_stabilizations: { type: "single", layers: [{ base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5" }] },

  mdsha_tmdl_any: {
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

const q = (params) =>
  Object.entries(params)
    .map(([k, v]) => `${encodeURIComponent(k)}=${encodeURIComponent(v)}`)
    .join("&");

async function fetchLayerMeta(base) {
  const url = `${base}?f=pjson`;
  const { data } = await axios.get(url, { timeout: 20000 });
  return data;
}

function buildWhereClause(id, metaFields) {
  const available = new Set(metaFields.map((f) => f.name.toUpperCase()));
  const fields = CANDIDATE_ID_FIELDS.filter((f) => available.has(f));
  if (fields.length === 0) return null;

  const esc = id.replace(/'/g, "''");
  const equals = fields.map((f) => `${f}='${esc}'`);
  const likes = fields.map((f) => `UPPER(${f}) LIKE '%${esc.toUpperCase()}%'`);
  return `(${equals.join(" OR ")}) OR (${likes.join(" OR ")})`;
}

async function fetchFeature(base, where) {
  // Prefer GeoJSON, fallback to Esri JSON
  const gjUrl = `${base}/query?${q({
    where,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "geojson",
  })}`;

  try {
    const { data } = await axios.get(gjUrl, { timeout: 25000 });
    if (data?.features?.length) {
      return data.features[0];
    }
  } catch {
    // ignore; fallback
  }

  const esriUrl = `${base}/query?${q({
    where,
    outFields: "*",
    returnGeometry: "true",
    outSR: "4326",
    f: "json",
  })}`;

  const { data: esri } = await axios.get(esriUrl, { timeout: 25000 });
  if (!esri?.features?.length) return null;

  // Convert centroid from Esri geometry
  const feat = esri.features[0];
  const centroid = computeEsriCentroid(feat.geometry, esri.geometryType);
  return {
    type: "Feature",
    properties: feat.attributes || {},
    geometry: centroid ? { type: "Point", coordinates: centroid } : null,
  };
}

function computeEsriCentroid(geom, gType) {
  if (!geom) return null;
  const t = (gType || "").toLowerCase();

  if (t.includes("point")) return [geom.x, geom.y];

  if (t.includes("polyline") && geom.paths?.length) {
    const coords = geom.paths.flat();
    const [xAvg, yAvg] = avgXY(coords);
    return [xAvg, yAvg];
  }

  if (t.includes("polygon") && geom.rings?.length) {
    // average of first ring
    const ring = geom.rings[0];
    const [xAvg, yAvg] = avgXY(ring);
    return [xAvg, yAvg];
  }
  return null;
}

function avgXY(points) {
  let sx = 0,
    sy = 0;
  const n = points.length;
  for (const [x, y] of points) {
    sx += x;
    sy += y;
  }
  return [sx / n, sy / n];
}

async function tryLayer(base, assetId) {
  const meta = await fetchLayerMeta(base);
  const where = buildWhereClause(assetId, meta.fields || []);
  if (!where) return null;

  const feature = await fetchFeature(base, where);
  if (!feature) return null;

  const [lng, lat] = feature.geometry?.coordinates || [];
  return { lat, lng, usedLayer: base, props: feature.properties || {} };
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
    } else {
      for (const lyr of ds.layers) {
        result = await tryLayer(lyr.base, assetId);
        if (result) break;
      }
    }

    if (!result) {
      const vpnHint = ds.layers.some((l) => l.base.includes("maps.roads.maryland.gov"))
        ? " (If you are on MDOT SHA datasets, make sure VPN is connected.)"
        : "";
      res.status(404).json({ error: `No feature found for '${assetId}' in ${dataset}.${vpnHint}` });
      return;
    }

    const gmaps = `https://www.google.com/maps/search/?api=1&query=${result.lat},${result.lng}`;
    res.status(200).json({
      assetId,
      dataset,
      lat: result.lat,
      lng: result.lng,
      gmaps,
      usedLayer: result.usedLayer,
      properties: result.props,
    });
  } catch (err) {
    const msg = err?.response?.data?.error?.message || err?.message || "Unknown error";
    res.status(500).json({ error: `Locate failed: ${msg}` });
  }
}
