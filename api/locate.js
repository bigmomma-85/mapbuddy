// /api/locate.js
// Looks up a feature (dataset+assetId) OR accepts raw GeoJSON, and returns centroid,
// Google Maps link, and simple bounds — for your UI to show a preview/link.

export const config = {
  api: {
    bodyParser: false,
  },
};

// ---------- Utilities ----------
function enableCORS(res) {
  res.setHeader("Access-Control-Allow-Origin", "*");
  res.setHeader("Access-Control-Allow-Methods", "POST,OPTIONS");
  res.setHeader("Access-Control-Allow-Headers", "Content-Type");
}

async function readJsonBody(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  const raw = Buffer.concat(chunks).toString("utf8") || "{}";
  try {
    return JSON.parse(raw);
  } catch {
    throw new Error("Invalid JSON body");
  }
}

function buildWhere(idFields, input) {
  const safe = String(input ?? "").replace(/'/g, "''");
  if (!safe) return "1=2";
  const exact = idFields.map(f => `${encodeURIComponent(f)}='${encodeURIComponent(safe)}'`);
  const like  = idFields.map(f => `${encodeURIComponent(f)} like '%25${encodeURIComponent(safe)}%25'`);
  return `(${exact.join(" OR ")}) OR (${like.join(" OR ")})`;
}

async function fetchArcgisGeoJSON(base, where) {
  const url =
    `${base}/query?where=${where}` +
    `&outFields=*` +
    `&returnGeometry=true&outSR=4326&f=geojson`;
  const r = await fetch(url);
  if (!r.ok) throw new Error(`ArcGIS request failed (${r.status})`);
  return await r.json();
}

function isFeatureCollection(g) {
  return g && g.type === "FeatureCollection" && Array.isArray(g.features);
}

function centroidOfFeatureCollection(fc) {
  const pts = [];

  function pushCoord([x, y]) {
    if (Number.isFinite(x) && Number.isFinite(y)) pts.push([x, y]);
  }
  function walkGeom(geom) {
    if (!geom) return;
    const { type, coordinates } = geom;
    if (!coordinates) return;
    switch (type) {
      case "Point": pushCoord(coordinates); break;
      case "MultiPoint":
      case "LineString": coordinates.forEach(pushCoord); break;
      case "MultiLineString":
      case "Polygon": coordinates.flat(1).forEach(pushCoord); break;
      case "MultiPolygon": coordinates.flat(2).forEach(pushCoord); break;
      default: break;
    }
  }
  for (const f of fc.features || []) walkGeom(f.geometry);
  if (!pts.length) return null;
  const [sx, sy] = pts.reduce((acc, [x, y]) => [acc[0] + x, acc[1] + y], [0, 0]);
  const cx = sx / pts.length, cy = sy / pts.length;
  return { lng: cx, lat: cy };
}

function boundsOfFeatureCollection(fc) {
  let minX = Infinity, minY = Infinity, maxX = -Infinity, maxY = -Infinity;

  function expand([x, y]) {
    if (!Number.isFinite(x) || !Number.isFinite(y)) return;
    if (x < minX) minX = x;
    if (y < minY) minY = y;
    if (x > maxX) maxX = x;
    if (y > maxY) maxY = y;
  }
  function walk(geom) {
    if (!geom) return;
    const { type, coordinates } = geom;
    if (!coordinates) return;
    switch (type) {
      case "Point": expand(coordinates); break;
      case "MultiPoint":
      case "LineString": coordinates.forEach(expand); break;
      case "MultiLineString":
      case "Polygon": coordinates.flat(1).forEach(expand); break;
      case "MultiPolygon": coordinates.flat(2).forEach(expand); break;
      default: break;
    }
  }
  for (const f of fc.features || []) walk(f.geometry);
  if ([minX, minY, maxX, maxY].some(v => !Number.isFinite(v))) return null;
  return { west: minX, south: minY, east: maxX, north: maxY };
}

function googleMapsLinkFromCentroid(c) {
  if (!c) return null;
  return `https://www.google.com/maps?q=${c.lat},${c.lng}&z=19`;
}

// ---------- DATASETS (same as convert.js) ----------
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID", "SITE_ID", "SITE_CODE", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  }
};

// ---------- Handler ----------
export default async function handler(req, res) {
  enableCORS(res);
  if (req.method === "OPTIONS") return res.status(200).end();

  try {
    if (req.method !== "POST") {
      return res.status(405).json({ error: "Use POST with JSON body { assetId?, dataset?, geojson?, geojsonUrl? }" });
    }

    const body = await readJsonBody(req);
    const { assetId, dataset, geojson, geojsonUrl } = body;

    let fc;
    let whereUsed = null;

    if (geojson || geojsonUrl) {
      const data = geojson
        ? (typeof geojson === "string" ? JSON.parse(geojson) : geojson)
        : await (await fetch(geojsonUrl)).json();

      if (!isFeatureCollection(data)) {
        return res.status(400).json({ error: "Provided GeoJSON is not a FeatureCollection" });
      }
      fc = data;
    } else {
      const cfg = DATASETS[dataset];
      if (!cfg) return res.status(400).json({ error: `Unknown dataset: ${dataset}` });
      if (!assetId) return res.status(400).json({ error: "assetId is required when not sending geojson/geojsonUrl" });

      const where = buildWhere(cfg.idFields, assetId);
      whereUsed = where;
      const data = await fetchArcgisGeoJSON(cfg.base, where);
      if (!isFeatureCollection(data) || !data.features.length) {
        return res.status(404).json({ error: "No feature found", where, dataset });
      }
      fc = data;
    }

    const center = centroidOfFeatureCollection(fc);
    const bounds = boundsOfFeatureCollection(fc);
    const mapsUrl = googleMapsLinkFromCentroid(center);

    return res.status(200).json({
      ok: true,
      featureCount: fc.features?.length || 0,
      centroid: center,
      bounds,
      googleMapsUrl: mapsUrl,
      where: whereUsed ? decodeURIComponent(whereUsed) : undefined
    });
  } catch (err) {
    console.error("locate error:", err);
    return res.status(500).json({ error: "Locate failed", details: String(err?.message || err) });
  }
}
