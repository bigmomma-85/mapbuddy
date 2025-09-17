// api/locate.js
import axios from "axios";

export const config = { runtime: "nodejs" };

// Robust JSON body reader
async function readJsonBody(req) {
  if (req.body && typeof req.body === "object") return req.body;
  if (typeof req.body === "string") {
    try { return JSON.parse(req.body); } catch { return {}; }
  }
  return await new Promise((resolve) => {
    let data = "";
    req.on("data", (c) => (data += c));
    req.on("end", () => { try { resolve(JSON.parse(data || "{}")); } catch { resolve({}); } });
  });
}

// Same datasets map as convert.js (BMPs are in the Fairfax layer 7 with FACILITY_ID)
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },
};

// --- Geometry helpers: compute a safe bbox + centroid for any geometry ---
function expandBbox(b, x, y) {
  if (!b) return [x, y, x, y];
  if (x < b[0]) b[0] = x;
  if (y < b[1]) b[1] = y;
  if (x > b[2]) b[2] = x;
  if (y > b[3]) b[3] = y;
  return b;
}

function scanCoords(coords, bbox) {
  // coords can be [x,y], or nested arrays; walk recursively
  if (typeof coords[0] === "number" && typeof coords[1] === "number") {
    return expandBbox(bbox, coords[0], coords[1]);
  }
  for (const c of coords) bbox = scanCoords(c, bbox);
  return bbox;
}

function centroidOfGeometry(geom) {
  if (!geom) return null;

  // Handle Points directly
  if (geom.type === "Point") {
    const [x, y] = geom.coordinates;
    return { lng: x, lat: y };
  }

  // Generic: bbox center for (Multi)Point, (Multi)LineString, (Multi)Polygon, Polygon, etc.
  let bbox = null;
  bbox = scanCoords(geom.coordinates, bbox);
  if (!bbox) return null;
  const [minX, minY, maxX, maxY] = bbox;
  return { lng: (minX + maxX) / 2, lat: (minY + maxY) / 2 };
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJsonBody(req);
    const assetId = body?.assetId;
    const dataset = body?.dataset;

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const cfg = DATASETS[dataset];
    if (!cfg) {
      res.status(400).json({ error: `Unsupported dataset '${dataset}'` });
      return;
    }

    const safeId = String(assetId).replace(/'/g, "''");
    const where = encodeURIComponent(cfg.idField) + "='" + encodeURIComponent(safeId) + "'";
    const url = `${cfg.base}/query?where=${where}&outFields=*&f=geojson`;

    const { data } = await axios.get(url, { timeout: 20000 });

    if (!data || !data.features || data.features.length === 0) {
      res.status(404).json({ error: `No feature found for ${assetId} in ${dataset}` });
      return;
    }

    // Use the first matching feature for centroid
    const f = data.features[0];
    const center = centroidOfGeometry(f.geometry);
    if (!center) {
      res.status(500).json({ error: "Could not compute centroid" });
      return;
    }

    const gmaps = `https://www.google.com/maps/search/?api=1&query=${center.lat},${center.lng}`;

    res.status(200).json({
      ok: true,
      assetId,
      dataset,
      centroid: center,            // { lat, lng }
      googleMapsUrl: gmaps,
      properties: f.properties || {}
    });
  } catch (err) {
    const details = (err && (err.stack || err.message)) ? (err.stack || err.message) : String(err);
    console.error("[/api/locate] ERROR:", details);
    res.status(500).json({ error: "Locate failed", details });
  }
}
