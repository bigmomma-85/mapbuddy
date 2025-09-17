// api/locate.js
// Return centroid + Google Maps link for an asset.

import axios from "axios";
export const config = { runtime: "nodejs" };

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); }
  catch { return {}; }
}

function buildQueryUrl(datasetKey, assetId) {
  const enc = encodeURIComponent;
  if (datasetKey === "fairfax_bmps") {
    const base =
      "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7";
    return `${base}/query?where=FACILITY_ID%3D'${enc(assetId)}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  }
  if (datasetKey === "mdsha_landscape") {
    const base =
      "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0";
    return `${base}/query?where=LOD_ID%3D'${enc(assetId)}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  }
  const tmdlBase =
    "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer";
  const layerDefs = {
    mdsha_tmdl_structures: { layer: 0, field: "SWM_FAC_NO" },
    mdsha_tmdl_retrofits: { layer: 1, field: "SWM_FAC_NO" },
    mdsha_tmdl_tree_plantings: { layer: 2, field: "STRU_ID" },
    mdsha_tmdl_pavement_removals: { layer: 3, field: "STRU_ID" },
    mdsha_tmdl_stream_restorations: { layer: 4, field: "STRU_ID" },
    mdsha_tmdl_outfall_stabilizations: { layer: 5, field: "STRU_ID" },
  };
  const def = layerDefs[datasetKey];
  if (!def) return null;
  return `${tmdlBase}/${def.layer}/query?where=${def.field}%3D'${enc(
    assetId
  )}'&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
}

function polygonCentroid(coords) {
  // coords: [ [ [lng,lat], ... ] , ... ]
  const ring = coords[0];
  let area = 0, cx = 0, cy = 0;
  for (let i = 0, j = ring.length - 1; i < ring.length; j = i++) {
    const [x1, y1] = ring[j];
    const [x2, y2] = ring[i];
    const f = x1 * y2 - x2 * y1;
    area += f;
    cx += (x1 + x2) * f;
    cy += (y1 + y2) * f;
  }
  area *= 0.5;
  if (Math.abs(area) < 1e-9) return null;
  return { lng: cx / (6 * area), lat: cy / (6 * area) };
}

function featureCentroid(f) {
  const g = f?.geometry;
  if (!g) return null;
  if (g.type === "Point") return { lat: g.coordinates[1], lng: g.coordinates[0] };
  if (g.type === "Polygon") return polygonCentroid(g.coordinates);
  if (g.type === "MultiPolygon") {
    // use the first polygon
    return polygonCentroid(g.coordinates[0]);
  }
  return null;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJson(req);
    const assetId = (body.assetId || "").trim();
    const dataset = (body.dataset || "").trim();
    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const url = buildQueryUrl(dataset, assetId);
    if (!url) {
      res.status(400).json({ error: `Unknown dataset '${dataset}'` });
      return;
    }

    const { data } = await axios.get(url, { timeout: 15000, responseType: "json" });

    if (!data || data.type !== "FeatureCollection" || !data.features?.length) {
      res.status(404).json({ error: "No feature found (VPN needed for MDOT SHA?)" });
      return;
    }

    const c = featureCentroid(data.features[0]);
    if (!c) {
      res.status(422).json({ error: "Could not compute centroid for this geometry." });
      return;
    }

    const googleMapsUrl = `https://www.google.com/maps?q=${c.lat},${c.lng}`;
    res.status(200).json({ centroid: c, googleMapsUrl });
  } catch (err) {
    res
      .status(500)
      .json({
        error: `Locate failed: ${err?.message || "Unknown error"}`,
        details: err?.stack?.slice(0, 300)
      });
  }
}
