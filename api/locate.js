// api/locate.js
// Returns centroid + Google Maps link for a single feature.

import axios from "axios";

// Node (not Edge)
export const config = { runtime: "nodejs" };

// ---------- dataset registry (in sync with convert/bulk) ----------
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idFields: ["FACILITY_ID"]
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idFields: ["LOD_ID"]
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
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idFields: ["STRU_ID", "NAME", "PROJECT_ID", "ASSET_ID"]
  }
};

function esc(s) { return String(s).replace(/'/g, "''"); }
function whereFrom(fields, id) { const v = esc(id); return fields.map(f => `${f}='${v}'`).join(" OR "); }

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); }
  catch { return {}; }
}

// ---- centroid helpers for GeoJSON geometry ----
function bboxCenter(coordsFlat) {
  let minx=Infinity, miny=Infinity, maxx=-Infinity, maxy=-Infinity;
  for (const [x,y] of coordsFlat){ if (x<minx) minx=x; if (y<miny) miny=y; if (x>maxx) maxx=x; if (y>maxy) maxy=y; }
  return { lng:(minx+maxx)/2, lat:(miny+maxy)/2 };
}
function flattenCoords(g){
  const out=[];
  const walk=(c)=>{ if (typeof c[0]==="number") out.push(c); else c.forEach(walk); };
  walk(g.coordinates); return out;
}
function centroidPolygon(rings){
  const ring = rings[0]; // outer ring
  let a=0, cx=0, cy=0;
  for (let i=0, j=ring.length-1; i<ring.length; j=i++){
    const [x0,y0]=ring[j], [x1,y1]=ring[i];
    const f = x0*y1 - x1*y0;
    a += f; cx += (x0+x1)*f; cy += (y0+y1)*f;
  }
  if (a === 0) return bboxCenter(ring);
  a *= 0.5; cx /= (6*a); cy /= (6*a);
  return { lng: cx, lat: cy };
}
function centroid(geom){
  if (!geom) return null;
  const t = geom.type;
  if (t === "Point")      return { lng: geom.coordinates[0], lat: geom.coordinates[1] };
  if (t === "MultiPoint" || t === "LineString"){
    const flat = flattenCoords(geom);
    let sx=0, sy=0; for (const [x,y] of flat){ sx+=x; sy+=y; }
    return { lng: sx/flat.length, lat: sy/flat.length };
  }
  if (t === "MultiLineString") {
    return bboxCenter(flattenCoords(geom));
  }
  if (t === "Polygon")     return centroidPolygon(geom.coordinates);
  if (t === "MultiPolygon"){
    // choose largest ring by area
    let best = null, bestAbsArea = -1;
    for (const poly of geom.coordinates){
      const ring = poly[0];
      let a=0;
      for (let i=0,j=ring.length-1;i<ring.length;j=i++){
        const [x0,y0]=ring[j],[x1,y1]=ring[i]; a += x0*y1 - x1*y0;
      }
      const abs = Math.abs(a);
      if (abs > bestAbsArea){ bestAbsArea = abs; best = centroidPolygon(poly); }
    }
    return best || bboxCenter(flattenCoords(geom));
  }
  return null;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST"){
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" }); return;
    }
    const { assetId, dataset } = await readJson(req);
    if (!assetId || !dataset) { res.status(400).json({ error: "Missing assetId or dataset" }); return; }

    const ds = DATASETS[dataset];
    if (!ds) { res.status(400).json({ error: `Unknown dataset '${dataset}'` }); return; }

    const url = `${ds.base}/query`;
    const params = {
      where: whereFrom(ds.idFields, assetId),
      outFields: "*",
      returnGeometry: true,
      outSR: 4326,
      f: "geojson"
    };
    const { data } = await axios.get(url, { params, timeout: 20000 });

    if (!data || !data.features || !data.features.length){
      res.status(404).json({ error: "No feature found" });
      return;
    }

    const feat = data.features[0];
    const c = centroid(feat.geometry);
    if (!c) { res.status(500).json({ error: "Unable to compute centroid" }); return; }

    const googleMapsUrl = `https://www.google.com/maps?q=${c.lat.toFixed(6)},${c.lng.toFixed(6)}`;
    res.status(200).json({
      assetId,
      dataset,
      centroid: c,
      googleMapsUrl,
      properties: feat.properties || {}
    });
  } catch (err) {
    res.status(500).json({ error: err?.message || String(err) });
  }
}
