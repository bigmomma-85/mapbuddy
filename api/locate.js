// api/locate.js
// Returns centroid + Google Maps link for a single feature.

import axios from "axios";
export const config = { runtime: "nodejs" };

// ---------- registry (mirror convert) ----------
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
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/0",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/1",
    idFields: ["SWM_FAC_NO", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/2",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/3",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/4",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/FeatureServer/5",
    idFields: ["STRU_ID", "ASSET_ID", "NAME", "PROJECT_ID"]
  }
};

function tmdlVariants(assetId){
  const s = String(assetId || "").trim().toUpperCase();
  const m = s.match(/^(\d+)\s*-?\s*([A-Z]{2})$/);
  if (m) { const num=m[1], suf=m[2]; return [ `${num}${suf}`, `${num}-${suf}`, `${num} ${suf}` ]; }
  return [s, s.replace(/-/g,""), s.replace(/\s+/g,""), s.replace(/(\d+)([A-Za-z]+)/,'$1-$2'), s.replace(/(\d+)([A-Za-z]+)/,'$1 $2')];
}
function whereFrom(fields, id, isTmdl){
  const vals = isTmdl ? tmdlVariants(id) : [String(id)];
  const esc = (v) => String(v).replace(/'/g, "''");
  const ors = [];
  for (const f of fields) for (const v of vals) ors.push(`UPPER(${f}) = UPPER('${esc(v)}')`);
  return ors.join(" OR ");
}

// centroid utils
function bboxCenter(coordsFlat){ let minx=Infinity,miny=Infinity,maxx=-Infinity,maxy=-Infinity;
  for(const [x,y] of coordsFlat){ if(x<minx)minx=x; if(y<miny)miny=y; if(x>maxx)maxx=x; if(y>maxy)maxy=y; }
  return { lng:(minx+maxx)/2, lat:(miny+maxy)/2 }; }
function flattenCoords(g){ const out=[]; const walk=(c)=>{ if(typeof c[0]==="number") out.push(c); else c.forEach(walk); }; walk(g.coordinates); return out; }
function centroidPolygon(rings){ const ring=rings[0]; let a=0,cx=0,cy=0;
  for(let i=0,j=ring.length-1;i<ring.length;j=i++){ const [x0,y0]=ring[j],[x1,y1]=ring[i]; const f=x0*y1-x1*y0; a+=f; cx+=(x0+x1)*f; cy+=(y0+y1)*f; }
  if(a===0) return bboxCenter(ring); a*=0.5; cx/=(6*a); cy/=(6*a); return { lng:cx, lat:cy }; }
function centroid(geom){
  if(!geom) return null; const t=geom.type;
  if(t==="Point") return { lng: geom.coordinates[0], lat: geom.coordinates[1] };
  if(t==="MultiPoint"||t==="LineString"){ const flat=flattenCoords(geom); let sx=0,sy=0; for(const [x,y] of flat){ sx+=x; sy+=y; } return { lng:sx/flat.length, lat:sy/flat.length }; }
  if(t==="MultiLineString") return bboxCenter(flattenCoords(geom));
  if(t==="Polygon") return centroidPolygon(geom.coordinates);
  if(t==="MultiPolygon"){ let best=null, bestAbs=-1;
    for(const poly of geom.coordinates){ const ring=poly[0]; let a=0; for(let i=0,j=ring.length-1;i<ring.length;j=i++){ const [x0,y0]=ring[j],[x1,y1]=ring[i]; a += x0*y1 - x1*y0; }
      const abs=Math.abs(a); if(abs>bestAbs){ bestAbs=abs; best=centroidPolygon(poly); } }
    return best || bboxCenter(flattenCoords(geom));
  }
  return null;
}

async function readJson(req){
  const chunks=[]; for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); } catch { return {}; }
}

export default async function handler(req, res){
  try{
    if(req.method!=="POST"){ res.status(405).json({ error:"Use POST with JSON body { assetId, dataset }" }); return; }
    const { assetId, dataset } = await readJson(req);
    if(!assetId || !dataset){ res.status(400).json({ error:"Missing assetId or dataset" }); return; }

    const ds = DATASETS[dataset];
    if(!ds){ res.status(400).json({ error:`Unknown dataset '${dataset}'` }); return; }

    const url = `${ds.base}/query`;
    const params = {
      where: whereFrom(ds.idFields, assetId, dataset.startsWith("mdsha_tmdl_")),
      outFields: "*",
      returnGeometry: true,
      outSR: 4326,
      f: "json" // FeatureServer expects JSON
    };

    const { data } = await axios.get(url, { params, timeout: 20000 });
    const feats = (data && Array.isArray(data.features)) ? data.features : [];
    if(!feats.length){ res.status(404).json({ error: "No feature found" }); return; }

    // make centroid from ESRI geometry
    const g = feats[0].geometry || {};
    let geo = null;
    if (Array.isArray(g.rings))      geo = { type:"Polygon",        coordinates:g.rings };
    else if (Array.isArray(g.paths)) geo = g.paths.length===1 ? { type:"LineString", coordinates:g.paths[0] } : { type:"MultiLineString", coordinates:g.paths };
    else if (typeof g.x==="number" && typeof g.y==="number") geo = { type:"Point", coordinates:[g.x,g.y] };

    const c = geo ? centroid(geo) : null;
    if(!c){ res.status(500).json({ error:"Unable to compute centroid" }); return; }

    const googleMapsUrl = `https://www.google.com/maps?q=${c.lat.toFixed(6)},${c.lng.toFixed(6)}`;
    res.status(200).json({ assetId, dataset, centroid:c, googleMapsUrl, properties: feats[0].attributes || {} });
  }catch(err){
    res.status(500).json({ error: err?.message || String(err) });
  }
}
