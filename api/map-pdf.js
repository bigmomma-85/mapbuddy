// api/map-pdf.js
// Create a street basemap PDF with the selected feature drawn on top.
//
// Requires:
//   "@napi-rs/canvas": "^0.1.53"
//   "pdf-lib": "^1.17.1"
// Runtime: node (NOT edge)

export const config = { runtime: "nodejs" };

import { createCanvas, loadImage } from "@napi-rs/canvas";
import { PDFDocument, rgb, StandardFonts } from "pdf-lib";

/* ----------------------- helpers ----------------------- */

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try { return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}"); }
  catch { return {}; }
}

function q(v) { return `'${String(v).replace(/'/g, "''")}'`; }

function project(lon, lat, z) {
  const tileSize = 256;
  const scale = tileSize * Math.pow(2, z);
  const x = (lon + 180) / 360 * scale;
  const sin = Math.sin(lat * Math.PI / 180);
  const y = (0.5 - Math.log((1 + sin) / (1 - sin)) / (4 * Math.PI)) * scale;
  return { x, y };
}

function bboxOfFeature(gj) {
  let minX = 180, minY = 90, maxX = -180, maxY = -90;
  const each = (coords) => {
    for (const [lon, lat] of coords) {
      if (lon < minX) minX = lon;
      if (lat < minY) minY = lat;
      if (lon > maxX) maxX = lon;
      if (lat > maxY) maxY = lat;
    }
  };
  const geom = gj.geometry || gj;
  if (!geom) return null;

  if (geom.type === "Point") {
    const [lon, lat] = geom.coordinates;
    const d = 0.0015;
    return { minX: lon - d, minY: lat - d, maxX: lon + d, maxY: lat + d };
  }
  if (geom.type === "Polygon")
    geom.coordinates.forEach(r => each(r));
  else if (geom.type === "MultiPolygon")
    geom.coordinates.forEach(p => p.forEach(r => each(r)));
  else if (geom.type === "LineString")
    each(geom.coordinates);
  else if (geom.type === "MultiLineString")
    geom.coordinates.forEach(each);
  else return null;

  return { minX, minY, maxX, maxY };
}

function drawGeometry(ctx, gj, z, originX, originY) {
  const geom = (gj.geometry || gj);
  const pathRing = (ring) => {
    ring.forEach(([lon, lat], i) => {
      const { x, y } = project(lon, lat, z);
      if (i === 0) ctx.moveTo(x - originX, y - originY);
      else ctx.lineTo(x - originX, y - originY);
    });
  };
  ctx.save();
  ctx.lineWidth = 3;
  ctx.strokeStyle = "rgba(0,153,255,1)";
  ctx.fillStyle   = "rgba(0,153,255,0.25)";

  ctx.beginPath();
  if (geom.type === "Polygon") geom.coordinates.forEach(pathRing);
  else if (geom.type === "MultiPolygon") geom.coordinates.forEach(poly => poly.forEach(pathRing));
  else if (geom.type === "LineString") pathRing(geom.coordinates);
  else if (geom.type === "MultiLineString") geom.coordinates.forEach(pathRing);
  else if (geom.type === "Point") {
    const [lon, lat] = geom.coordinates;
    const { x, y } = project(lon, lat, z);
    ctx.beginPath();
    ctx.arc(x - originX, y - originY, 8, 0, Math.PI * 2);
    ctx.fill();
    ctx.stroke();
    ctx.restore();
    return;
  }
  ctx.fill("evenodd");
  ctx.stroke();
  ctx.restore();
}

function centerOfBBox(b) {
  return { lon: (b.minX + b.maxX) / 2, lat: (b.minY + b.maxY) / 2 };
}

function metersPerPixelAtLat(lat, z) {
  // Earth circumference in meters / pixels at this zoom
  const metersPerTile = 40075016.686 / Math.pow(2, z);
  const metersPerPixel = metersPerTile / 256 * Math.cos(lat * Math.PI / 180);
  return metersPerPixel;
}

/* ----------------------- datasets ----------------------- */

const DATASETS = {
  // Fairfax County DPWES — layer 7 — FACILITY_ID
  fairfax_bmps: {
    label: "Fairfax — Stormwater Facilities (FACILITY_ID)",
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },

  // MDOT SHA — Managed Landscape (Layer 0) — LOD_ID
  mdsha_landscape: {
    label: "MDOT SHA — Managed Landscape (LOD_ID)",
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },

  // TMDL (Stormwater Control Structures, Retrofits, Tree Plantings, Pavement Removals, Stream Restorations, Outfall Stabilizations)
  mdsha_tmdl_structures:        { label: "TMDL — Stormwater Control Structures", base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0", idField: "SWM_FAC_NO" },
  mdsha_tmdl_retrofits:         { label: "TMDL — Retrofits",                   base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1", idField: "SWM_FAC_NO" },
  mdsha_tmdl_tree_plantings:    { label: "TMDL — Tree Plantings",               base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2", idField: "STRU_ID" },
  mdsha_tmdl_pavement_removals: { label: "TMDL — Pavement Removals",            base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3", idField: "STRU_ID" },
  mdsha_tmdl_stream_restorations:{label: "TMDL — Stream Restorations",          base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4", idField: "STRU_ID" },
  mdsha_tmdl_outfall_stabilizations:{label:"TMDL — Outfall Stabilizations",     base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5", idField: "STRU_ID" },
};

async function fetchFeatureGeoJSON(datasetKey, assetId) {
  const def = DATASETS[datasetKey];
  if (!def) throw new Error(`Unknown dataset '${datasetKey}'`);

  const where = `${def.idField}=${q(assetId)}`;
  const url = `${def.base}/query?where=${encodeURIComponent(where)}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;

  const r = await fetch(url, { headers: { "User-Agent": "MapBuddy/1.0" } });
  if (!r.ok) {
    const t = await r.text().catch(()=> "");
    throw new Error(`ArcGIS query failed (${r.status}) ${t.slice(0,160)}`);
  }
  const gj = await r.json();
  const feat = gj?.features?.[0];
  if (!feat) throw new Error(`No feature found for ${assetId} in ${datasetKey}`);
  return feat;
}

/* ----------------------- tile rendering ----------------------- */

async function renderBasemapAndOverlay(feature, options = {}) {
  // target map size inside the PDF (pixels)
  const TARGET_W = options.width  || 1400;
  const TARGET_H = options.height || 900;
  const TILE = 512; // using @2x tiles (512px)
  const BASE = (z, x, y) => `https://a.basemaps.cartocdn.com/light_all/${z}/${x}/${y}@2x.png`;

  const bbox = bboxOfFeature(feature);
  if (!bbox) throw new Error("Unable to compute bounding box.");

  // choose zoom that fits bbox near target size
  let z = 16;
  const fit = () => {
    const p1 = project(bbox.minX, bbox.maxY, z);
    const p2 = project(bbox.maxX, bbox.minY, z);
    const w = Math.abs(p2.x - p1.x);
    const h = Math.abs(p2.y - p1.y);
    return { w, h };
  };

  // binary-ish search for a pleasing zoom (street level but not too tight)
  for (;;) {
    const { w, h } = fit();
    if (w > TARGET_W * 1.6 || h > TARGET_H * 1.6) z--;      // too zoomed-in
    else if (w < TARGET_W * 0.7 && h < TARGET_H * 0.7) z++; // too zoomed-out
    else break;
    if (z < 10) break;
    if (z > 19) break;
  }

  // compute tile range
  const pMin = project(bbox.minX, bbox.maxY, z);
  const pMax = project(bbox.maxX, bbox.minY, z);
  const minTileX = Math.floor(pMin.x / TILE);
  const minTileY = Math.floor(pMin.y / TILE);
  const maxTileX = Math.floor(pMax.x / TILE);
  const maxTileY = Math.floor(pMax.y / TILE);

  const cols = maxTileX - minTileX + 1;
  const rows = maxTileY - minTileY + 1;

  // build a canvas for the mosaic
  const mapCanvas = createCanvas(cols * TILE, rows * TILE);
  const ctx = mapCanvas.getContext("2d");

  // fetch & draw tiles
  for (let ty = minTileY; ty <= maxTileY; ty++) {
    for (let tx = minTileX; tx <= maxTileX; tx++) {
      const url = BASE(z, tx, ty);
      try {
        const img = await loadImage(await (await fetch(url)).arrayBuffer());
        ctx.drawImage(img, (tx - minTileX) * TILE, (ty - minTileY) * TILE);
      } catch {
        // draw a blank if a tile fails
        ctx.fillStyle = "#eef2f7";
        ctx.fillRect((tx - minTileX) * TILE, (ty - minTileY) * TILE, TILE, TILE);
      }
    }
  }

  // overlay geometry
  drawGeometry(
    ctx,
    feature,
    z,
    minTileX * TILE,
    minTileY * TILE
  );

  // scale map image to target size preserving aspect
  const a = Math.min(TARGET_W / mapCanvas.width, TARGET_H / mapCanvas.height);
  const drawW = Math.round(mapCanvas.width * a);
  const drawH = Math.round(mapCanvas.height * a);

  const out = createCanvas(drawW, drawH);
  const octx = out.getContext("2d");
  octx.drawImage(mapCanvas, 0, 0, drawW, drawH);

  // simple scale bar
  const { lon, lat } = centerOfBBox(bbox);
  const mpp = metersPerPixelAtLat(lat, z) * (1 / a); // scaled
  const barMeters = [50, 100, 200, 500, 1000, 2000, 5000].find(m => m / mpp < drawW / 4) || 1000;
  const barPx = Math.round(barMeters / mpp);

  octx.fillStyle = "rgba(255,255,255,0.9)";
  octx.fillRect(20, drawH - 50, barPx + 10, 30);
  octx.fillStyle = "#111";
  octx.fillRect(25, drawH - 35, barPx, 8);
  octx.font = "16px sans-serif";
  octx.fillText(`${barMeters >= 1000 ? barMeters/1000 + " km" : barMeters + " m"}`, 25, drawH - 40);

  return { canvas: out, center: { lon, lat } };
}

/* ----------------------- handler ----------------------- */

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST { assetId, dataset }" });
      return;
    }
    const { assetId, dataset } = await readJson(req);
    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    const feature = await fetchFeatureGeoJSON(dataset, assetId);
    const { canvas, center } = await renderBasemapAndOverlay(feature);

    // Compose PDF
    const pdf = await PDFDocument.create();
    const pageMargin = 40;
    const mapW = canvas.width, mapH = canvas.height;
    const page = pdf.addPage([mapW + pageMargin * 2, mapH + pageMargin * 2]);

    // embed PNG
    const png = await pdf.embedPng(canvas.toBuffer("image/png"));
    page.drawImage(png, { x: pageMargin, y: pageMargin, width: mapW, height: mapH });

    // header/footer
    const font = await pdf.embedFont(StandardFonts.HelveticaBold);
    const font2 = await pdf.embedFont(StandardFonts.Helvetica);

    const dsLabel = DATASETS[dataset]?.label || dataset;
    page.drawText(`MapBuddy • ${assetId}`, { x: pageMargin, y: page.getHeight() - 24, size: 16, font, color: rgb(0.1,0.1,0.1) });
    page.drawText(dsLabel, { x: pageMargin + 180, y: page.getHeight() - 24, size: 12, font: font2, color: rgb(0.2,0.2,0.2) });

    // link to Google Maps
    const gmaps = `https://www.google.com/maps/search/?api=1&query=${center.lat},${center.lon}`;
    page.drawText("Open in Google Maps", {
      x: page.getWidth() - pageMargin - 180,
      y: page.getHeight() - 24,
      size: 12,
      font: font2,
      color: rgb(0.0, 0.45, 1.0),
      link: gmaps
    });

    // attribution
    page.drawText("Basemap © Carto • Data © respective agencies", {
      x: pageMargin, y: 18, size: 9, font: font2, color: rgb(0.35,0.35,0.4)
    });

    const pdfBytes = await pdf.save();
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}_map.pdf"`);
    res.status(200).send(Buffer.from(pdfBytes));
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) });
  }
}
