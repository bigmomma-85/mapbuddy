// api/map-pdf.js
// Builds a printable Map PDF for a single asset using an OSM basemap image,
// draws the feature outline on top, adds title, scale bar, north arrow,
// OSM attribution, and a QR code to open in Google Maps.
//
// No external map keys needed. Uses the OSM Static Map service for the basemap
// and pdf-lib + qrcode to compose the PDF.

// IMPORTANT: make sure package.json includes:
//   "pdf-lib": "^1.17.1",
//   "qrcode": "^1.5.3"
//
// and that Vercel runs Node 22 (package.json -> "engines": { "node": "22.x" })

export const config = { runtime: "nodejs" };

import { PDFDocument, rgb, StandardFonts } from "pdf-lib";
import QRCode from "qrcode";

// --- Dataset catalog (match keys used in index.html) -------------------------
const DATASETS = {
  // Fairfax County DPWES Stormwater Facilities (Layer 7) | field: FACILITY_ID
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
  },

  // MDOT SHA — Managed Landscape (Layer 0)
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
  },

  // MDOT SHA — TMDL (examples; all use STRU_ID except 0/1 which use SWM_FAC_NO)
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idField: "SWM_FAC_NO",
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idField: "SWM_FAC_NO",
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idField: "STRU_ID",
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idField: "STRU_ID",
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idField: "STRU_ID",
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idField: "STRU_ID",
  },
};

// ---- helpers ----------------------------------------------------------------

async function readJson(req) {
  const chunks = [];
  for await (const c of req) chunks.push(c);
  try {
    return JSON.parse(Buffer.concat(chunks).toString("utf8") || "{}");
  } catch {
    return {};
  }
}

// Query ArcGIS REST and return a single GeoJSON Feature with geometry in WGS84.
async function fetchFeature(datasetKey, assetId) {
  const ds = DATASETS[datasetKey];
  if (!ds) throw new Error(`Unknown dataset '${datasetKey}'`);

  const base = ds.base.replace(/\/$/, "");
  const idField = ds.idField;

  // Wrap value in single quotes for string fields; many IDs are strings.
  const where = `${encodeURIComponent(idField)}%3D'${encodeURIComponent(assetId)}'`;
  const url =
    `${base}/query?where=${where}` +
    `&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;

  const r = await fetch(url, { headers: { "User-Agent": "MapBuddy" } });
  if (!r.ok) throw new Error(`ArcGIS query failed (${r.status})`);
  const json = await r.json();

  const f = json?.features?.[0];
  if (!f || !f.geometry) throw new Error("Feature not found");
  return f;
}

// Flatten any geometry into an array of rings (each ring is [ [lon,lat], ... ]).
function extractRings(geom) {
  const rings = [];
  if (!geom) return rings;

  const pushRing = (ring) => {
    if (ring && ring.length >= 3) rings.push(ring);
  };

  if (geom.type === "Polygon") {
    for (const ring of geom.coordinates) pushRing(ring);
  } else if (geom.type === "MultiPolygon") {
    for (const poly of geom.coordinates) {
      for (const ring of poly) pushRing(ring);
    }
  } else if (geom.type === "LineString") {
    pushRing(geom.coordinates);
  } else if (geom.type === "MultiLineString") {
    for (const line of geom.coordinates) pushRing(line);
  } else if (geom.type === "Point") {
    // treat as a tiny triangle around the point so we can draw something
    const [lon, lat] = geom.coordinates;
    pushRing([
      [lon, lat],
      [lon + 0.0001, lat + 0.00005],
      [lon - 0.0001, lat + 0.00005],
      [lon, lat],
    ]);
  }

  return rings;
}

function centroidOfRings(rings) {
  let sumX = 0, sumY = 0, n = 0;
  for (const ring of rings) {
    for (const [lon, lat] of ring) {
      sumX += lon;
      sumY += lat;
      n++;
    }
  }
  if (!n) return { lon: 0, lat: 0 };
  return { lon: sumX / n, lat: sumY / n };
}

function bboxOfRings(rings) {
  let minX = +Infinity, minY = +Infinity, maxX = -Infinity, maxY = -Infinity;
  for (const ring of rings) {
    for (const [lon, lat] of ring) {
      if (lon < minX) minX = lon;
      if (lat < minY) minY = lat;
      if (lon > maxX) maxX = lon;
      if (lat > maxY) maxY = lat;
    }
  }
  if (!Number.isFinite(minX)) {
    return { minX: -77.2, minY: 38.7, maxX: -77.1, maxY: 38.8 }; // fallback NOVA
  }
  return { minX, minY, maxX, maxY };
}

// Web Mercator helpers
const TILE_SIZE = 256;
function lonLatToWorldPx(lon, lat, zoom) {
  const siny = Math.sin((lat * Math.PI) / 180);
  const z = Math.pow(2, zoom);
  const x = ((lon + 180) / 360) * z * TILE_SIZE;
  const y = (0.5 - Math.log((1 + siny) / (1 - siny)) / (4 * Math.PI)) * z * TILE_SIZE;
  return { x, y };
}

function metersPerPixel(lat, zoom) {
  // Approximate scale for Web Mercator
  return (156543.03392 * Math.cos((lat * Math.PI) / 180)) / Math.pow(2, zoom);
}

function pickZoomToFit(rings, width, height, paddingPx = 40) {
  // Choose the highest zoom where the geometry fits fully inside width/height - padding
  for (let z = 18; z >= 5; z--) {
    let minX = +Infinity, minY = +Infinity, maxX = -Infinity, maxY = -Infinity;
    for (const ring of rings) {
      for (const [lon, lat] of ring) {
        const p = lonLatToWorldPx(lon, lat, z);
        if (p.x < minX) minX = p.x;
        if (p.y < minY) minY = p.y;
        if (p.x > maxX) maxX = p.x;
        if (p.y > maxY) maxY = p.y;
      }
    }
    const w = maxX - minX;
    const h = maxY - minY;
    if (w <= width - paddingPx && h <= height - paddingPx) {
      return z;
    }
  }
  return 11;
}

function drawNorthArrow(page, x, y, size) {
  // simple triangle north arrow
  page.drawLine({ start: { x, y }, end: { x, y + size }, color: rgb(0, 0, 0), thickness: 1.2 });
  page.drawLine({ start: { x: x - size * 0.4, y: y + size * 0.4 }, end: { x, y + size }, color: rgb(0, 0, 0), thickness: 1.2 });
  page.drawLine({ start: { x: x + size * 0.4, y: y + size * 0.4 }, end: { x, y + size }, color: rgb(0, 0, 0), thickness: 1.2 });
  page.drawText("N", { x: x - 4, y: y + size + 2, size: 10 });
}

function niceScaleLength(mpp, targetPx = 120) {
  const meters = mpp * targetPx;
  const steps = [1, 2, 5];
  const pow10 = Math.pow(10, Math.floor(Math.log10(meters)));
  for (const s of steps) {
    const v = s * pow10;
    if (v >= meters) return v;
  }
  return 10 * pow10;
}

// -----------------------------------------------------------------------------

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST with JSON body { assetId, dataset }" });
      return;
    }

    const body = await readJson(req);
    const assetId = String(body.assetId || "").trim();
    const dataset = String(body.dataset || "").trim();

    if (!assetId || !dataset) {
      res.status(400).json({ error: "Missing assetId or dataset" });
      return;
    }

    // 1) Get GeoJSON feature
    const feat = await fetchFeature(dataset, assetId);
    const rings = extractRings(feat.geometry);
    if (!rings.length) {
      res.status(404).json({ error: "No geometry to map" });
      return;
    }

    const { lon, lat } = centroidOfRings(rings);
    const bbox = bboxOfRings(rings);

    // 2) Choose output PDF canvas + map image size
    const MAP_W = 1000; // px
    const MAP_H = 700;  // px
    const HEADER = 110;
    const FOOTER = 90;
    const PAGE_W = MAP_W;
    const PAGE_H = MAP_H + HEADER + FOOTER;

    // 3) Compute a zoom that fits the feature
    const zoom = pickZoomToFit(rings, MAP_W, MAP_H, 60);

    // 4) Fetch OSM static basemap image
    // OSM static map expects lat,lon (note order), with zoom & size
    const osmUrl = `https://staticmap.openstreetmap.de/staticmap.php?center=${lat},${lon}&zoom=${zoom}&size=${MAP_W}x${MAP_H}&maptype=mapnik&markers=${lat},${lon},lightblue1`;
    const imgResp = await fetch(osmUrl, { headers: { "User-Agent": "MapBuddy" } });
    if (!imgResp.ok) {
      res.status(502).json({ error: `Basemap fetch failed (${imgResp.status})` });
      return;
    }
    const mapPng = Buffer.from(await imgResp.arrayBuffer());

    // 5) Compose PDF
    const pdf = await PDFDocument.create();
    const page = pdf.addPage([PAGE_W, PAGE_H]);

    const font = await pdf.embedFont(StandardFonts.Helvetica);
    const fontB = await pdf.embedFont(StandardFonts.HelveticaBold);

    // header title
    const title = `Map — ${assetId}`;
    page.drawText("MapBuddy", { x: 20, y: PAGE_H - 36, size: 16, font: fontB, color: rgb(0.08, 0.32, 0.64) });
    page.drawText(title, { x: 20, y: PAGE_H - 60, size: 18, font: fontB });
    page.drawText(`Dataset: ${dataset}`, { x: 20, y: PAGE_H - 82, size: 11, font });

    // embed map image
    const img = await pdf.embedPng(mapPng);
    page.drawImage(img, { x: 0, y: FOOTER, width: MAP_W, height: MAP_H });

    // 6) Draw geometry overlay (project lon/lat to pixel at center/zoom)
    const centerPx = lonLatToWorldPx(lon, lat, zoom);

    // outline style
    const stroke = rgb(0.0, 0.66, 0.20);
    const thickness = 2.2;

    for (const ring of rings) {
      let path = "";
      ring.forEach(([LON, LAT], idx) => {
        const p = lonLatToWorldPx(LON, LAT, zoom);
        const x = (p.x - centerPx.x) + MAP_W / 2;
        const y = (p.y - centerPx.y) + MAP_H / 2 + FOOTER;
        path += `${idx === 0 ? "M" : "L"} ${x.toFixed(2)} ${y.toFixed(2)} `;
      });
      // close path
      if (ring.length) {
        const [LON, LAT] = ring[0];
        const p0 = lonLatToWorldPx(LON, LAT, zoom);
        const x0 = (p0.x - centerPx.x) + MAP_W / 2;
        const y0 = (p0.y - centerPx.y) + MAP_H / 2 + FOOTER;
        path += `L ${x0.toFixed(2)} ${y0.toFixed(2)} Z`;
      }

      page.drawSvgPath(path, {
        borderColor: stroke,
        borderWidth: thickness,
        color: rgb(0, 0.8, 0.3, 0.12), // light fill
      });
    }

    // 7) Scale bar & north arrow in footer
    const mpp = metersPerPixel(lat, zoom);
    const niceMeters = niceScaleLength(mpp, 140);
    const pxLen = niceMeters / mpp;
    const sbX = 20, sbY = 50;

    // scale bar rectangle
    page.drawRectangle({
      x: sbX,
      y: sbY,
      width: pxLen,
      height: 6,
      color: rgb(0.2, 0.2, 0.2),
    });
    page.drawText(`${niceMeters >= 1000 ? (niceMeters / 1000).toFixed(1) + " km" : Math.round(niceMeters) + " m"}`, {
      x: sbX, y: sbY + 10, size: 10, font
    });

    // north arrow
    drawNorthArrow(page, PAGE_W - 40, sbY - 4, 22);

    // 8) Google Maps link + QR
    const gmapsUrl = `https://maps.google.com/?q=${lat.toFixed(6)},${lon.toFixed(6)}`;
    page.drawText("Open in Google Maps:", { x: 20, y: 18, size: 10, font });
    page.drawText(gmapsUrl, { x: 135, y: 18, size: 10, font, color: rgb(0.1, 0.3, 0.8) });

    const qrDataUrl = await QRCode.toDataURL(gmapsUrl, { margin: 0, scale: 3 });
    const qrPng = await pdf.embedPng(Buffer.from(qrDataUrl.split(",")[1], "base64"));
    page.drawImage(qrPng, { x: PAGE_W - 80, y: 8, width: 64, height: 64 });

    // OSM attribution
    page.drawText("© OpenStreetMap contributors", {
      x: PAGE_W / 2 - 80, y: 18, size: 9, font, color: rgb(0.35, 0.35, 0.35)
    });

    // 9) send
    const bytes = await pdf.save();
    res.setHeader("Content-Type", "application/pdf");
    res.setHeader("Content-Disposition", `attachment; filename="${assetId}_map.pdf"`);
    res.status(200).send(Buffer.from(bytes));
  } catch (err) {
    res.status(500).json({ error: String(err?.message || err) });
  }
}
