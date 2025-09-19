// api/bulk.js
import axios from "axios";
import AdmZip from "adm-zip";
export const config = { runtime: "nodejs" };

/* ---------- Datasets ---------- */
const DATASETS = {
  fairfax_bmps: {
    base: "https://www.fairfaxcounty.gov/mercator/rest/services/DPWES/StwFieldMap/MapServer/7",
    idField: "FACILITY_ID",
    label: "Fairfax — Stormwater Facilities (FACILITY_ID)"
  },
  mdsha_landscape: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/OED_Env_Assets_Mgr/OED_Environmental_Assets_WGS84_Maryland_MDOTSHA/MapServer/0",
    idField: "LOD_ID",
    label: "MDOT SHA — Managed Landscape (LOD_ID)"
  },
  mdsha_tmdl_structures: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/0",
    idField: "SWM_FAC_NO",
    label: "TMDL — Stormwater Control Structures"
  },
  mdsha_tmdl_retrofits: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/1",
    idField: "SWM_FAC_NO",
    label: "TMDL — Retrofits"
  },
  mdsha_tmdl_tree_plantings: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/2",
    idField: "STRU_ID",
    label: "TMDL — Tree Plantings"
  },
  mdsha_tmdl_pavement_removals: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/3",
    idField: "STRU_ID",
    label: "TMDL — Pavement Removals"
  },
  mdsha_tmdl_stream_restorations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/4",
    idField: "STRU_ID",
    label: "TMDL — Stream Restorations"
  },
  mdsha_tmdl_outfall_stabilizations: {
    base: "https://maps.roads.maryland.gov/arcgis/rest/services/BayRestoration/TMDLBayRestorationViewer_Maryland_MDOTSHA/MapServer/5",
    idField: "STRU_ID",
    label: "TMDL — Outfall Stabilizations"
  }
};

/* Accept labels/aliases or canonical keys */
const ALIASES = (() => {
  const m = new Map();
  for (const [k, v] of Object.entries(DATASETS)) {
    m.set(k.toLowerCase(), k);
    m.set(v.label.toLowerCase(), k);
    m.set(v.label.replaceAll("—", "-").toLowerCase(), k); // tolerate hyphen variant
  }
  // short forms you might paste
  m.set("fairfax", "fairfax_bmps");
  m.set("mdot sha landscape", "mdsha_landscape");
  m.set("tmdl structures", "mdsha_tmdl_structures");
  m.set("tmdl retrofits", "mdsha_tmdl_retrofits");
  m.set("tmdl trees", "mdsha_tmdl_tree_plantings");
  return m;
})();

function normalizeDs(d) {
  if (!d) return "";
  const key = ALIASES.get(String(d).trim().toLowerCase());
  return key || String(d).trim();
}

function ds(key) {
  const d = DATASETS[key];
  if (!d) throw new Error(`Unknown dataset '${key}'`);
  return d;
}
function where(field, val) {
  return `${field}='${String(val).replace(/'/g, "''")}'`;
}

async function fetchFeature(datasetKey, assetId) {
  const { base, idField } = ds(datasetKey);
  const url = `${base}/query?where=${encodeURIComponent(where(idField, assetId))}&outFields=*&returnGeometry=true&outSR=4326&f=geojson`;
  const r = await axios.get(url, { timeout: 30000 });
  const feats = Array.isArray(r.data?.features) ? r.data.features : [];
  if (!feats.length) return null;
  return { type: "FeatureCollection", features: [feats[0]] };
}

async function getApply() {
  const ms = await import("mapshaper");
  return ms.applyCommands || ms.default?.applyCommands || null;
}
function addErrorsCsv(zip, misses) {
  if (!misses.length) return;
  const header = "assetId,dataset,reason\n";
  const rows = misses
    .map(
      (m) =>
        `"${(m.assetId || "").replace(/"/g, '""')}","${(m.datasetKey || "").replace(
          /"/g,
          '""'
        )}","${(m.reason || "not found").replace(/"/g, '""')}"`
    )
    .join("\n") + "\n";
  zip.addFile("errors.csv", Buffer.from(header + rows, "utf8"));
}
function outName(dsKey, id, ext) {
  return `${dsKey}__${id}.${ext}`;
}

export default async function handler(req, res) {
  try {
    if (req.method !== "POST") {
      res.status(405).json({ error: "Use POST" });
      return;
    }
    const body = typeof req.body === "string" ? JSON.parse(req.body || "{}") : req.body || {};
    let { items = [], defaultDataset = "", format = "kmlzip" } = body;

    if (!Array.isArray(items) || !items.length) {
      res.status(400).json({ error: "Provide items: [{assetId, dataset?}, ...]" });
      return;
    }

    defaultDataset = normalizeDs(defaultDataset);
    const apply = await getApply();
    if (!apply) throw new Error("Mapshaper API not available");

    const hits = [];
    const misses = [];
    for (const row of items) {
      const assetId = String(row?.assetId || "").trim();
      const datasetKey = normalizeDs(row?.dataset || defaultDataset);
      if (!assetId || !datasetKey) {
        if (assetId) misses.push({ assetId, datasetKey, reason: "missing dataset" });
        continue;
      }
      const fc = await fetchFeature(datasetKey, assetId).catch(() => null);
      if (fc) hits.push({ assetId, datasetKey, fc });
      else misses.push({ assetId, datasetKey, reason: "not found" });
    }

    if (!hits.length) {
      res.status(404).json({
        error:
          "No features found for the provided inputs. Check that the dataset matches your IDs.",
        sampleDatasetKeys: Object.keys(DATASETS)
      });
      return;
    }

    const wantZip = /zip$/i.test(format);
    const asKml = /^kml/i.test(format);
    const asGeo = /^geojson/i.test(format);
    const asShp = /^shapefile/i.test(format);

    if (asKml || asGeo) {
      if (wantZip) {
        const zip = new AdmZip();
        for (const { assetId, datasetKey, fc } of hits) {
          if (asKml) {
            const out = await apply(`-i in.json -o format=kml precision=0.000001 out.kml`, {
              "in.json": JSON.stringify(fc)
            });
            const kml = out["out.kml"] || "";
            if (kml) zip.addFile(outName(datasetKey, assetId, "kml"), Buffer.from(kml, "utf8"));
            else misses.push({ assetId, datasetKey, reason: "empty KML" });
          } else {
            zip.addFile(
              outName(datasetKey, assetId, "geojson"),
              Buffer.from(JSON.stringify(fc), "utf8")
            );
          }
        }
        addErrorsCsv(zip, misses);
        const buf = zip.toBuffer();
        res.setHeader("Content-Type", "application/zip");
        res.setHeader(
          "Content-Disposition",
          `attachment; filename=mapbuddy_${asKml ? "kml" : "geojson"}.zip`
        );
        res.end(buf);
        return;
      }
      // merged single file (non-zip)
      const merged = { type: "FeatureCollection", features: hits.flatMap((h) => h.fc.features) };
      if (asKml) {
        const out = await apply(`-i in.json -o format=kml precision=0.000001 out.kml`, {
          "in.json": JSON.stringify(merged)
        });
        const kml = out["out.kml"] || "";
        res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
        res.setHeader("Content-Disposition", "attachment; filename=mapbuddy.kml");
        res.end(kml);
      } else {
        res.setHeader("Content-Type", "application/geo+json");
        res.setHeader("Content-Disposition", "attachment; filename=mapbuddy.geojson");
        res.end(JSON.stringify(merged));
      }
      return;
    }

    if (asShp) {
      const master = new AdmZip();
      for (const { assetId, datasetKey, fc } of hits) {
        const out = await apply(`-i in.json -o format=shapefile encoding=UTF-8 out.zip`, {
          "in.json": JSON.stringify(fc)
        });
        const inner = out["out.zip"];
        if (inner?.length)
          master.addFile(outName(datasetKey, assetId, "shp.zip"), Buffer.from(inner));
        else misses.push({ assetId, datasetKey, reason: "empty shapefile" });
      }
      addErrorsCsv(master, misses);
      const buf = master.toBuffer();
      res.setHeader("Content-Type", "application/zip");
      res.setHeader("Content-Disposition", "attachment; filename=mapbuddy_shapefiles.zip");
      res.end(buf);
      return;
    }

    res.status(400).json({ error: `Unknown format '${format}'` });
  } catch (e) {
    res.status(500).json({ error: e?.message || String(e) });
  }
}
