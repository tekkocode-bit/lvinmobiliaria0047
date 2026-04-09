import express from "express";
import { createCatalogAdmin } from "./router.js";

function safeJson(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

const app = express();
const PORT = Number(process.env.PORT || 3100);

let localCatalog = safeJson(process.env.INITIAL_PROPERTY_CATALOG_JSON || "[]", []);
if (!Array.isArray(localCatalog)) localCatalog = [];

const admin = createCatalogAdmin({
  basePath: "",
  businessName: process.env.BUSINESS_NAME || "LV Inmobiliaria",
  adminUsername: process.env.ADMIN_PANEL_USERNAME || "admin",
  adminPassword: process.env.ADMIN_PANEL_PASSWORD || "admin123456",
  getCatalog: () => localCatalog,
  setCatalog: async (nextCatalog) => {
    localCatalog = Array.isArray(nextCatalog) ? nextCatalog : [];
  },
});

await admin.init();
app.use("/", admin.router);

app.listen(PORT, () => {
  console.log(`Catalog admin running on :${PORT}`);
  console.log(`Properties loaded: ${localCatalog.length}`);
});
