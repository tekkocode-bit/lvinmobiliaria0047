import express from "express";
import axios from "axios";
import crypto from "crypto";
import fs from "fs/promises";
import path from "path";
import { fileURLToPath } from "url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

function safeJson(value, fallback) {
  try {
    return JSON.parse(value);
  } catch {
    return fallback;
  }
}

function normalizeText(value) {
  return String(value || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function parseList(value) {
  if (Array.isArray(value)) return value.map((v) => cleanText(v)).filter(Boolean);
  return String(value || "")
    .split(/[\n,|•]+/g)
    .map((v) => cleanText(v))
    .filter(Boolean);
}

function parseMaybeNumber(value) {
  if (value === null || value === undefined || value === "") return "";
  const t = String(value).trim();
  if (!t) return "";
  const n = Number(t);
  return Number.isFinite(n) ? n : t;
}

function parseMaybeBoolean(value) {
  if (value === true || value === false) return value;
  const t = normalizeText(value);
  if (!t) return "";
  if (["si", "sí", "true", "1", "yes"].includes(t)) return true;
  if (["no", "false", "0"].includes(t)) return false;
  return String(value).trim();
}

function parsePriceNumber(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  let text = normalizeText(raw);
  const hasMillion = text.includes("millon") || text.includes("millones") || /\bmm\b/.test(text);
  const hasThousand = /\bk\b/.test(text) || text.includes("mil");
  text = text.replace(/rd\$/g, "").replace(/us\$/g, "").replace(/usd/g, "").replace(/dop/g, "").replace(/,/g, "");
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  let n = Number(match[0]);
  if (!Number.isFinite(n)) return "";
  if (hasMillion) n *= 1000000;
  else if (hasThousand && n < 100000) n *= 1000;
  return String(Math.round(n));
}

function buildMetaName(property) {
  const op = normalizeText(property.operation || "").includes("alquiler") ? "ALQUILER" : "VENTA";
  return `${op} | ${cleanText(property.title || property.code || property.id)}`.slice(0, 150);
}

function formatMetaDescription(property) {
  const pieces = [];
  const op = normalizeText(property.operation || "").includes("alquiler") ? "alquiler" : "venta";
  pieces.push(`Propiedad en ${op}.`);
  if (property.short_description) pieces.push(cleanText(property.short_description));
  if (property.location) pieces.push(`Ubicación: ${cleanText(property.location)}.`);
  const details = [];
  if (property.bedrooms !== "" && property.bedrooms !== null && property.bedrooms !== undefined) details.push(`${property.bedrooms} habitaciones`);
  if (property.bathrooms !== "" && property.bathrooms !== null && property.bathrooms !== undefined) details.push(`${property.bathrooms} baños`);
  if (property.area_m2 !== "" && property.area_m2 !== null && property.area_m2 !== undefined) details.push(`${property.area_m2} m²`);
  if (property.parking !== "" && property.parking !== null && property.parking !== undefined) details.push(`${property.parking} parqueos`);
  if (details.length) pieces.push(`Detalles: ${details.join(", ")}.`);
  return pieces.join(" ").replace(/\s+/g, " ").trim().slice(0, 5000);
}

function normalizeProperty(raw, index = 0) {
  const id = cleanText(raw?.id || `prop_${index + 1}`);
  const retailerId = cleanText(raw?.retailer_id || raw?.product_retailer_id || raw?.code || id);
  const code = cleanText(raw?.code || retailerId || id);
  const nowIso = new Date().toISOString();
  return {
    id,
    retailer_id: retailerId,
    product_retailer_id: retailerId,
    code,
    title: cleanText(raw?.title || raw?.name || code),
    category: cleanText(raw?.category || raw?.type || "apartamentos"),
    operation: cleanText(raw?.operation || "venta"),
    price: raw?.price ?? "",
    currency: cleanText(raw?.currency || "DOP"),
    location: cleanText(raw?.location || raw?.zone || ""),
    exact_address: cleanText(raw?.exact_address || raw?.direccion || raw?.direccion_exacta || ""),
    exact_location_reference: cleanText(raw?.exact_location_reference || raw?.referencia_ubicacion || raw?.ubicacion_referencia || ""),
    bedrooms: parseMaybeNumber(raw?.bedrooms ?? raw?.rooms ?? ""),
    bathrooms: parseMaybeNumber(raw?.bathrooms ?? ""),
    parking: parseMaybeNumber(raw?.parking ?? raw?.parkings ?? ""),
    floor_level: cleanText(raw?.floor_level || raw?.nivel || raw?.piso || ""),
    area_m2: parseMaybeNumber(raw?.area_m2 ?? raw?.area ?? ""),
    lot_m2: parseMaybeNumber(raw?.lot_m2 ?? raw?.solar_m2 ?? raw?.terreno_m2 ?? raw?.metros_solar ?? ""),
    construction_m2: parseMaybeNumber(raw?.construction_m2 ?? raw?.construccion_m2 ?? raw?.metros_construccion ?? ""),
    short_description: cleanText(raw?.short_description || raw?.description || ""),
    features: parseList(raw?.features),
    year_built: cleanText(raw?.year_built || raw?.ano_construccion || raw?.año_construccion || ""),
    condition: cleanText(raw?.condition || raw?.estado_propiedad || raw?.terminacion || raw?.condicion || ""),
    title_deed: parseMaybeBoolean(raw?.title_deed ?? raw?.titulo_deslindado ?? raw?.title_deed_available ?? ""),
    has_mortgage: parseMaybeBoolean(raw?.has_mortgage ?? raw?.hipoteca ?? raw?.carga_legal ?? ""),
    legal_status: cleanText(raw?.legal_status || raw?.estado_legal || raw?.legalidad || ""),
    documents_up_to_date: parseMaybeBoolean(raw?.documents_up_to_date ?? raw?.documentos_al_dia ?? ""),
    bank_financing: parseMaybeBoolean(raw?.bank_financing ?? raw?.acepta_financiamiento ?? raw?.financiamiento_bancario ?? ""),
    bank_financing_note: cleanText(raw?.bank_financing_note || raw?.financiamiento_nota || raw?.financing_notes || ""),
    down_payment: cleanText(raw?.down_payment || raw?.inicial_requerido || raw?.separacion || raw?.separacion_requerida || ""),
    payment_facilities: cleanText(raw?.payment_facilities || raw?.facilidades_pago || raw?.owner_payment_facilities || ""),
    estimated_monthly_fee: cleanText(raw?.estimated_monthly_fee || raw?.cuota_aproximada || raw?.monthly_fee_estimate || ""),
    transfer_cost: cleanText(raw?.transfer_cost || raw?.costo_traspaso || ""),
    sewer: parseMaybeBoolean(raw?.sewer ?? raw?.cloaca ?? ""),
    paved_street: parseMaybeBoolean(raw?.paved_street ?? raw?.calle_asfaltada ?? ""),
    water_service: parseMaybeBoolean(raw?.water_service ?? raw?.servicio_agua ?? ""),
    electric_service: parseMaybeBoolean(raw?.electric_service ?? raw?.servicio_energia ?? raw?.servicio_electrica ?? ""),
    nearby_places: parseList(raw?.nearby_places || raw?.lugares_cercanos),
    safety: cleanText(raw?.safety || raw?.seguridad_zona || raw?.zona_segura || ""),
    transport_access: cleanText(raw?.transport_access || raw?.acceso_transporte || ""),
    purchase_steps: cleanText(raw?.purchase_steps || raw?.pasos_compra || raw?.proceso_compra || ""),
    purchase_timeline: cleanText(raw?.purchase_timeline || raw?.tiempo_proceso || raw?.proceso_tiempo || ""),
    faq: raw?.faq && typeof raw.faq === "object" ? raw.faq : {},
    status: cleanText(raw?.status || "available") || "available",
    duration_min: parseMaybeNumber(raw?.duration_min || ""),
    active: raw?.active !== false,
    agent_name: cleanText(raw?.agent_name || ""),
    agent_phone: cleanText(raw?.agent_phone || ""),
    meta_url: cleanText(raw?.meta_url || raw?.url || ""),
    meta_image_url: cleanText(raw?.meta_image_url || raw?.image_url || ""),
    meta_availability: cleanText(raw?.meta_availability || "in stock") || "in stock",
    updated_at: cleanText(raw?.updated_at || nowIso),
    created_at: cleanText(raw?.created_at || nowIso),
  };
}

function sortProperties(items = []) {
  return [...items].sort((a, b) => {
    const aTime = new Date(a.updated_at || a.created_at || 0).getTime();
    const bTime = new Date(b.updated_at || b.created_at || 0).getTime();
    if (bTime !== aTime) return bTime - aTime;
    return cleanText(a.title).localeCompare(cleanText(b.title), "es");
  });
}

function dedupeProperties(items = []) {
  const seen = new Set();
  const output = [];
  for (const item of items) {
    const normalized = normalizeProperty(item, output.length);
    const key = normalizeText(normalized.id || normalized.retailer_id || normalized.code);
    if (!key || seen.has(key)) continue;
    seen.add(key);
    output.push(normalized);
  }
  return sortProperties(output);
}

function getPropertyMergeKey(item = {}) {
  return normalizeText(item?.retailer_id || item?.product_retailer_id || item?.code || item?.id || "");
}

function isPlaceholderProperty(item = {}) {
  const key = getPropertyMergeKey(item);
  const title = normalizeText(item?.title || "");
  return key === "test-1" || key === "test_1" || title === "prueba";
}

function mergePropertyCollections(baseItems = [], overlayItems = []) {
  const map = new Map();

  const upsert = (incoming, preferIncoming = true) => {
    const normalized = normalizeProperty(incoming, map.size);
    const key = getPropertyMergeKey(normalized);
    if (!key) return;

    const current = map.get(key);
    if (!current) {
      map.set(key, normalized);
      return;
    }

    const merged = preferIncoming
      ? normalizeProperty(
          {
            ...current,
            ...normalized,
            id: cleanText(current.id || normalized.id || normalized.retailer_id || normalized.code),
            retailer_id: cleanText(normalized.retailer_id || current.retailer_id || current.code || current.id),
            code: cleanText(normalized.code || current.code || current.retailer_id || current.id),
            created_at: cleanText(current.created_at || normalized.created_at || new Date().toISOString()),
            updated_at: cleanText(normalized.updated_at || new Date().toISOString()),
          },
          map.size
        )
      : normalizeProperty(
          {
            ...normalized,
            ...current,
            id: cleanText(current.id || normalized.id || normalized.retailer_id || normalized.code),
            retailer_id: cleanText(current.retailer_id || normalized.retailer_id || normalized.code || normalized.id),
            code: cleanText(current.code || normalized.code || normalized.retailer_id || normalized.id),
            created_at: cleanText(current.created_at || normalized.created_at || new Date().toISOString()),
            updated_at: cleanText(current.updated_at || normalized.updated_at || new Date().toISOString()),
          },
          map.size
        );

    map.set(key, merged);
  };

  dedupeProperties(baseItems).forEach((item) => upsert(item, true));
  dedupeProperties(overlayItems).forEach((item) => upsert(item, true));
  return sortProperties([...map.values()]);
}

function parseWordNumber(token) {
  const t = normalizeText(token);
  const table = {
    cero: 0,
    un: 1,
    uno: 1,
    una: 1,
    dos: 2,
    tres: 3,
    cuatro: 4,
    cinco: 5,
    seis: 6,
    siete: 7,
    ocho: 8,
    nueve: 9,
    diez: 10,
    medio: 0.5,
    media: 0.5,
  };
  return Object.prototype.hasOwnProperty.call(table, t) ? table[t] : null;
}

function parseFlexibleNumber(token) {
  const raw = cleanText(token);
  if (!raw) return "";
  const normalized = raw.replace(/,/g, ".");
  const direct = Number(normalized);
  if (Number.isFinite(direct)) return direct;
  const word = parseWordNumber(raw);
  if (word !== null) return word;
  const match = raw.match(/(\d+(?:[.,]\d+)?)/);
  if (match) {
    const n = Number(match[1].replace(/,/g, "."));
    if (Number.isFinite(n)) return n;
  }
  return "";
}

function formatCatalogPriceFromMeta(priceValue, currency = "") {
  const value = cleanText(priceValue);
  const curr = cleanText(currency || "DOP").toUpperCase() || "DOP";
  if (!value) return "";
  const digits = parsePriceNumber(value);
  if (!digits) return value;
  const prefix = curr === "USD" ? "US$" : curr === "DOP" ? "RD$" : `${curr} `;
  return `${prefix}${Number(digits).toLocaleString("es-DO")}`.trim();
}

function cleanImportedShortDescription(description = "") {
  const cleaned = cleanText(
    String(description || "")
      .replace(/propiedad en (venta|alquiler)\.?/gi, "")
      .replace(/ubicaci[oó]n:\s*[^.]+\.?/gi, "")
      .replace(/detalles:\s*/gi, "")
      .replace(/\s+/g, " ")
  );
  return cleaned;
}

function detectOperationFromMetaText(text = "", fallback = "venta") {
  const t = normalizeText(text);
  if (t.includes("alquiler") || t.includes("renta") || t.includes("rent")) return "alquiler";
  if (t.includes("venta") || t.includes("comprar") || t.includes("sale")) return "venta";
  return cleanText(fallback || "venta") || "venta";
}

function detectCategoryFromMetaText(text = "", fallback = "apartamentos") {
  const t = normalizeText(text);
  if (t.includes("local") || t.includes("nave")) return "locales_comerciales";
  if (t.includes("solar")) return "solares";
  if (t.includes("proyecto") || t.includes("en plano")) return "proyectos";
  if (t.includes("casa")) return "casas";
  if (t.includes("aparta") || t.includes("apto") || t.includes("apartaestudio") || t.includes("apartastudio") || t.includes("estudio")) return "apartamentos";
  return cleanText(fallback || "apartamentos") || "apartamentos";
}

function extractLocationFromMetaDescription(description = "", fallback = "") {
  const raw = String(description || "");
  const patterns = [
    /ubicaci[oó]n:\s*([^\n.]+)/i,
    /📍\s*([^\n]+)/i,
    /ubicada? en\s+([^\n.]+)/i,
  ];

  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (match?.[1]) return cleanText(match[1]);
  }

  return cleanText(fallback || "");
}

function parseMetricFromText(text = "", kind = "") {
  const raw = String(text || "");
  if (!raw) return "";
  const patternsByKind = {
    bedrooms: [/(\d+(?:[.,]\d+)?)\s*(hab|habitaci[oó]n|habitaciones|cuartos?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis)\s*(hab|habitaci[oó]n|habitaciones|cuartos?)/i],
    bathrooms: [/(\d+(?:[.,]\d+)?)\s*(bañ[oa]s?|banos?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis|medio|media)\s*(bañ[oa]s?|banos?)/i],
    parking: [/(\d+(?:[.,]\d+)?)\s*(parqueos?|veh[íi]culos?|marquesinas?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis)\s*(parqueos?|veh[íi]culos?|marquesinas?)/i],
    area_m2: [/(\d+(?:[.,]\d+)?)\s*(m2|mts2|mt2|m²|metros? cuadrados?)/i],
  };

  const patterns = patternsByKind[kind] || [];
  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (match?.[1]) {
      const parsed = parseFlexibleNumber(match[1]);
      if (parsed !== "") return parsed;
    }
  }
  return "";
}

function extractPhoneFromText(text = "", fallback = "") {
  const raw = String(text || "");
  const match = raw.match(/(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
  return cleanText(match?.[0] || fallback || "");
}

function stripMetaPrefixFromName(name = "", fallback = "") {
  const cleaned = cleanText(String(name || "").replace(/^(VENTA|ALQUILER)\s*[|:-]\s*/i, ""));
  return cleaned || cleanText(fallback || "");
}

function buildImportedPropertyFromMetaProduct(product = {}, existing = null) {
  const nowIso = new Date().toISOString();
  const retailerId = cleanText(product?.retailer_id || product?.product_retailer_id || existing?.retailer_id || existing?.code || existing?.id || "");
  const title = stripMetaPrefixFromName(product?.name || existing?.title || retailerId, existing?.title || retailerId);
  const description = cleanText(product?.description || existing?.short_description || "");
  const operation = detectOperationFromMetaText(`${cleanText(product?.name)} ${description}`, existing?.operation || "venta");
  const category = detectCategoryFromMetaText(`${title} ${description}`, existing?.category || "apartamentos");
  const currency = cleanText(product?.currency || existing?.currency || (String(product?.price || "").toUpperCase().includes("USD") ? "USD" : "DOP") || "DOP") || "DOP";
  const formattedPrice = formatCatalogPriceFromMeta(product?.price || existing?.price || "", currency) || cleanText(existing?.price || "");
  const location = extractLocationFromMetaDescription(description, existing?.location || "");

  return normalizeProperty({
    ...(existing || {}),
    id: cleanText(existing?.id || retailerId || cleanText(product?.id || "") || cleanText(product?.name || "")) || retailerId,
    retailer_id: retailerId,
    product_retailer_id: retailerId,
    code: cleanText(existing?.code || retailerId || cleanText(product?.id || "")) || retailerId,
    title,
    category,
    operation,
    price: formattedPrice,
    currency,
    location,
    short_description: cleanImportedShortDescription(description) || cleanText(existing?.short_description || description || ""),
    bedrooms: existing?.bedrooms !== "" && existing?.bedrooms !== undefined && existing?.bedrooms !== null ? existing.bedrooms : parseMetricFromText(description, "bedrooms"),
    bathrooms: existing?.bathrooms !== "" && existing?.bathrooms !== undefined && existing?.bathrooms !== null ? existing.bathrooms : parseMetricFromText(description, "bathrooms"),
    parking: existing?.parking !== "" && existing?.parking !== undefined && existing?.parking !== null ? existing.parking : parseMetricFromText(description, "parking"),
    area_m2: existing?.area_m2 !== "" && existing?.area_m2 !== undefined && existing?.area_m2 !== null ? existing.area_m2 : parseMetricFromText(description, "area_m2"),
    meta_url: cleanText(product?.url || existing?.meta_url || ""),
    meta_image_url: cleanText(product?.image_url || existing?.meta_image_url || ""),
    meta_availability: cleanText(product?.availability || existing?.meta_availability || "in stock") || "in stock",
    agent_phone: cleanText(existing?.agent_phone || extractPhoneFromText(description, "")),
    created_at: cleanText(existing?.created_at || nowIso),
    updated_at: nowIso,
  });
}

function computeStats(items = []) {
  const total = items.length;
  const active = items.filter((p) => p.active !== false).length;
  const alquiler = items.filter((p) => normalizeText(p.operation) === "alquiler").length;
  const venta = items.filter((p) => normalizeText(p.operation) === "venta").length;
  const metaReady = items.filter((p) => buildMetaPayload(p, {}, {}).url && buildMetaPayload(p, {}, {}).image_url && buildMetaPayload(p, {}, {}).price).length;
  return { total, active, alquiler, venta, metaReady };
}

function serializeCatalogJson(items = []) {
  const cleaned = items.map((p) => ({ ...p, product_retailer_id: p.retailer_id }));
  return JSON.stringify(cleaned, null, 2);
}

function createSessionToken(username, secret, ttlMs) {
  const payload = { username, exp: Date.now() + ttlMs };
  const encoded = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signature = crypto.createHmac("sha256", secret).update(encoded).digest("base64url");
  return `${encoded}.${signature}`;
}

function verifySessionToken(token, secret) {
  if (!token || !token.includes(".")) return null;
  const [encoded, signature] = token.split(".");
  const expected = crypto.createHmac("sha256", secret).update(encoded).digest("base64url");
  const a = Buffer.from(signature || "", "utf8");
  const b = Buffer.from(expected || "", "utf8");
  if (!a.length || a.length !== b.length) return null;
  if (!crypto.timingSafeEqual(a, b)) return null;
  const payload = safeJson(Buffer.from(encoded, "base64url").toString("utf8"), null);
  if (!payload?.username || !payload?.exp || payload.exp < Date.now()) return null;
  return payload;
}

function parseCookies(req) {
  const header = String(req.headers.cookie || "");
  const cookies = {};
  for (const part of header.split(";")) {
    const idx = part.indexOf("=");
    if (idx === -1) continue;
    const key = part.slice(0, idx).trim();
    const value = decodeURIComponent(part.slice(idx + 1).trim());
    cookies[key] = value;
  }
  return cookies;
}

function buildMetaPayload(property, defaults = {}, options = {}) {
  const availability = cleanText(property.meta_availability || defaults.metaAvailability || "in stock") || "in stock";
  const url = cleanText(property.meta_url || defaults.metaUrl || "");
  const imageUrl = cleanText(property.meta_image_url || defaults.metaImageUrl || "");
  return {
    retailer_id: cleanText(property.retailer_id || property.code || property.id),
    name: buildMetaName(property),
    description: formatMetaDescription(property),
    price: parsePriceNumber(property.price),
    currency: cleanText(property.currency || "DOP") || "DOP",
    availability,
    url,
    image_url: imageUrl,
    ...(options.extraMetaFields || {}),
  };
}

export function createCatalogAdmin(options = {}) {
  const router = express.Router();
  const ADMIN_BASE_PATH = cleanText(options.basePath || "/admin") || "/admin";
  const ADMIN_USERNAME = cleanText(options.adminUsername || process.env.ADMIN_PANEL_USERNAME || "admin") || "admin";
  const ADMIN_PASSWORD = cleanText(options.adminPassword || process.env.ADMIN_PANEL_PASSWORD || "admin123456") || "admin123456";
  const SESSION_SECRET = cleanText(options.sessionSecret || process.env.ADMIN_PANEL_SESSION_SECRET || "catalog-admin-secret") || "catalog-admin-secret";
  const SESSION_TTL_MS = Number(options.sessionTtlMs || process.env.ADMIN_PANEL_SESSION_TTL_MS || 1000 * 60 * 60 * 10);
  const SESSION_COOKIE = cleanText(options.sessionCookie || "lv_catalog_admin_session") || "lv_catalog_admin_session";
  const BUSINESS_NAME = cleanText(options.businessName || process.env.BUSINESS_NAME || "LV Inmobiliaria") || "LV Inmobiliaria";
  const RENDER_API_KEY = cleanText(options.renderApiKey || process.env.RENDER_API_KEY || "");
  const RENDER_API_BASE_URL = cleanText(options.renderApiBaseUrl || process.env.RENDER_API_BASE_URL || "https://api.render.com/v1").replace(/\/$/, "");
  const RENDER_BOT_SERVICE_ID = cleanText(options.renderBotServiceId || process.env.RENDER_BOT_SERVICE_ID || "");
  const RENDER_BOT_ENV_KEY = cleanText(options.renderBotEnvKey || process.env.RENDER_BOT_ENV_KEY || "PROPERTY_CATALOG_JSON") || "PROPERTY_CATALOG_JSON";
  const RENDER_DEPLOY_HOOK_URL = cleanText(options.renderDeployHookUrl || process.env.RENDER_BOT_DEPLOY_HOOK_URL || "");
  const RENDER_TRIGGER_DEPLOY_ON_SYNC = String(options.renderTriggerDeployOnSync ?? process.env.RENDER_TRIGGER_DEPLOY_ON_SYNC ?? "0") === "1";
  const META_ACCESS_TOKEN = cleanText(options.metaAccessToken || process.env.META_ACCESS_TOKEN || "");
  const META_CATALOG_ID = cleanText(options.metaCatalogId || process.env.META_CATALOG_ID || "");
  const META_GRAPH_VERSION = cleanText(options.metaGraphVersion || process.env.META_GRAPH_VERSION || "v23.0") || "v23.0";
  const META_DEFAULT_URL = cleanText(options.metaDefaultUrl || process.env.META_DEFAULT_URL || "");
  const META_DEFAULT_IMAGE_URL = cleanText(options.metaDefaultImageUrl || process.env.META_DEFAULT_IMAGE_URL || "");
  const META_DEFAULT_AVAILABILITY = cleanText(options.metaDefaultAvailability || process.env.META_DEFAULT_AVAILABILITY || "in stock") || "in stock";
  const AUTO_SYNC_ON_SAVE = String(options.autoSyncOnSave ?? process.env.AUTO_SYNC_ON_SAVE ?? "1") === "1";
  const DATA_FILE = path.resolve(__dirname, options.dataFile || process.env.CATALOG_ADMIN_DATA_FILE || "./data/properties.store.json");
  const PUBLIC_DIR = path.resolve(__dirname, options.publicDir || "./public");
  const getCatalog = typeof options.getCatalog === "function" ? options.getCatalog : () => [];
  const setCatalog = typeof options.setCatalog === "function" ? options.setCatalog : async () => {};
  const extraMetaFields = options.extraMetaFields || {};

  let store = { properties: dedupeProperties(getCatalog()) };
  const syncState = {
    lastBotSyncAt: null,
    lastBotSyncOk: null,
    lastBotSyncMessage: "Aún no sincronizado",
    lastMetaSyncAt: null,
    lastMetaSyncOk: null,
    lastMetaSyncMessage: "Aún no sincronizado",
    lastMetaImportAt: null,
    lastMetaImportOk: null,
    lastMetaImportMessage: "Aún no importado desde Meta",
  };

  router.use(express.json({ limit: "6mb" }));
  router.use(express.urlencoded({ extended: true, limit: "6mb" }));

  async function ensureDataFile() {
    await fs.mkdir(path.dirname(DATA_FILE), { recursive: true });
    try {
      await fs.access(DATA_FILE);
    } catch {
      await fs.writeFile(DATA_FILE, JSON.stringify({ properties: store.properties }, null, 2), "utf8");
    }
  }

  async function readFileJson(file, fallback) {
    try {
      const raw = await fs.readFile(file, "utf8");
      return safeJson(raw, fallback);
    } catch {
      return fallback;
    }
  }

  async function writeStore() {
    store.properties = dedupeProperties(store.properties);
    await ensureDataFile();
    await fs.writeFile(DATA_FILE, JSON.stringify({ properties: store.properties }, null, 2), "utf8");
  }

  async function updateSharedCatalog(nextProperties = [], persist = true) {
    store.properties = dedupeProperties(nextProperties);
    await setCatalog(store.properties);
    if (persist) await writeStore();
    return store.properties;
  }

  async function init() {
    await ensureDataFile();
    const fromDisk = await readFileJson(DATA_FILE, { properties: [] });
    const sharedProps = dedupeProperties(getCatalog());
    let diskProps = Array.isArray(fromDisk?.properties) ? dedupeProperties(fromDisk.properties) : [];

    if (sharedProps.length) {
      diskProps = diskProps.filter((item) => !isPlaceholderProperty(item));
      const merged = mergePropertyCollections(sharedProps, diskProps);
      await updateSharedCatalog(merged, false);
      await writeStore();
      return;
    }

    if (diskProps.length) {
      await updateSharedCatalog(diskProps, false);
      return;
    }

    await writeStore();
  }

  function setSessionCookie(res, token) {
    const maxAge = Math.floor(SESSION_TTL_MS / 1000);
    res.setHeader(
      "Set-Cookie",
      `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=${ADMIN_BASE_PATH}; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`
    );
  }

  function clearSessionCookie(res) {
    res.setHeader("Set-Cookie", `${SESSION_COOKIE}=; Path=${ADMIN_BASE_PATH}; HttpOnly; SameSite=Lax; Max-Age=0`);
  }

  function requireAuth(req, res, next) {
    const cookies = parseCookies(req);
    const token = cookies[SESSION_COOKIE];
    const session = verifySessionToken(token, SESSION_SECRET);
    if (!session) return res.status(401).json({ ok: false, error: "No autorizado" });
    req.adminSession = session;
    next();
  }

  async function updateRenderCatalogEnv(catalogJson) {
    if (!RENDER_API_KEY || !RENDER_BOT_SERVICE_ID) return { ok: true, skipped: true, message: "Sin credenciales Render" };

    const headers = {
      Authorization: `Bearer ${RENDER_API_KEY}`,
      "Content-Type": "application/json",
    };

    const attempts = [
      {
        method: "patch",
        url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}`,
        body: { envVars: [{ key: RENDER_BOT_ENV_KEY, value: catalogJson }] },
      },
      {
        method: "patch",
        url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars`,
        body: [{ key: RENDER_BOT_ENV_KEY, value: catalogJson }],
      },
      {
        method: "put",
        url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars/${encodeURIComponent(RENDER_BOT_ENV_KEY)}`,
        body: { value: catalogJson },
      },
      {
        method: "post",
        url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars`,
        body: { key: RENDER_BOT_ENV_KEY, value: catalogJson },
      },
    ];

    let lastError = null;
    for (const attempt of attempts) {
      try {
        const res = await axios({
          method: attempt.method,
          url: attempt.url,
          headers,
          data: attempt.body,
          timeout: 25000,
          validateStatus: () => true,
        });
        if (res.status >= 200 && res.status < 300) {
          return { ok: true, response: res.data, attempt: `${attempt.method.toUpperCase()} ${attempt.url}` };
        }
        lastError = new Error(`Render respondió ${res.status}: ${JSON.stringify(res.data)}`);
      } catch (error) {
        lastError = error;
      }
    }
    throw lastError || new Error("No se pudo actualizar la variable en Render");
  }

  async function triggerRenderDeploy() {
    if (!RENDER_TRIGGER_DEPLOY_ON_SYNC) return { ok: true, skipped: true, mode: "skip_deploy_same_service" };
    if (RENDER_DEPLOY_HOOK_URL) {
      await axios.post(RENDER_DEPLOY_HOOK_URL, {}, { timeout: 20000 });
      return { ok: true, mode: "deploy_hook" };
    }
    if (!RENDER_API_KEY || !RENDER_BOT_SERVICE_ID) return { ok: false, mode: "none" };

    const headers = { Authorization: `Bearer ${RENDER_API_KEY}` };
    const attempts = [
      `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/deploys`,
      `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/deploy`,
    ];

    for (const url of attempts) {
      const res = await axios.post(url, {}, { headers, timeout: 20000, validateStatus: () => true });
      if (res.status >= 200 && res.status < 300) return { ok: true, mode: url };
    }

    return { ok: false, mode: "unknown" };
  }

  async function syncBotCatalog(properties) {
    await updateSharedCatalog(properties, true);
    const catalogJson = serializeCatalogJson(store.properties);
    const envRes = await updateRenderCatalogEnv(catalogJson);
    const deployRes = await triggerRenderDeploy();
    syncState.lastBotSyncAt = new Date().toISOString();
    syncState.lastBotSyncOk = true;
    syncState.lastBotSyncMessage = envRes?.skipped
      ? "Catálogo activo en memoria del bot. Sincronización persistente en Render pendiente por falta de credenciales."
      : deployRes.ok && !deployRes.skipped
      ? `Catálogo del bot actualizado, Render sincronizado y deploy lanzado (${deployRes.mode}).`
      : "Catálogo del bot actualizado en memoria y variable de Render sincronizada.";
    return { ok: true, envRes, deployRes, message: syncState.lastBotSyncMessage };
  }

  async function postMetaProduct(payload) {
    const url = `https://graph.facebook.com/${META_GRAPH_VERSION}/${META_CATALOG_ID}/products`;
    const params = new URLSearchParams();
    Object.entries(payload).forEach(([key, value]) => {
      if (value !== undefined && value !== null && String(value).trim() !== "") params.append(key, String(value));
    });
    const res = await axios.post(url, params, {
      headers: {
        Authorization: `Bearer ${META_ACCESS_TOKEN}`,
        "Content-Type": "application/x-www-form-urlencoded",
      },
      timeout: 30000,
      validateStatus: () => true,
    });
    if (res.status >= 200 && res.status < 300) return res.data;
    throw new Error(`Meta respondió ${res.status}: ${JSON.stringify(res.data)}`);
  }

  async function syncMetaCatalog(properties) {
    if (!META_ACCESS_TOKEN) throw new Error("Falta META_ACCESS_TOKEN");
    if (!META_CATALOG_ID) throw new Error("Falta META_CATALOG_ID");

    const results = [];
    for (const property of properties) {
      const payload = buildMetaPayload(
        property,
        {
          metaUrl: META_DEFAULT_URL,
          metaImageUrl: META_DEFAULT_IMAGE_URL,
          metaAvailability: META_DEFAULT_AVAILABILITY,
        },
        { extraMetaFields }
      );
      if (!payload.retailer_id || !payload.name || !payload.price || !payload.currency) {
        results.push({ id: property.id, ok: false, message: "Faltan campos mínimos para Meta (retailer_id, name, price, currency)." });
        continue;
      }
      if (!payload.url || !payload.image_url) {
        results.push({ id: property.id, ok: false, message: "Falta meta_url o meta_image_url para sincronizar con Meta." });
        continue;
      }
      try {
        const data = await postMetaProduct(payload);
        results.push({ id: property.id, ok: true, data });
      } catch (error) {
        results.push({ id: property.id, ok: false, message: error.message });
      }
    }

    const okCount = results.filter((r) => r.ok).length;
    const failCount = results.length - okCount;
    syncState.lastMetaSyncAt = new Date().toISOString();
    syncState.lastMetaSyncOk = failCount === 0;
    syncState.lastMetaSyncMessage = `Meta sincronizado: ${okCount} OK, ${failCount} con error.`;
    return { ok: failCount === 0, results, message: syncState.lastMetaSyncMessage };
  }


  async function fetchMetaCatalogProducts() {
    if (!META_ACCESS_TOKEN) throw new Error("Falta META_ACCESS_TOKEN");
    if (!META_CATALOG_ID) throw new Error("Falta META_CATALOG_ID");

    const items = [];
    let after = "";
    let pageCount = 0;

    while (pageCount < 25) {
      pageCount += 1;
      const res = await axios.get(`https://graph.facebook.com/${META_GRAPH_VERSION}/${META_CATALOG_ID}/products`, {
        headers: { Authorization: `Bearer ${META_ACCESS_TOKEN}` },
        params: {
          fields: "id,retailer_id,name,description,price,currency,url,image_url,availability",
          limit: 100,
          ...(after ? { after } : {}),
        },
        timeout: 30000,
        validateStatus: () => true,
      });

      if (res.status < 200 || res.status >= 300) {
        throw new Error(`Meta respondió ${res.status}: ${JSON.stringify(res.data)}`);
      }

      const pageItems = Array.isArray(res.data?.data) ? res.data.data : [];
      items.push(...pageItems);

      const nextAfter = cleanText(res.data?.paging?.cursors?.after || "");
      if (!nextAfter || !pageItems.length) break;
      after = nextAfter;
    }

    return items;
  }

  async function importMetaCatalogIntoPanel() {
    const metaItems = await fetchMetaCatalogProducts();
    const current = dedupeProperties(store.properties);
    const currentByKey = new Map(current.map((item) => [getPropertyMergeKey(item), item]));

    const imported = metaItems
      .map((product) => buildImportedPropertyFromMetaProduct(product, currentByKey.get(getPropertyMergeKey(product)) || null))
      .filter(Boolean);

    const merged = mergePropertyCollections(current, imported);
    await updateSharedCatalog(merged, true);

    const importedCount = imported.length;
    const createdCount = imported.filter((item) => !currentByKey.has(getPropertyMergeKey(item))).length;
    const updatedCount = importedCount - createdCount;

    syncState.lastMetaImportAt = new Date().toISOString();
    syncState.lastMetaImportOk = true;
    syncState.lastMetaImportMessage = `Meta importado: ${importedCount} leídas, ${createdCount} nuevas, ${updatedCount} actualizadas.`;

    return {
      ok: true,
      items: store.properties,
      importedCount,
      createdCount,
      updatedCount,
      message: syncState.lastMetaImportMessage,
    };
  }

  async function maybeAutoSync() {
    if (!AUTO_SYNC_ON_SAVE) return { ok: true, skipped: true };
    const bot = await syncBotCatalog(store.properties);
    let meta = { ok: false, skipped: true };
    if (META_ACCESS_TOKEN && META_CATALOG_ID) {
      meta = await syncMetaCatalog(store.properties);
    }
    return { ok: true, bot, meta };
  }

  function pickEditablePropertyPayload(body = {}) {
    const raw = { ...body };
    delete raw.created_at;
    delete raw.updated_at;
    return normalizeProperty({
      ...raw,
      features: parseList(raw.features),
      nearby_places: parseList(raw.nearby_places),
      faq: typeof raw.faq === "string" ? safeJson(raw.faq, {}) : raw.faq,
      active: raw.active === false || raw.active === "false" ? false : true,
      updated_at: new Date().toISOString(),
      created_at: raw.created_at || new Date().toISOString(),
    });
  }

  function validateProperty(property, currentId = "") {
    const errors = [];
    if (!cleanText(property.id)) errors.push("El campo id es obligatorio.");
    if (!cleanText(property.retailer_id)) errors.push("El campo retailer_id es obligatorio.");
    if (!cleanText(property.code)) errors.push("El campo code es obligatorio.");
    if (!cleanText(property.title)) errors.push("El campo title es obligatorio.");
    if (!cleanText(property.category)) errors.push("El campo category es obligatorio.");
    if (!cleanText(property.operation)) errors.push("El campo operation es obligatorio.");
    if (!cleanText(property.currency)) errors.push("El campo currency es obligatorio.");

    const duplicated = store.properties.find((item) => {
      if (currentId && item.id === currentId) return false;
      return (
        normalizeText(item.id) === normalizeText(property.id) ||
        normalizeText(item.retailer_id) === normalizeText(property.retailer_id) ||
        normalizeText(item.code) === normalizeText(property.code)
      );
    });

    if (duplicated) errors.push("Ya existe una propiedad con el mismo id, retailer_id o code.");
    return errors;
  }

  function getFilteredProperties(query = {}) {
    const q = normalizeText(query.q || "");
    const category = normalizeText(query.category || "");
    const operation = normalizeText(query.operation || "");
    const active = cleanText(query.active || "");

    return store.properties.filter((item) => {
      if (category && normalizeText(item.category) !== category) return false;
      if (operation && normalizeText(item.operation) !== operation) return false;
      if (active === "true" && item.active !== true) return false;
      if (active === "false" && item.active !== false) return false;
      if (!q) return true;
      const haystack = normalizeText([
        item.id,
        item.retailer_id,
        item.code,
        item.title,
        item.location,
        item.short_description,
        item.agent_name,
        item.agent_phone,
      ].join(" | "));
      return haystack.includes(q);
    });
  }

  router.get("/api/health", (_req, res) => {
    res.json({ ok: true, service: "catalog-admin", business: BUSINESS_NAME, now: new Date().toISOString() });
  });

  router.get("/api/session", (req, res) => {
    const cookies = parseCookies(req);
    const token = cookies[SESSION_COOKIE];
    const session = verifySessionToken(token, SESSION_SECRET);
    res.json({ ok: true, authenticated: !!session, username: session?.username || null });
  });

  router.post("/api/auth/login", (req, res) => {
    const username = cleanText(req.body?.username);
    const password = cleanText(req.body?.password);
    if (username !== ADMIN_USERNAME || password !== ADMIN_PASSWORD) {
      return res.status(401).json({ ok: false, error: "Credenciales inválidas" });
    }
    const token = createSessionToken(username, SESSION_SECRET, SESSION_TTL_MS);
    setSessionCookie(res, token);
    return res.json({ ok: true, username });
  });

  router.post("/api/auth/logout", (_req, res) => {
    clearSessionCookie(res);
    return res.json({ ok: true });
  });

  router.get("/api/config/status", requireAuth, (_req, res) => {
    const stats = computeStats(store.properties);
    return res.json({
      ok: true,
      businessName: BUSINESS_NAME,
      stats,
      integrations: {
        renderReady: !!(RENDER_API_KEY && RENDER_BOT_SERVICE_ID),
        renderBotEnvKey: RENDER_BOT_ENV_KEY,
        renderDeployHook: !!RENDER_DEPLOY_HOOK_URL,
        renderTriggerDeployOnSync: RENDER_TRIGGER_DEPLOY_ON_SYNC,
        metaReady: !!(META_ACCESS_TOKEN && META_CATALOG_ID),
        metaImportReady: !!(META_ACCESS_TOKEN && META_CATALOG_ID),
        metaCatalogId: META_CATALOG_ID || null,
        autoSyncOnSave: AUTO_SYNC_ON_SAVE,
      },
      syncState,
    });
  });

  router.get("/api/properties", requireAuth, (req, res) => {
    const items = getFilteredProperties(req.query);
    return res.json({ ok: true, items: sortProperties(items), stats: computeStats(store.properties) });
  });

  router.get("/api/properties/export", requireAuth, (_req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Disposition", 'attachment; filename="PROPERTY_CATALOG_JSON.json"');
    res.send(serializeCatalogJson(store.properties));
  });

  router.post("/api/properties/import", requireAuth, async (req, res) => {
    const jsonText = cleanText(req.body?.jsonText || "");
    const parsed = safeJson(jsonText, null);
    if (!Array.isArray(parsed)) return res.status(400).json({ ok: false, error: "Debes pegar un JSON array válido." });
    await updateSharedCatalog(parsed, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    return res.json({ ok: true, items: store.properties, stats: computeStats(store.properties), auto });
  });

  router.post("/api/properties", requireAuth, async (req, res) => {
    const property = pickEditablePropertyPayload(req.body || {});
    const errors = validateProperty(property);
    if (errors.length) return res.status(400).json({ ok: false, errors });
    store.properties.unshift(property);
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    return res.json({ ok: true, item: property, stats: computeStats(store.properties), auto });
  });

  router.put("/api/properties/:id", requireAuth, async (req, res) => {
    const id = cleanText(req.params.id);
    const current = store.properties.find((item) => item.id === id);
    if (!current) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    const property = pickEditablePropertyPayload({ ...current, ...req.body, created_at: current.created_at, updated_at: new Date().toISOString() });
    const errors = validateProperty(property, id);
    if (errors.length) return res.status(400).json({ ok: false, errors });
    store.properties = store.properties.map((item) => (item.id === id ? property : item));
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    return res.json({ ok: true, item: property, stats: computeStats(store.properties), auto });
  });

  router.delete("/api/properties/:id", requireAuth, async (req, res) => {
    const id = cleanText(req.params.id);
    const exists = store.properties.some((item) => item.id === id);
    if (!exists) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    store.properties = store.properties.filter((item) => item.id !== id);
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    return res.json({ ok: true, stats: computeStats(store.properties), auto });
  });

  router.post("/api/sync/bot", requireAuth, async (_req, res) => {
    try {
      const result = await syncBotCatalog(store.properties);
      return res.json({ ok: true, ...result });
    } catch (error) {
      syncState.lastBotSyncAt = new Date().toISOString();
      syncState.lastBotSyncOk = false;
      syncState.lastBotSyncMessage = error.message;
      return res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/meta/import", requireAuth, async (_req, res) => {
    try {
      const result = await importMetaCatalogIntoPanel();
      return res.json({ ok: true, ...result, stats: computeStats(store.properties), syncState });
    } catch (error) {
      syncState.lastMetaImportAt = new Date().toISOString();
      syncState.lastMetaImportOk = false;
      syncState.lastMetaImportMessage = error.message;
      return res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/sync/meta", requireAuth, async (_req, res) => {
    try {
      const result = await syncMetaCatalog(store.properties);
      return res.json({ ok: result.ok, ...result });
    } catch (error) {
      syncState.lastMetaSyncAt = new Date().toISOString();
      syncState.lastMetaSyncOk = false;
      syncState.lastMetaSyncMessage = error.message;
      return res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/sync/all", requireAuth, async (_req, res) => {
    try {
      const bot = await syncBotCatalog(store.properties);
      const meta = META_ACCESS_TOKEN && META_CATALOG_ID ? await syncMetaCatalog(store.properties) : { ok: false, skipped: true };
      return res.json({ ok: bot.ok && (meta.ok || meta.skipped), bot, meta, syncState });
    } catch (error) {
      return res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.get("/api/meta-preview/:id", requireAuth, (req, res) => {
    const property = store.properties.find((item) => item.id === req.params.id);
    if (!property) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    return res.json({
      ok: true,
      property,
      metaPayload: buildMetaPayload(
        property,
        {
          metaUrl: META_DEFAULT_URL,
          metaImageUrl: META_DEFAULT_IMAGE_URL,
          metaAvailability: META_DEFAULT_AVAILABILITY,
        },
        { extraMetaFields }
      ),
    });
  });

  router.get("/styles.css", (_req, res) => {
    res.sendFile(path.join(PUBLIC_DIR, "styles.css"));
  });

  router.get("/app.js", (_req, res) => {
    res.sendFile(path.join(PUBLIC_DIR, "app.js"));
  });

  router.get("/", async (_req, res) => {
    const html = await fs.readFile(path.join(PUBLIC_DIR, "index.html"), "utf8");
    return res.type("html").send(
      html
        .replaceAll("__ADMIN_BASE_PATH__", ADMIN_BASE_PATH)
        .replaceAll("__BUSINESS_NAME__", BUSINESS_NAME)
    );
  });

  return { router, init };
}
