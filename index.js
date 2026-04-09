import express from "express";
import axios from "axios";
import crypto from "crypto";
import { google } from "googleapis";
import Redis from "ioredis";
import { createCatalogAdmin } from "./catalog-admin/router.js";

// =========================
// Helpers base
// =========================
function safeJson(str, fallback) {
  try {
    return JSON.parse(str);
  } catch {
    return fallback;
  }
}

function normalizeText(t) {
  return String(t || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/\p{Diacritic}/gu, "")
    .replace(/\s+/g, " ")
    .trim();
}

function addMinutes(date, minutes) {
  return new Date(date.getTime() + minutes * 60000);
}

function removeUndefinedDeep(value) {
  if (Array.isArray(value)) return value.map(removeUndefinedDeep);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value)
        .filter(([, v]) => v !== undefined)
        .map(([k, v]) => [k, removeUndefinedDeep(v)])
    );
  }
  return value;
}

function stableStringify(obj) {
  if (obj === null || typeof obj !== "object") return JSON.stringify(obj);
  if (Array.isArray(obj)) return `[${obj.map(stableStringify).join(",")}]`;
  const keys = Object.keys(obj).sort();
  return `{${keys.map((k) => JSON.stringify(k) + ":" + stableStringify(obj[k])).join(",")}}`;
}

function normalizePhoneDigits(raw) {
  return String(raw || "").replace(/[^\d]/g, "");
}

function toE164DigitsRD(phoneDigits) {
  const d = normalizePhoneDigits(phoneDigits);
  if (d.length === 10) return "1" + d;
  if (d.length === 11 && d.startsWith("1")) return d;
  return d;
}

function timingSafeEqualHex(aHex, bHex) {
  const a = Buffer.from(String(aHex || ""), "utf8");
  const b = Buffer.from(String(bHex || ""), "utf8");
  if (!a.length || a.length !== b.length) return false;
  return crypto.timingSafeEqual(a, b);
}

function extFromMimeType(mime) {
  const m = String(mime || "").toLowerCase();
  if (m.includes("audio/ogg")) return ".ogg";
  if (m.includes("audio/mpeg") || m.includes("audio/mp3")) return ".mp3";
  if (m.includes("audio/wav")) return ".wav";
  if (m.includes("audio/webm")) return ".webm";
  if (m.includes("image/jpeg")) return ".jpg";
  if (m.includes("image/png")) return ".png";
  if (m.includes("image/gif")) return ".gif";
  if (m.includes("image/webp")) return ".webp";
  if (m.includes("video/mp4")) return ".mp4";
  if (m.includes("application/pdf")) return ".pdf";
  if (m.includes("word")) return ".docx";
  if (m.includes("sheet")) return ".xlsx";
  if (m.includes("presentation")) return ".pptx";
  return "";
}

function sanitizeFileName(name, fallback = "file") {
  const raw = String(name || fallback).trim() || fallback;
  return raw.replace(/[\\/:*?"<>|]+/g, "_");
}

function cleanText(value) {
  return String(value ?? "").trim();
}

function safeArrayText(value) {
  if (Array.isArray(value)) return value.map((v) => cleanText(v)).filter(Boolean);
  return String(value || "")
    .split(/[\n,|•]+/g)
    .map((v) => cleanText(v))
    .filter(Boolean);
}

function toBoolOrNull(value) {
  if (value === true || value === false) return value;
  const t = normalizeText(value);
  if (!t) return null;

  if (["si", "sí", "true", "1", "yes", "aplica", "incluye", "disponible", "tiene"].includes(t)) return true;
  if (["no", "false", "0", "none", "no aplica", "no disponible", "no tiene"].includes(t)) return false;

  return null;
}

function yesNoUnknown(value, yes = "Sí", no = "No", unknown = "No especificado") {
  const b = toBoolOrNull(value);
  if (b === true) return yes;
  if (b === false) return no;
  const t = cleanText(value);
  return t || unknown;
}

function hasAnyKeyword(textNorm, keywords = []) {
  const t = normalizeText(textNorm || "");
  return keywords.some((k) => t.includes(normalizeText(k)));
}

function digitsOnly(value) {
  return String(value || "").replace(/[^\d]/g, "");
}

function bufferToDataUrl(buffer, mimeType = "application/octet-stream") {
  return `data:${mimeType};base64,${Buffer.from(buffer).toString("base64")}`;
}

function extractSharedContactsDetails(contacts = []) {
  const list = Array.isArray(contacts) ? contacts : [];

  return list.slice(0, 3).map((c) => {
    const formattedName = cleanText(
      c?.name?.formatted_name ||
        [c?.name?.first_name, c?.name?.middle_name, c?.name?.last_name]
          .map(cleanText)
          .filter(Boolean)
          .join(" ")
    );

    const phones = (Array.isArray(c?.phones) ? c.phones : [])
      .map((p) => cleanText(p?.phone || p?.wa_id || p?.value))
      .filter(Boolean);

    const emails = (Array.isArray(c?.emails) ? c.emails : [])
      .map((e) => cleanText(e?.email || e?.value))
      .filter(Boolean);

    const company = cleanText(c?.org?.company || "");
    const department = cleanText(c?.org?.department || "");
    const title = cleanText(c?.org?.title || "");

    return {
      name: formattedName,
      phones,
      emails,
      company,
      department,
      title,
    };
  }).filter((c) => c.name || c.phones.length || c.emails.length || c.company || c.department || c.title);
}

function formatSharedContactsForText(contacts = []) {
  const items = extractSharedContactsDetails(contacts);
  if (!items.length) return "[CONTACTS]";

  return items
    .map((c, idx) => {
      const parts = [];
      if (c.name) parts.push(c.name);
      if (c.phones.length) parts.push(`Tel: ${c.phones.join(", ")}`);
      if (c.emails.length) parts.push(`Email: ${c.emails.join(", ")}`);
      if (c.company) parts.push(`Empresa: ${c.company}`);
      if (c.department) parts.push(`Depto: ${c.department}`);
      if (c.title) parts.push(`Cargo: ${c.title}`);
      return `Contacto ${idx + 1}: ${parts.join(" | ")}`;
    })
    .join("\n");
}

function getPrimarySharedContact(contacts = []) {
  const items = extractSharedContactsDetails(contacts);
  return items[0] || null;
}

function getPrimarySharedContactPhoneDigits(contacts = []) {
  const c = getPrimarySharedContact(contacts);
  if (!c?.phones?.length) return "";
  for (const p of c.phones) {
    const digits = normalizePhoneDigits(p);
    if (digits) return digits;
  }
  return "";
}

function getPrimarySharedContactName(contacts = []) {
  return cleanText(getPrimarySharedContact(contacts)?.name || "");
}

function defaultWorkHours() {
  return {
    mon: { start: "08:00", end: "17:30" },
    tue: { start: "08:00", end: "17:30" },
    wed: { start: "08:00", end: "17:30" },
    thu: { start: "08:00", end: "17:30" },
    fri: { start: "08:00", end: "17:30" },
    sat: { start: "08:00", end: "13:00" },
    sun: null,
  };
}

function defaultCategoryDuration() {
  return {
    alquiler: 30,
    venta: 45,
    solares: 30,
    proyectos: 45,
    locales_comerciales: 30,
    casas: 45,
    apartamentos: 30,
    default: 30,
  };
}

function defaultPropertyCategories() {
  return [
    { key: "alquiler", title: "Propiedades en alquiler", id: "cat_alquiler" },
    { key: "venta", title: "Propiedades en venta", id: "cat_venta" },
    { key: "solares", title: "Solares", id: "cat_solares" },
    { key: "proyectos", title: "Proyectos", id: "cat_proyectos" },
    { key: "locales_comerciales", title: "Locales comerciales", id: "cat_locales" },
    { key: "casas", title: "Casas", id: "cat_casas" },
    { key: "apartamentos", title: "Apartamentos", id: "cat_apartamentos" },
  ];
}

function getZonedParts(date, timeZone) {
  const fmt = new Intl.DateTimeFormat("en-CA", {
    timeZone,
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  });
  const parts = fmt.formatToParts(date);
  const obj = {};
  for (const p of parts) obj[p.type] = p.value;
  return {
    year: parseInt(obj.year, 10),
    month: parseInt(obj.month, 10),
    day: parseInt(obj.day, 10),
    hour: parseInt(obj.hour, 10),
    minute: parseInt(obj.minute, 10),
    second: parseInt(obj.second, 10),
  };
}

function getOffsetMinutes(date, timeZone) {
  const p = getZonedParts(date, timeZone);
  const asUTC = Date.UTC(p.year, p.month - 1, p.day, p.hour, p.minute, p.second);
  return (asUTC - date.getTime()) / 60000;
}

function zonedTimeToUtc({ year, month, day, hour, minute, second = 0 }, timeZone) {
  const guess = new Date(Date.UTC(year, month - 1, day, hour, minute, second));
  const offsetMin = getOffsetMinutes(guess, timeZone);
  return new Date(Date.UTC(year, month - 1, day, hour, minute, second) - offsetMin * 60000);
}

function weekdayKeyFromISOWeekday(isoWeekday) {
  return ["", "mon", "tue", "wed", "thu", "fri", "sat", "sun"][isoWeekday];
}

function formatTimeInTZ(iso, timeZone) {
  const d = new Date(iso);
  return new Intl.DateTimeFormat("es-DO", {
    timeZone,
    hour: "2-digit",
    minute: "2-digit",
    hour12: true,
  }).format(d);
}

function formatDateInTZ(iso, timeZone) {
  const d = new Date(iso);
  return new Intl.DateTimeFormat("es-DO", {
    timeZone,
    weekday: "long",
    day: "2-digit",
    month: "long",
    year: "numeric",
  }).format(d);
}

function startOfLocalDayUTC(date, tz) {
  const p = getZonedParts(date, tz);
  return zonedTimeToUtc({ year: p.year, month: p.month, day: p.day, hour: 0, minute: 0 }, tz);
}

function addLocalDaysUTC(dateUTC, days, tz) {
  const p = getZonedParts(dateUTC, tz);
  const base = new Date(Date.UTC(p.year, p.month - 1, p.day, 12, 0, 0));
  base.setUTCDate(base.getUTCDate() + days);
  return zonedTimeToUtc(
    { year: base.getUTCFullYear(), month: base.getUTCMonth() + 1, day: base.getUTCDate(), hour: 0, minute: 0 },
    tz
  );
}

function rangeForWholeMonth(year, month, timeZone) {
  const from = zonedTimeToUtc({ year, month, day: 1, hour: 0, minute: 0 }, timeZone);
  const toMonth = month === 12 ? { year: year + 1, month: 1 } : { year, month: month + 1 };
  const to = zonedTimeToUtc({ year: toMonth.year, month: toMonth.month, day: 1, hour: 0, minute: 0 }, timeZone);
  return { from: from.toISOString(), to: to.toISOString() };
}

function nextWeekdayFromTodayUTC(targetIsoDow, tz, isNext = false) {
  const now = new Date();
  const todayLocal = startOfLocalDayUTC(now, tz);
  const p = getZonedParts(todayLocal, tz);
  const mid = zonedTimeToUtc({ year: p.year, month: p.month, day: p.day, hour: 12, minute: 0 }, tz);
  const js = new Date(mid.toISOString());
  const isoToday = ((js.getUTCDay() + 6) % 7) + 1;

  let diff = targetIsoDow - isoToday;
  if (diff < 0) diff += 7;
  if (diff === 0 && isNext) diff = 7;

  return addLocalDaysUTC(todayLocal, diff, tz);
}

// =========================
// ENV
// =========================
const PORT = process.env.PORT || 3000;

const WA_TOKEN = process.env.WA_TOKEN;
const PHONE_NUMBER_ID = process.env.PHONE_NUMBER_ID;
const VERIFY_TOKEN = process.env.VERIFY_TOKEN;
const META_APP_SECRET = process.env.META_APP_SECRET;

const OPENAI_API_KEY = process.env.OPENAI_API_KEY || "";
const OPENAI_MODEL = process.env.OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_VISION_MODEL = process.env.OPENAI_VISION_MODEL || OPENAI_MODEL || "gpt-4.1-mini";
const OPENAI_AUDIO_MODEL = process.env.OPENAI_AUDIO_MODEL || "whisper-1";

const REAL_ESTATE_AI_ENABLED = (process.env.REAL_ESTATE_AI_ENABLED || "1") === "1";
const MEDIA_AI_ENABLED = (process.env.MEDIA_AI_ENABLED || "1") === "1";
const AI_PROPERTY_RECOMMENDATION_LIMIT = parseInt(process.env.AI_PROPERTY_RECOMMENDATION_LIMIT || "3", 10);
const MEDIA_AUDIO_MAX_BYTES = parseInt(process.env.MEDIA_AUDIO_MAX_BYTES || String(12 * 1024 * 1024), 10);
const MEDIA_IMAGE_MAX_BYTES = parseInt(process.env.MEDIA_IMAGE_MAX_BYTES || String(8 * 1024 * 1024), 10);

const GOOGLE_CALENDAR_ID = process.env.GOOGLE_CALENDAR_ID;
const BUSINESS_NAME = process.env.BUSINESS_NAME || process.env.CLINIC_NAME || "LV Inmobiliaria";
const BUSINESS_ADDRESS = process.env.BUSINESS_ADDRESS || process.env.CLINIC_ADDRESS || "";
const BUSINESS_TIMEZONE = process.env.BUSINESS_TIMEZONE || process.env.CLINIC_TIMEZONE || "America/Santo_Domingo";

const WORK_HOURS = safeJson(process.env.WORK_HOURS_JSON, null) || defaultWorkHours();
const CATEGORY_DURATION = safeJson(process.env.CATEGORY_DURATION_JSON, null) || defaultCategoryDuration();
const SLOT_STEP_MIN = parseInt(process.env.SLOT_STEP_MIN || "15", 10);
const DEFAULT_VISIT_DURATION_MIN = parseInt(process.env.DEFAULT_VISIT_DURATION_MIN || "30", 10);

const HOURLY_LIST_MODE = (process.env.HOURLY_LIST_MODE || "1") === "1";
const HOURLY_LIST_START = parseInt(process.env.HOURLY_LIST_START || "8", 10);
const HOURLY_LIST_END = parseInt(process.env.HOURLY_LIST_END || "17", 10);
const HOURLY_LIST_COUNT = HOURLY_LIST_END - HOURLY_LIST_START + 1;

const MIN_BOOKING_LEAD_MIN = parseInt(process.env.MIN_BOOKING_LEAD_MIN || "60", 10);
const DISPLAY_SLOTS_LIMIT = parseInt(process.env.DISPLAY_SLOTS_LIMIT || "16", 10);
const MAX_SLOTS_RETURN = parseInt(process.env.MAX_SLOTS_RETURN || "80", 10);

const REMINDER_24H = (process.env.REMINDER_24H || "1") === "1";
const REMINDER_2H = (process.env.REMINDER_2H || "1") === "1";
const PERSONAL_WA_TO = (process.env.PERSONAL_WA_TO || "").trim();

const WA_CATALOG_ID = (process.env.WA_CATALOG_ID || "").trim();

const BOTHUB_WEBHOOK_URL = (process.env.BOTHUB_WEBHOOK_URL || "").trim();
const BOTHUB_WEBHOOK_SECRET = (process.env.BOTHUB_WEBHOOK_SECRET || "").trim();
const BOTHUB_TIMEOUT_MS = Number(process.env.BOTHUB_TIMEOUT_MS || 6000);

const BOT_PUBLIC_BASE_URL = (process.env.BOT_PUBLIC_BASE_URL || "").replace(/\/$/, "");
const HUB_MEDIA_SECRET = (process.env.HUB_MEDIA_SECRET || BOTHUB_WEBHOOK_SECRET || VERIFY_TOKEN || "").trim();
const HUB_MEDIA_TTL_SEC = parseInt(process.env.HUB_MEDIA_TTL_SEC || "900", 10);
const META_GRAPH_VERSION = process.env.WHATSAPP_GRAPH_VERSION || process.env.META_GRAPH_VERSION || "v23.0";

const REDIS_URL_RAW = (process.env.REDIS_URL || "").trim();
const SESSION_TTL_SEC = parseInt(process.env.SESSION_TTL_SEC || String(60 * 60 * 24 * 14), 10);
const SESSION_PREFIX = process.env.SESSION_PREFIX || "tekko:realestate:sess:";

const PROPERTY_CATEGORIES = safeJson(process.env.PROPERTY_CATEGORIES_JSON, null) || defaultPropertyCategories();

function normalizeProperty(raw, index) {
  const id = String(raw?.id || `prop_${index + 1}`);
  const retailerId = String(raw?.retailer_id || raw?.product_retailer_id || raw?.code || id);
  const code = String(raw?.code || raw?.reference || retailerId || id);

  return {
    id,
    retailer_id: retailerId,
    product_retailer_id: retailerId,
    catalog_id: String(raw?.catalog_id || WA_CATALOG_ID || ""),
    code,
    title: String(raw?.title || raw?.name || code),
    category: String(raw?.category || raw?.type || "venta"),
    operation: String(raw?.operation || ""),
    price: raw?.price ?? "",
    currency: String(raw?.currency || ""),
    location: String(raw?.location || raw?.zone || ""),
    exact_address: cleanText(raw?.exact_address || raw?.direccion || raw?.direccion_exacta),
    exact_location_reference: cleanText(
      raw?.exact_location_reference || raw?.referencia_ubicacion || raw?.ubicacion_referencia
    ),

    bedrooms: raw?.bedrooms ?? raw?.rooms ?? "",
    bathrooms: raw?.bathrooms ?? "",
    parking: raw?.parking ?? raw?.parkings ?? "",
    floor_level: cleanText(raw?.floor_level || raw?.nivel || raw?.piso),

    area_m2: raw?.area_m2 ?? raw?.area ?? "",
    lot_m2: raw?.lot_m2 ?? raw?.solar_m2 ?? raw?.terreno_m2 ?? raw?.metros_solar ?? "",
    construction_m2:
      raw?.construction_m2 ?? raw?.construccion_m2 ?? raw?.metros_construccion ?? "",

    short_description: String(raw?.short_description || raw?.description || ""),
    features: safeArrayText(raw?.features),

    year_built: cleanText(raw?.year_built || raw?.ano_construccion || raw?.año_construccion),
    condition: cleanText(raw?.condition || raw?.estado_propiedad || raw?.terminacion || raw?.condicion),

    title_deed: raw?.title_deed ?? raw?.titulo_deslindado ?? raw?.title_deed_available ?? "",
    has_mortgage: raw?.has_mortgage ?? raw?.hipoteca ?? raw?.carga_legal ?? "",
    legal_status: cleanText(raw?.legal_status || raw?.estado_legal || raw?.legalidad),
    documents_up_to_date: raw?.documents_up_to_date ?? raw?.documentos_al_dia ?? "",

    bank_financing:
      raw?.bank_financing ?? raw?.acepta_financiamiento ?? raw?.financiamiento_bancario ?? "",
    bank_financing_note: cleanText(
      raw?.bank_financing_note || raw?.financiamiento_nota || raw?.financing_notes
    ),
    down_payment: cleanText(
      raw?.down_payment || raw?.inicial_requerido || raw?.separacion || raw?.separacion_requerida
    ),
    payment_facilities: cleanText(
      raw?.payment_facilities || raw?.facilidades_pago || raw?.owner_payment_facilities
    ),
    estimated_monthly_fee: cleanText(
      raw?.estimated_monthly_fee || raw?.cuota_aproximada || raw?.monthly_fee_estimate
    ),
    transfer_cost: cleanText(raw?.transfer_cost || raw?.costo_traspaso),

    sewer: raw?.sewer ?? raw?.cloaca ?? "",
    paved_street: raw?.paved_street ?? raw?.calle_asfaltada ?? "",
    water_service: raw?.water_service ?? raw?.servicio_agua ?? "",
    electric_service:
      raw?.electric_service ?? raw?.servicio_energia ?? raw?.servicio_electrica ?? "",

    nearby_places: safeArrayText(raw?.nearby_places || raw?.lugares_cercanos),
    safety: cleanText(raw?.safety || raw?.seguridad_zona || raw?.zona_segura),
    transport_access: cleanText(raw?.transport_access || raw?.acceso_transporte),

    purchase_steps: cleanText(raw?.purchase_steps || raw?.pasos_compra || raw?.proceso_compra),
    purchase_timeline: cleanText(
      raw?.purchase_timeline || raw?.tiempo_proceso || raw?.proceso_tiempo
    ),

    faq: typeof raw?.faq === "object" && raw?.faq ? raw.faq : {},

    status: String(raw?.status || (raw?.active === false ? "inactiva" : "disponible")),
    duration_min: Number(raw?.duration_min || 0),
    active:
      raw?.active !== false &&
      !["vendida", "alquilada", "rentada", "inactiva", "oculta", "no publicar"].includes(
        normalizeText(raw?.status || "disponible")
      ),
  };
}

function normalizeOperationKey(operation) {
  const t = normalizeText(operation || "");
  if (!t) return "";
  if (t.includes("alquiler") || t.includes("renta")) return "alquiler";
  if (t.includes("venta") || t.includes("compr")) return "venta";
  return "";
}

let PROPERTY_CATALOG = [];
const PROPERTY_CATEGORIES_BY_ID = Object.fromEntries(PROPERTY_CATEGORIES.map((c) => [c.id, c]));
const PROPERTY_CATEGORIES_BY_KEY = Object.fromEntries(PROPERTY_CATEGORIES.map((c) => [c.key, c]));
let PROPERTY_BY_ID = {};
let PROPERTY_BY_RETAILER_ID = {};
let PROPERTY_BY_CODE = {};
let PROPERTY_BY_TITLE = {};
let PROPERTY_GROUPS = {};

function logPropertyCatalogStats() {
  console.log("TOTAL PROPERTY_CATALOG:", PROPERTY_CATALOG.length);
  console.log(
    "CASAS:",
    PROPERTY_CATALOG.filter((p) => normalizeText(p.category) === "casas").length
  );
  console.log(
    "LOCALES:",
    PROPERTY_CATALOG.filter((p) => normalizeText(p.category) === "locales_comerciales").length
  );
  console.log(
    "VENTA:",
    PROPERTY_CATALOG.filter((p) => normalizeOperationKey(p.operation || p.category || "") === "venta").length
  );
  console.log(
    "ALQUILER:",
    PROPERTY_CATALOG.filter((p) => normalizeOperationKey(p.operation || p.category || "") === "alquiler").length
  );
}

function groupPropertiesByCategory(properties) {
  const grouped = {};
  for (const p of properties) {
    const key = p.category || "otros";
    if (!grouped[key]) grouped[key] = [];
    grouped[key].push(p);
  }
  return grouped;
}

function refreshPropertyCatalog(nextCatalog = []) {
  const source = Array.isArray(nextCatalog) ? nextCatalog : [];
  PROPERTY_CATALOG = source.map(normalizeProperty).filter((p) => p.active);
  PROPERTY_BY_ID = Object.fromEntries(PROPERTY_CATALOG.map((p) => [p.id, p]));
  PROPERTY_BY_RETAILER_ID = Object.fromEntries(PROPERTY_CATALOG.map((p) => [p.retailer_id, p]));
  PROPERTY_BY_CODE = Object.fromEntries(PROPERTY_CATALOG.map((p) => [normalizeText(p.code), p]));
  PROPERTY_BY_TITLE = Object.fromEntries(PROPERTY_CATALOG.map((p) => [normalizeText(p.title), p]));
  PROPERTY_GROUPS = groupPropertiesByCategory(PROPERTY_CATALOG);
  logPropertyCatalogStats();
  return PROPERTY_CATALOG;
}

refreshPropertyCatalog(safeJson(process.env.PROPERTY_CATALOG_JSON, []) || []);

function getPropertiesForMenuCategory(categoryKey) {
  const normalizedInput = normalizeText(categoryKey || "");
  const resolvedKey =
    PROPERTY_CATEGORIES_BY_ID[categoryKey]?.key ||
    PROPERTY_CATEGORIES.find((c) => normalizeText(c.id) === normalizedInput)?.key ||
    PROPERTY_CATEGORIES.find((c) => normalizeText(c.title) === normalizedInput)?.key ||
    normalizedInput;

  if (!resolvedKey) return [];

  if (resolvedKey === "venta" || resolvedKey === "alquiler") {
    return PROPERTY_CATALOG.filter((p) => {
      const op = normalizeOperationKey(p.operation || p.category || "");
      return p.active && op === resolvedKey;
    });
  }

  return PROPERTY_CATALOG.filter((p) => {
    return p.active && normalizeText(p.category || "") === resolvedKey;
  });
}

function normalizeRedisUrl(url) {
  const u = String(url || "").trim();
  if (!u) return "";
  if (u.startsWith("redis://")) return "rediss://" + u.slice("redis://".length);
  return u;
}

const redisUrl = normalizeRedisUrl(REDIS_URL_RAW);
const redis = redisUrl
  ? new Redis(redisUrl, {
      tls: redisUrl.startsWith("rediss://") ? { rejectUnauthorized: false } : undefined,
      maxRetriesPerRequest: 2,
      enableReadyCheck: true,
    })
  : null;

const sessions = new Map();

function defaultSession() {
  return {
    messages: [],
    state: "idle",
    lastSlots: [],
    lastDisplaySlots: [],
    selectedSlot: null,
    selectedProperty: null,
    pendingCategory: null,
    pendingRange: null,
    pendingName: null,
    pendingPhone: null,
    pendingZone: null,
    pendingBudget: null,
    lastRecommendations: [],
    aiProfile: {
      operation: "",
      category: "",
      zone_interest: "",
      budget: "",
      budget_min: null,
      budget_max: null,
      bedrooms: null,
      bathrooms: null,
      purpose: "",
      timeline: "",
    },
    lastVisit: null,
    greeted: false,
    lastMsgId: null,
    humanTakeover: false,
    mediaContext: {
      lastImageText: "",
      lastAudioText: "",
    },
    reschedule: {
      active: false,
      visit_id: "",
      phone: "",
      lead_name: "",
      property_id: "",
      property_code: "",
      property_title: "",
      property_retailer_id: "",
      zone_interest: "",
      budget: "",
      category: "",
    },
  };
}

function sanitizeSession(session) {
  if (!session || typeof session !== "object") return defaultSession();
  if (!Array.isArray(session.messages)) session.messages = [];
  session.messages = session.messages.slice(-20);
  if (!Array.isArray(session.lastSlots)) session.lastSlots = [];
  session.lastSlots = session.lastSlots.slice(0, MAX_SLOTS_RETURN);
  if (!Array.isArray(session.lastDisplaySlots)) session.lastDisplaySlots = [];
  session.lastDisplaySlots = session.lastDisplaySlots.slice(0, HOURLY_LIST_COUNT);
  if (!Array.isArray(session.lastRecommendations)) session.lastRecommendations = [];
  session.lastRecommendations = session.lastRecommendations.slice(0, Math.max(1, AI_PROPERTY_RECOMMENDATION_LIMIT));
  if (!session.aiProfile || typeof session.aiProfile !== "object") {
    session.aiProfile = defaultSession().aiProfile;
  } else {
    session.aiProfile = {
      ...defaultSession().aiProfile,
      ...session.aiProfile,
      budget_min: Number.isFinite(Number(session.aiProfile?.budget_min)) ? Number(session.aiProfile.budget_min) : null,
      budget_min: Number.isFinite(Number(session.aiProfile?.budget_min)) ? Number(session.aiProfile.budget_min) : null,
      budget_max: Number.isFinite(Number(session.aiProfile?.budget_max)) ? Number(session.aiProfile.budget_max) : null,
      bedrooms: Number.isFinite(Number(session.aiProfile?.bedrooms)) ? Number(session.aiProfile.bedrooms) : null,
      bathrooms: Number.isFinite(Number(session.aiProfile?.bathrooms)) ? Number(session.aiProfile.bathrooms) : null,
    };
  }
  if (!session.reschedule || typeof session.reschedule !== "object") {
    session.reschedule = defaultSession().reschedule;
  }
  if (!session.mediaContext || typeof session.mediaContext !== "object") {
    session.mediaContext = defaultSession().mediaContext;
  }
  if (typeof session.state !== "string") session.state = "idle";
  if (typeof session.greeted !== "boolean") session.greeted = false;
  if (typeof session.humanTakeover !== "boolean") session.humanTakeover = false;
  return session;
}

async function getSession(userId) {
  if (!userId) return sanitizeSession(defaultSession());
  if (!redis) {
    if (!sessions.has(userId)) sessions.set(userId, defaultSession());
    return sanitizeSession(sessions.get(userId));
  }
  const key = `${SESSION_PREFIX}${userId}`;
  const raw = await redis.get(key);
  const s = raw ? safeJson(raw, defaultSession()) : defaultSession();
  return sanitizeSession(s);
}

async function saveSession(userId, session) {
  if (!userId || !session) return;
  const clean = sanitizeSession(session);
  if (!redis) {
    sessions.set(userId, clean);
    return;
  }
  const key = `${SESSION_PREFIX}${userId}`;
  await redis.set(key, JSON.stringify(clean), "EX", SESSION_TTL_SEC);
}

function bothubHmacStable(payload, secret) {
  const raw = stableStringify(payload);
  return crypto.createHmac("sha256", secret).update(raw).digest("hex");
}

function bothubHmacJson(payload, secret) {
  return crypto.createHmac("sha256", secret).update(JSON.stringify(payload)).digest("hex");
}

function getHubSignature(req) {
  const h =
    req.get("X-HUB-SIGNATURE") ||
    req.get("x-hub-signature") ||
    req.get("X-Hub-Signature") ||
    req.get("X-HUB-SIGNATURE-256") ||
    req.get("X-Hub-Signature-256") ||
    req.get("x-hub-signature-256") ||
    "";
  const sig = String(h || "").trim();
  if (!sig) return "";
  return sig.startsWith("sha256=") ? sig.slice("sha256=".length) : sig;
}

function verifyHubSignature(reqBody, signatureHex, secret) {
  if (!signatureHex || !secret) return false;
  const expectedStable = bothubHmacStable(reqBody, secret);
  if (timingSafeEqualHex(signatureHex, expectedStable)) return true;
  const expectedJson = bothubHmacJson(reqBody, secret);
  if (timingSafeEqualHex(signatureHex, expectedJson)) return true;
  return false;
}

async function bothubReportMessage(payload) {
  if (!BOTHUB_WEBHOOK_URL || !BOTHUB_WEBHOOK_SECRET) return;
  try {
    const cleanPayload = removeUndefinedDeep(payload);
    const raw = stableStringify(cleanPayload);
    const sig = crypto.createHmac("sha256", BOTHUB_WEBHOOK_SECRET).update(raw).digest("hex");

    await axios.post(BOTHUB_WEBHOOK_URL, raw, {
      headers: {
        "Content-Type": "application/json",
        "X-HUB-SIGNATURE": sig,
      },
      timeout: BOTHUB_TIMEOUT_MS,
      transformRequest: [(data) => data],
    });
  } catch (e) {
    console.error("Bothub report failed:", e?.response?.data || e?.message || e);
  }
}

function getRequestBaseUrl(req) {
  const proto = String(req.headers["x-forwarded-proto"] || req.protocol || "https")
    .split(",")[0]
    .trim();
  const host = String(req.headers["x-forwarded-host"] || req.get("host") || "")
    .split(",")[0]
    .trim();
  if (!host) return BOT_PUBLIC_BASE_URL || "";
  return `${proto}://${host}`;
}

function getBotPublicBaseUrl(req) {
  return BOT_PUBLIC_BASE_URL || getRequestBaseUrl(req);
}

function signHubMediaToken(mediaId, ts) {
  if (!HUB_MEDIA_SECRET) return "";
  return crypto.createHmac("sha256", HUB_MEDIA_SECRET).update(`${String(mediaId)}:${String(ts)}`).digest("hex");
}

function verifyHubMediaToken(mediaId, ts, sig) {
  if (!HUB_MEDIA_SECRET || !mediaId || !ts || !sig) return false;
  const tsNum = Number(ts);
  if (!Number.isFinite(tsNum)) return false;
  const ageMs = Math.abs(Date.now() - tsNum);
  if (ageMs > HUB_MEDIA_TTL_SEC * 1000) return false;
  const expected = signHubMediaToken(mediaId, ts);
  return timingSafeEqualHex(sig, expected);
}

function buildHubMediaUrl(req, mediaId) {
  if (!mediaId || !HUB_MEDIA_SECRET) return "";
  const base = getBotPublicBaseUrl(req);
  if (!base) return "";
  const ts = String(Date.now());
  const sig = signHubMediaToken(mediaId, ts);
  return `${base.replace(/\/$/, "")}/hub_media/${encodeURIComponent(mediaId)}?ts=${encodeURIComponent(ts)}&sig=${encodeURIComponent(sig)}`;
}

function attachHubMediaUrl(req, meta) {
  const out = { ...(meta || {}) };
  const kind = String(out?.kind || "").toUpperCase();
  if (out?.mediaId && ["AUDIO", "IMAGE", "VIDEO", "DOCUMENT", "STICKER"].includes(kind)) {
    const mediaUrl = buildHubMediaUrl(req, out.mediaId);
    if (mediaUrl) out.mediaUrl = mediaUrl;
  }
  return out;
}

async function getMetaMediaInfo(mediaId) {
  if (!WA_TOKEN) throw new Error("WA_TOKEN not configured");
  const res = await axios.get(`https://graph.facebook.com/${META_GRAPH_VERSION}/${encodeURIComponent(mediaId)}`, {
    headers: { Authorization: `Bearer ${WA_TOKEN}` },
    timeout: 30000,
    validateStatus: () => true,
  });
  if (res.status < 200 || res.status >= 300) {
    throw new Error(
      res?.data?.error?.message || res?.data?.error?.error_user_msg || `Meta media lookup failed (${res.status})`
    );
  }
  return res.data || {};
}

async function downloadMetaMedia(mediaId) {
  const info = await getMetaMediaInfo(mediaId);
  const mediaUrl = info?.url;
  const mimeType = info?.mime_type || "application/octet-stream";
  if (!mediaUrl) throw new Error("Meta respondió sin url para ese mediaId");

  const bin = await axios.get(mediaUrl, {
    headers: { Authorization: `Bearer ${WA_TOKEN}` },
    responseType: "arraybuffer",
    timeout: 60000,
    validateStatus: () => true,
  });

  if (bin.status < 200 || bin.status >= 300) {
    throw new Error(typeof bin.data === "string" ? bin.data : `Meta media download failed (${bin.status})`);
  }

  return {
    buffer: Buffer.from(bin.data),
    mimeType,
    meta: info,
  };
}

async function transcribeAudioBuffer(buffer, mimeType = "audio/ogg") {
  if (!OPENAI_API_KEY || !MEDIA_AI_ENABLED || !buffer?.length) return "";
  if (buffer.length > MEDIA_AUDIO_MAX_BYTES) return "";

  try {
    const form = new FormData();
    const ext = extFromMimeType(mimeType) || ".ogg";
    form.append("model", OPENAI_AUDIO_MODEL);
    form.append("language", "es");
    form.append("file", new Blob([buffer], { type: mimeType || "audio/ogg" }), `nota_voz${ext}`);

    const resp = await fetch("https://api.openai.com/v1/audio/transcriptions", {
      method: "POST",
      headers: {
        Authorization: `Bearer ${OPENAI_API_KEY}`,
      },
      body: form,
    });

    if (!resp.ok) {
      const errText = await resp.text();
      throw new Error(errText || `OpenAI audio transcription failed (${resp.status})`);
    }

    const data = await resp.json();
    return cleanText(data?.text || "");
  } catch (e) {
    console.error("transcribeAudioBuffer error:", e?.message || e);
    return "";
  }
}

async function understandPropertyImageBuffer(buffer, mimeType = "image/jpeg", caption = "") {
  const fallback = {
    is_property_related: false,
    normalized_user_text: cleanText(caption),
    extracted_text: "",
    wants_visit: false,
  };

  if (!OPENAI_API_KEY || !MEDIA_AI_ENABLED || !buffer?.length) return fallback;
  if (buffer.length > MEDIA_IMAGE_MAX_BYTES) return fallback;

  try {
    const dataUrl = bufferToDataUrl(buffer, mimeType || "image/jpeg");

    const resp = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model: OPENAI_VISION_MODEL,
        temperature: 0.1,
        response_format: { type: "json_object" },
        messages: [
          {
            role: "system",
            content:
              `Analiza una imagen recibida por WhatsApp. ` +
              `Si es un flyer, captura o anuncio de una propiedad inmobiliaria, extrae la información visible y conviértela en un mensaje corto en español que un bot inmobiliario pueda entender para identificar la propiedad y continuar el flujo. ` +
              `No inventes datos. Si hay caption del usuario, intégralo. ` +
              `Si no hay una pregunta explícita pero sí una propiedad identificable, redacta normalized_user_text como interés natural del usuario, por ejemplo: "Me interesa la propiedad en X, precio Y, quiero más información". ` +
              `Devuelve SOLO JSON con estas claves exactas: is_property_related, normalized_user_text, extracted_text, wants_visit.`,
          },
          {
            role: "user",
            content: [
              {
                type: "text",
                text:
                  `Caption del usuario: ${cleanText(caption) || "(sin caption)"}\n` +
                  `Extrae el texto visible y genera normalized_user_text en español.`,
              },
              {
                type: "image_url",
                image_url: { url: dataUrl },
              },
            ],
          },
        ],
      },
      {
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
        timeout: 90000,
      }
    );

    const raw = resp.data?.choices?.[0]?.message?.content?.trim() || "{}";
    const parsed = safeJson(raw, {});
    const normalized_user_text = cleanText(parsed?.normalized_user_text || "");
    const extracted_text = cleanText(parsed?.extracted_text || "");

    return {
      is_property_related: parsed?.is_property_related === true,
      normalized_user_text: normalized_user_text || cleanText(caption) || extracted_text,
      extracted_text,
      wants_visit: parsed?.wants_visit === true,
    };
  } catch (e) {
    console.error("understandPropertyImageBuffer error:", e?.response?.data || e?.message || e);
    return fallback;
  }
}

function extractInboundMeta(msg) {
  if (!msg) return {};
  if (msg?.type === "audio") {
    return {
      kind: "AUDIO",
      mediaId: msg?.audio?.id,
      mimeType: msg?.audio?.mime_type,
      voice: msg?.audio?.voice,
    };
  }
  if (msg?.type === "location") {
    return {
      kind: "LOCATION",
      latitude: msg?.location?.latitude,
      longitude: msg?.location?.longitude,
      name: msg?.location?.name,
      address: msg?.location?.address,
    };
  }
  if (msg?.type === "image") {
    return { kind: "IMAGE", mediaId: msg?.image?.id, mimeType: msg?.image?.mime_type, caption: msg?.image?.caption };
  }
  if (msg?.type === "video") {
    return { kind: "VIDEO", mediaId: msg?.video?.id, mimeType: msg?.video?.mime_type, caption: msg?.video?.caption };
  }
  if (msg?.type === "document") {
    return { kind: "DOCUMENT", mediaId: msg?.document?.id, mimeType: msg?.document?.mime_type, filename: msg?.document?.filename };
  }
  if (msg?.type === "sticker") {
    return { kind: "STICKER", mediaId: msg?.sticker?.id, mimeType: msg?.sticker?.mime_type };
  }
  if (msg?.type === "contacts") {
    return {
      kind: "CONTACTS",
      count: msg?.contacts?.length || 0,
      contacts: extractSharedContactsDetails(msg?.contacts || []),
    };
  }
  if (msg?.type === "reaction") {
    return { kind: "REACTION", emoji: msg?.reaction?.emoji, messageId: msg?.reaction?.message_id };
  }
  if (msg?.type === "order") {
    return {
      kind: "ORDER",
      catalogId: msg?.order?.catalog_id,
      productCount: msg?.order?.product_items?.length || 0,
      productRetailerIds: (msg?.order?.product_items || []).map((p) => p.product_retailer_id).filter(Boolean),
    };
  }
  return { kind: msg?.type ? String(msg.type).toUpperCase() : "UNKNOWN" };
}

const app = express();
app.use(
  express.json({
    verify: (req, _res, buf) => {
      req.rawBody = buf;
    },
  })
);

const catalogAdmin = createCatalogAdmin({
  basePath: "/admin",
  businessName: BUSINESS_NAME,
  adminUsername: process.env.ADMIN_PANEL_USERNAME || "admin",
  adminPassword: process.env.ADMIN_PANEL_PASSWORD || "admin123456",
  getCatalog: () => PROPERTY_CATALOG.map((p) => ({ ...p, product_retailer_id: p.retailer_id })),
  setCatalog: async (nextCatalog) => {
    refreshPropertyCatalog(nextCatalog || []);
  },
});

app.use("/admin", catalogAdmin.router);

function verifyMetaSignature(req) {
  if (!META_APP_SECRET) return true;
  const signature = req.get("X-Hub-Signature-256");
  if (!signature) return false;

  const expected =
    "sha256=" + crypto.createHmac("sha256", META_APP_SECRET).update(req.rawBody || Buffer.from("")).digest("hex");

  try {
    return crypto.timingSafeEqual(Buffer.from(signature), Buffer.from(expected));
  } catch {
    return false;
  }
}

function getCalendarClient() {
  const json = safeJson(process.env.GOOGLE_SERVICE_ACCOUNT_JSON, null);
  if (!json?.client_email || !json?.private_key) throw new Error("Missing GOOGLE_SERVICE_ACCOUNT_JSON");
  const auth = new google.auth.JWT({
    email: json.client_email,
    key: json.private_key,
    scopes: ["https://www.googleapis.com/auth/calendar"],
  });
  return google.calendar({ version: "v3", auth });
}

function categoryTitle(categoryKey) {
  if (PROPERTY_CATEGORIES_BY_KEY[categoryKey]?.title) return PROPERTY_CATEGORIES_BY_KEY[categoryKey].title;
  if (PROPERTY_CATEGORIES_BY_ID[categoryKey]?.title) return PROPERTY_CATEGORIES_BY_ID[categoryKey].title;
  return categoryKey || "propiedades";
}

function propertyOperationLabel(operation) {
  const op = normalizeText(operation || "");
  if (!op) return "";
  if (op.includes("alquiler") || op.includes("renta") || op.includes("rent")) return "Alquiler";
  if (op.includes("venta") || op.includes("sale")) return "Venta";
  if (op.includes("proyecto")) return "Proyecto";
  return String(operation || "").trim();
}

function getLeadTagsForProperty(property) {
  const tags = ["lead_inmobiliario"];
  const op = normalizeText(property?.operation || "");
  const category = normalizeText(property?.category || "");

  if (op.includes("venta") || category === "venta") tags.push("venta");
  if (op.includes("alquiler") || op.includes("renta") || category === "alquiler") tags.push("alquiler");
  if (category === "solares") tags.push("solar");
  if (category === "proyectos") tags.push("proyecto");
  if (category === "locales_comerciales") tags.push("local_comercial");
  if (category === "casas") tags.push("casa");
  if (category === "apartamentos") tags.push("apartamento");

  return [...new Set(tags.filter(Boolean))];
}

function getPrimaryLeadTag(property) {
  const tags = getLeadTagsForProperty(property).filter((t) => t !== "lead_inmobiliario");
  return tags[0] || "lead_inmobiliario";
}

function formatMoney(rawValue, currency = "") {
  if (rawValue === null || rawValue === undefined || rawValue === "") return "";
  if (typeof rawValue === "number") return `${currency ? currency + " " : ""}${rawValue.toLocaleString("es-DO")}`.trim();
  return `${currency ? currency + " " : ""}${String(rawValue)}`.trim();
}

function propertyLabel(property) {
  if (!property) return "propiedad";
  return property.code ? `${property.title} (${property.code})` : property.title;
}

function propertyPublicLabel(property) {
  if (!property) return "propiedad";
  return property.title || property.code || "propiedad";
}

function propertySummary(property) {
  if (!property) return "";
  const price = formatMoney(property.price, property.currency);
  const operation = propertyOperationLabel(property.operation);
  const parts = [
    `🏠 *${propertyPublicLabel(property)}*`,
    operation ? `🏷️ ${operation}` : "",
    property.category ? `📂 ${categoryTitle(property.category)}` : "",
    property.location ? `📍 ${property.location}` : "",
    price ? `💰 ${price}` : "",
    property.bedrooms !== "" ? `🛏️ ${property.bedrooms} hab.` : "",
    property.bathrooms !== "" ? `🛁 ${property.bathrooms} baños` : "",
    property.area_m2 !== "" ? `📐 ${property.area_m2} m²` : "",
    property.short_description ? `📝 ${property.short_description}` : "",
  ].filter(Boolean);
  return parts.join("\n");
}

function propertyFaqSnapshot(property) {
  return {
    title: property?.title || "",
    code: property?.code || "",
    category: property?.category || "",
    operation: property?.operation || "",
    price: property?.price || "",
    currency: property?.currency || "",
    location: property?.location || "",
    exact_address: property?.exact_address || "",
    exact_location_reference: property?.exact_location_reference || "",
    bedrooms: property?.bedrooms ?? "",
    bathrooms: property?.bathrooms ?? "",
    parking: property?.parking ?? "",
    floor_level: property?.floor_level || "",
    area_m2: property?.area_m2 ?? "",
    lot_m2: property?.lot_m2 ?? "",
    construction_m2: property?.construction_m2 ?? "",
    short_description: property?.short_description || "",
    features: Array.isArray(property?.features) ? property.features : [],
    year_built: property?.year_built || "",
    condition: property?.condition || "",
    title_deed: property?.title_deed ?? "",
    has_mortgage: property?.has_mortgage ?? "",
    legal_status: property?.legal_status || "",
    documents_up_to_date: property?.documents_up_to_date ?? "",
    bank_financing: property?.bank_financing ?? "",
    bank_financing_note: property?.bank_financing_note || "",
    down_payment: property?.down_payment || "",
    payment_facilities: property?.payment_facilities || "",
    estimated_monthly_fee: property?.estimated_monthly_fee || "",
    transfer_cost: property?.transfer_cost || "",
    sewer: property?.sewer ?? "",
    paved_street: property?.paved_street ?? "",
    water_service: property?.water_service ?? "",
    electric_service: property?.electric_service ?? "",
    nearby_places: Array.isArray(property?.nearby_places) ? property.nearby_places : [],
    safety: property?.safety || "",
    transport_access: property?.transport_access || "",
    purchase_steps: property?.purchase_steps || "",
    purchase_timeline: property?.purchase_timeline || "",
    faq: property?.faq || {},
  };
}

function propertyFeatureListText(property, limit = 12) {
  const out = [];

  if (Array.isArray(property?.features)) out.push(...property.features);
  if (property?.parking !== "" && property?.parking !== null && property?.parking !== undefined) {
    out.push(`${property.parking} parqueo(s)`);
  }

  const water = toBoolOrNull(property?.water_service);
  if (water === true) out.push("servicio de agua");

  const electric = toBoolOrNull(property?.electric_service);
  if (electric === true) out.push("servicio eléctrico");

  const paved = toBoolOrNull(property?.paved_street);
  if (paved === true) out.push("calle asfaltada");

  const sewer = toBoolOrNull(property?.sewer);
  if (sewer === true) out.push("cloaca");

  return [...new Set(out.filter(Boolean))].slice(0, limit);
}

function propertySearchText(property) {
  return normalizeText(
    [
      property?.title || "",
      property?.code || "",
      property?.category || "",
      property?.operation || "",
      property?.location || "",
      property?.exact_address || "",
      property?.exact_location_reference || "",
      property?.short_description || "",
      property?.condition || "",
      property?.legal_status || "",
      property?.bank_financing_note || "",
      property?.payment_facilities || "",
      property?.estimated_monthly_fee || "",
      property?.purchase_steps || "",
      property?.purchase_timeline || "",
      property?.transport_access || "",
      property?.safety || "",
      property?.floor_level || "",
      ...(Array.isArray(property?.features) ? property.features : []),
      ...(Array.isArray(property?.nearby_places) ? property.nearby_places : []),
    ]
      .filter(Boolean)
      .join(" | ")
  );
}

function propertyFindRelevantPiece(property, keywords = []) {
  const norms = (keywords || []).map((k) => normalizeText(k)).filter(Boolean);
  const pieces = [
    ...(Array.isArray(property?.features) ? property.features : []),
    ...String(property?.short_description || "")
      .split(/[.\n]+/g)
      .map((s) => s.trim())
      .filter(Boolean),
    property?.condition || "",
    property?.legal_status || "",
    property?.bank_financing_note || "",
    property?.payment_facilities || "",
    property?.estimated_monthly_fee || "",
    property?.purchase_steps || "",
    property?.purchase_timeline || "",
    property?.transport_access || "",
    property?.safety || "",
    property?.floor_level || "",
    ...(Array.isArray(property?.nearby_places) ? property.nearby_places : []),
  ].filter(Boolean);

  for (const piece of pieces) {
    const n = normalizeText(piece);
    if (norms.some((k) => n.includes(k))) return piece;
  }

  return "";
}

function scorePropertyAgainstText(property, text) {
  const raw = String(text || "").trim();
  const t = normalizeText(raw);
  if (!t) return 0;

  let score = 0;

  const importantFields = [
    { value: property?.code, points: 14, min: 2 },
    { value: property?.title, points: 11, min: 4 },
    { value: property?.exact_address, points: 10, min: 5 },
    { value: property?.location, points: 8, min: 4 },
    { value: property?.exact_location_reference, points: 5, min: 4 },
  ];

  for (const field of importantFields) {
    const n = normalizeText(field.value);
    if (n && n.length >= field.min && t.includes(n)) {
      score += field.points;
    }
  }

  const titleTokens = normalizeText(property?.title || "")
    .split(" ")
    .filter((x) => x.length >= 4);
  let titleTokenHits = 0;
  for (const tk of titleTokens) {
    if (t.includes(tk)) titleTokenHits++;
  }
  score += Math.min(4, titleTokenHits);

  const textDigits = digitsOnly(raw);
  const priceDigits = digitsOnly(property?.price);
  if (priceDigits && priceDigits.length >= 5 && textDigits.includes(priceDigits)) {
    score += 4;
  }

  const m2Variants = [property?.area_m2, property?.lot_m2, property?.construction_m2]
    .map((v) => digitsOnly(v))
    .filter(Boolean);

  for (const m of new Set(m2Variants)) {
    if (m.length >= 2 && textDigits.includes(m)) {
      score += 2;
      break;
    }
  }

  const beds = Number(property?.bedrooms);
  if (Number.isFinite(beds) && beds > 0) {
    const bedRx = new RegExp(`\\b${beds}\\b\\s*(hab|habitacion|habitaciones|cuartos?)`);
    if (bedRx.test(t)) score += 2;
  }

  const baths = Number(property?.bathrooms);
  if (Number.isFinite(baths) && baths > 0) {
    const bathRx = new RegExp(`\\b${baths}\\b\\s*(bano|banos|baño|baños)`);
    if (bathRx.test(t)) score += 2;
  }

  const parking = Number(property?.parking);
  if (Number.isFinite(parking) && parking > 0) {
    const parkRx = new RegExp(`\\b${parking}\\b\\s*(parqueo|parqueos|parking|marquesina)`);
    if (parkRx.test(t)) score += 1;
  }

  const features = propertyFeatureListText(property, 12).map((v) => normalizeText(v)).filter((v) => v.length >= 4);
  let featureHits = 0;
  for (const ft of features) {
    if (t.includes(ft)) featureHits++;
    if (featureHits >= 3) break;
  }
  score += Math.min(3, featureHits);

  return score;
}

function findPropertyFromRichText(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  const ranked = PROPERTY_CATALOG.map((p) => ({
    property: p,
    score: scorePropertyAgainstText(p, raw),
  }))
    .filter((x) => x.score > 0)
    .sort((a, b) => b.score - a.score);

  if (!ranked.length) return null;

  const best = ranked[0];
  const second = ranked[1];

  if (best.score >= 7) return best.property;
  if (best.score >= 5 && (!second || best.score >= second.score + 2)) return best.property;

  return null;
}

function looksLikeGeneralQuestion(text) {
  const raw = String(text || "").trim();
  const t = normalizeText(raw);
  if (!t) return false;
  if (raw.includes("?")) return true;

  return [
    "tiene ",
    "tienen ",
    "hay ",
    "incluye ",
    "incluyen ",
    "cuenta con ",
    "se puede ",
    "cuanto ",
    "cuánto ",
    "como ",
    "cómo ",
    "donde ",
    "dónde ",
    "queda ",
    "esta ",
    "está ",
    "es ",
    "son ",
    "tendra ",
    "tendrá ",
    "quiero informacion",
    "quiero información",
    "mas informacion",
    "más información",
    "detalles ",
  ].some((k) => t.startsWith(normalizeText(k)));
}

function parseNumericValue(value) {
  if (value === null || value === undefined || value === "") return null;
  if (typeof value === "number" && Number.isFinite(value)) return value;
  let s = normalizeText(String(value));
  if (!s) return null;
  const hasMillion = s.includes("millon") || s.includes("millones") || /\bmm\b/.test(s);
  const hasThousand = /\bk\b/.test(s) || s.includes("mil");
  s = s.replace(/rd\$/g, "").replace(/us\$/g, "").replace(/usd/g, "").replace(/dop/g, "").replace(/,/g, "");
  const match = s.match(/\d+(?:\.\d+)?/);
  if (!match) return null;
  let n = Number(match[0]);
  if (!Number.isFinite(n)) return null;
  if (hasMillion) n *= 1000000;
  else if (hasThousand && n < 100000) n *= 1000;
  return n;
}

function extractBudgetRange(text) {
  const raw = String(text || "").trim();
  const t = normalizeText(raw);
  if (!t) return { budget_min: null, budget_max: null, budget_label: "" };

  const rangeMatch =
    t.match(
      /(?:entre|de)\s+(?:los\s+)?(rd\$|us\$|usd)?\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?\s*(?:y|a|-)\s*(rd\$|us\$|usd)?\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?/i
    ) ||
    t.match(
      /([\d.,]+)\s*(k|mil|millon|millones|mm)?\s*-\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?/i
    );

  if (rangeMatch) {
    let minRaw = "";
    let maxRaw = "";

    if (rangeMatch.length >= 7) {
      const cur1 = rangeMatch[1] || rangeMatch[4] || "";
      const num1 = rangeMatch[2] || "";
      const suf1 = rangeMatch[3] || rangeMatch[6] || "";
      const cur2 = rangeMatch[4] || rangeMatch[1] || "";
      const num2 = rangeMatch[5] || "";
      const suf2 = rangeMatch[6] || rangeMatch[3] || "";

      minRaw = `${cur1}${num1}${suf1}`.trim();
      maxRaw = `${cur2}${num2}${suf2}`.trim();
    } else {
      const num1 = rangeMatch[1] || "";
      const suf1 = rangeMatch[2] || rangeMatch[4] || "";
      const num2 = rangeMatch[3] || "";
      const suf2 = rangeMatch[4] || rangeMatch[2] || "";
      minRaw = `${num1}${suf1}`.trim();
      maxRaw = `${num2}${suf2}`.trim();
    }

    const min = parseNumericValue(minRaw);
    const max = parseNumericValue(maxRaw);

    if (Number.isFinite(min) || Number.isFinite(max)) {
      return {
        budget_min: Number.isFinite(min) ? min : null,
        budget_max: Number.isFinite(max) ? max : null,
        budget_label: `entre ${minRaw} y ${maxRaw}`.trim(),
      };
    }
  }

  const hastaMatch = t.match(/(?:hasta|maximo|máximo|tope de)\s+(rd\$|us\$|usd)?\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?/i);
  if (hastaMatch) {
    const maxRaw = `${hastaMatch[1] || ""}${hastaMatch[2] || ""}${hastaMatch[3] || ""}`.trim();
    const max = parseNumericValue(maxRaw);
    if (Number.isFinite(max)) {
      return {
        budget_min: null,
        budget_max: max,
        budget_label: `hasta ${maxRaw}`.trim(),
      };
    }
  }

  const desdeMatch = t.match(/(?:desde|minimo|mínimo)\s+(rd\$|us\$|usd)?\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?/i);
  if (desdeMatch) {
    const minRaw = `${desdeMatch[1] || ""}${desdeMatch[2] || ""}${desdeMatch[3] || ""}`.trim();
    const min = parseNumericValue(minRaw);
    if (Number.isFinite(min)) {
      return {
        budget_min: min,
        budget_max: null,
        budget_label: `desde ${minRaw}`.trim(),
      };
    }
  }

  return { budget_min: null, budget_max: null, budget_label: "" };
}

function looksLikeBudgetSearch(text) {
  const t = normalizeText(text || "");
  if (!t) return false;

  return (
    /\bentre\b/.test(t) ||
    /\bhasta\b/.test(t) ||
    /\bdesde\b/.test(t) ||
    /\bmaximo\b/.test(t) ||
    /\bmáximo\b/.test(t) ||
    /\bminimo\b/.test(t) ||
    /\bmínimo\b/.test(t) ||
    /\bpresupuesto\b/.test(t) ||
    /\bmil\b/.test(t) ||
    /\bk\b/.test(t) ||
    /\brd\$\b/.test(t) ||
    /\bus\$\b/.test(t) ||
    /\busd\b/.test(t) ||
    /\d+\s*-\s*\d+/.test(t)
  );
}

function propertyPriceNumber(property) {
  return parseNumericValue(property?.price);
}

function summarizeCatalogForPrompt(limit = 25) {
  return PROPERTY_CATALOG.slice(0, limit)
    .map((p) => {
      const pieces = [
        `id=${p.id}`,
        `codigo=${p.code}`,
        `titulo=${p.title}`,
        p.operation ? `operacion=${p.operation}` : "",
        p.category ? `categoria=${p.category}` : "",
        p.location ? `ubicacion=${p.location}` : "",
        p.price !== "" ? `precio=${p.price}` : "",
        p.bedrooms !== "" ? `habitaciones=${p.bedrooms}` : "",
        p.bathrooms !== "" ? `banos=${p.bathrooms}` : "",
      ].filter(Boolean);
      return pieces.join(" | ");
    })
    .join("\n");
}

function mergeLeadProfile(base = {}, extra = {}) {
  const next = { ...base };
  for (const [k, v] of Object.entries(extra || {})) {
    if (v === null || v === undefined || v === "") continue;
    next[k] = v;
  }
  if (!Number.isFinite(Number(next.budget_min))) next.budget_min = null;
  else next.budget_min = Number(next.budget_min);
  if (!Number.isFinite(Number(next.budget_max))) next.budget_max = null;
  else next.budget_max = Number(next.budget_max);

  if (!Number.isFinite(Number(next.bedrooms))) next.bedrooms = null;
  else next.bedrooms = Number(next.bedrooms);

  if (!Number.isFinite(Number(next.bathrooms))) next.bathrooms = null;
  else next.bathrooms = Number(next.bathrooms);

  return next;
}

function propertyMatchesCriteria(property, criteria = {}) {
  if (!property) return false;
  const category = normalizeText(criteria.category || "");
  const operation = normalizeOperationKey(criteria.operation || criteria.intent || "");
  const zone = normalizeText(criteria.zone_interest || criteria.zone || "");
  const budgetMin = Number(criteria.budget_min);
  const budgetMax = Number(criteria.budget_max);
  const bedrooms = Number(criteria.bedrooms);
  const bathrooms = Number(criteria.bathrooms);

  if (category && normalizeText(property.category) !== category) return false;
  if (operation) {
    const propOp = normalizeOperationKey(property.operation || property.category || "");
    if (propOp && propOp !== operation) return false;
  }
  if (zone) {
    const haystack = normalizeText(`${property.location || ""} ${property.title || ""} ${property.short_description || ""}`);
    if (!haystack.includes(zone)) return false;
  }

  const price = propertyPriceNumber(property);
  if (Number.isFinite(price)) {
    if (Number.isFinite(budgetMin) && budgetMin > 0 && price < budgetMin) return false;
    if (Number.isFinite(budgetMax) && budgetMax > 0 && price > budgetMax) return false;
  }

  if (Number.isFinite(bedrooms) && bedrooms > 0) {
    const propBedrooms = Number(property?.bedrooms);
    if (Number.isFinite(propBedrooms) && propBedrooms < bedrooms) return false;
  }
  if (Number.isFinite(bathrooms) && bathrooms > 0) {
    const propBathrooms = Number(property?.bathrooms);
    if (Number.isFinite(propBathrooms) && propBathrooms < bathrooms) return false;
  }
  return true;
}

function rankPropertyMatch(property, criteria = {}) {
  let score = 0;
  const zone = normalizeText(criteria.zone_interest || criteria.zone || "");
  const operation = normalizeOperationKey(criteria.operation || "");
  const category = normalizeText(criteria.category || "");
  const budgetMin = Number(criteria.budget_min);
  const budgetMax = Number(criteria.budget_max);
  const price = propertyPriceNumber(property);

  if (zone) {
    const haystack = normalizeText(`${property.location || ""} ${property.title || ""}`);
    if (haystack.includes(zone)) score += 3;
  }

  if (operation && normalizeOperationKey(property.operation || property.category || "") === operation) score += 2;
  if (category && normalizeText(property.category || "") === category) score += 2;

  if (Number.isFinite(price)) {
    if (Number.isFinite(budgetMin) && Number.isFinite(budgetMax) && budgetMin > 0 && budgetMax > 0) {
      if (price >= budgetMin && price <= budgetMax) score += 3;
      else score -= 2;
    } else if (Number.isFinite(budgetMax) && budgetMax > 0) {
      score += price <= budgetMax ? 2 : -2;
    } else if (Number.isFinite(budgetMin) && budgetMin > 0) {
      score += price >= budgetMin ? 2 : -2;
    }
  }

  if (Number.isFinite(Number(criteria.bedrooms)) && Number(property?.bedrooms) >= Number(criteria.bedrooms)) score += 1;
  if (Number.isFinite(Number(criteria.bathrooms)) && Number(property?.bathrooms) >= Number(criteria.bathrooms)) score += 1;
  return score;
}

function findMatchingProperties(criteria = {}, limit = AI_PROPERTY_RECOMMENDATION_LIMIT) {
  const matches = PROPERTY_CATALOG.filter((p) => propertyMatchesCriteria(p, criteria)).sort((a, b) => {
    return rankPropertyMatch(b, criteria) - rankPropertyMatch(a, criteria);
  });
  return matches.slice(0, Math.max(1, limit));
}

function formatPropertyShortLine(property, index) {
  const price = formatMoney(property.price, property.currency);
  return [
    `${index + 1}. *${propertyPublicLabel(property)}*`,
    property.location ? `- ${property.location}` : "",
    price ? `- ${price}` : "",
    property.bedrooms !== "" ? `- ${property.bedrooms} hab.` : "",
  ].filter(Boolean).join(" ");
}

function buildRecommendationIntro(criteria = {}) {
  const bits = [];
  if (criteria.operation) bits.push(propertyOperationLabel(criteria.operation));
  if (criteria.category) bits.push(categoryTitle(criteria.category));
  if (criteria.zone_interest) bits.push(`en ${criteria.zone_interest}`);
  if (criteria.budget) bits.push(`con presupuesto ${criteria.budget}`);
  return bits.join(" ").trim();
}

function formatRecommendationMessage(criteria = {}, properties = []) {
  const intro = buildRecommendationIntro(criteria);
  const header = intro ? `Encontré opciones que encajan con *${intro}* ✅` : `Encontré estas opciones para ti ✅`;
  return (
    `${header}\n\n` +
    properties.map((p, i) => formatPropertyShortLine(p, i)).join("\n") +
    `\n\nRespóndeme con el *número* o con el *nombre* de la propiedad que te interese, y te ayudo a agendar la visita.`
  );
}

function tryPickRecommendedPropertyFromUserText(session, userText) {
  const t = normalizeText(userText);
  if (!Array.isArray(session?.lastRecommendations) || !session.lastRecommendations.length) return null;
  if (/^\d+$/.test(t)) {
    const idx = Number(t) - 1;
    if (idx >= 0 && idx < session.lastRecommendations.length) return session.lastRecommendations[idx] || null;
  }
  return findPropertyByAny(userText);
}

function shouldUseAdvisorSearch(textNorm) {
  const t = normalizeText(textNorm || "");
  if (!t) return false;

  return (
    [
      "busco",
      "buscando",
      "quiero",
      "necesito",
      "estoy buscando",
      "me interesa",
      "presupuesto",
      "comprar",
      "alquilar",
      "rentar",
      "invertir",
      "apartamento",
      "apto",
      "casa",
      "solar",
      "proyecto",
      "local",
      "habitacion",
      "habitaciones",
      "bano",
      "banos",
      "zona",
      "sector",
      "apartaestudio",
      "estudio",
      "unitaria",
      "unidad",
      "opciones",
      "disponible",
      "disponibles",
    ].some((k) => t.includes(k)) ||
    looksLikeBudgetSearch(t)
  );
}

function looksLikePropertyQuestion(textNorm) {
  return hasAnyKeyword(textNorm, [
    "precio",
    "cuanto cuesta",
    "cuánto cuesta",
    "valor",
    "costo",
    "informacion",
    "información",
    "mas informacion",
    "más información",
    "detalles",
    "más detalles",
    "mas detalles",

    "metros",
    "metro cuadrado",
    "metraje",
    "m2",
    "mt2",
    "area",
    "área",
    "solar",
    "construccion",
    "construcción",

    "parqueo",
    "parqueos",
    "parking",
    "estacionamiento",
    "marquesina",

    "mantenimiento",
    "incluye mantenimiento",
    "paga mantenimiento",

    "agua",
    "servicio de agua",
    "entrada de agua",
    "salida de agua",
    "cisterna",
    "tinaco",

    "luz",
    "energia",
    "energía",
    "electricidad",
    "electrica",
    "eléctrica",
    "servicio electrico",
    "servicio eléctrico",

    "habitacion",
    "habitaciones",
    "cuarto",
    "cuartos",
    "bano",
    "banos",
    "baño",
    "baños",
    "banos comunes",
    "baños comunes",
    "bano comun",
    "baño común",

    "ano construccion",
    "año construccion",
    "construida",
    "terminada",
    "reparaciones",
    "estado",
    "condicion",
    "condición",

    "cloaca",
    "asfaltada",
    "calle asfaltada",

    "nivel",
    "piso",
    "primer nivel",
    "segundo nivel",
    "tercer nivel",
    "4to nivel",
    "2do nivel",

    "balcon",
    "balcón",
    "patio",
    "terraza",
    "galeria",
    "galería",
    "jardin",
    "jardín",
    "piscina",
    "jacuzzi",

    "intercom",
    "camara",
    "cámara",
    "camaras",
    "cámaras",
    "cerco electrico",
    "cerco eléctrico",
    "porton electrico",
    "portón eléctrico",
    "seguridad 24 horas",

    "queda",
    "ubicacion",
    "ubicación",
    "ubicada",
    "direccion",
    "dirección",
    "por donde queda",
    "por dónde queda",
    "donde esta",
    "dónde está",
    "sector",

    "titulo",
    "título",
    "titulo deslindado",
    "título deslindado",
    "deslinde",

    "hipoteca",
    "carga legal",
    "gravamen",
    "legal",
    "legalidad",

    "documentos",
    "documentacion",
    "documentación",
    "papeles",
    "al dia",
    "al día",

    "financiamiento",
    "financiacion",
    "financiación",
    "banco",
    "financiar",
    "se puede financiar",

    "inicial",
    "separacion",
    "separación",
    "facilidades de pago",
    "pago con el propietario",
    "cuota",
    "mensual",
    "mensualidad",

    "traspaso",
    "costo de traspaso",

    "colegios",
    "hospitales",
    "supermercados",
    "cerca",
    "lugares cercanos",
    "zona segura",
    "segura",
    "seguridad",
    "acceso",
    "transporte",

    "cuarto de servicio",
    "area de lavado",
    "área de lavado",
    "lavado",
    "despensa",
    "desayunador",

    "pasos para comprar",
    "proceso de compra",
    "como comprar",
    "cómo comprar",
    "cuanto tiempo tarda",
    "cuánto tiempo tarda",
    "tiempo del proceso",

    "amenidades",
    "que incluye",
    "qué incluye",
    "que tiene",
    "qué tiene",
  ]);
}

function buildUnknownPropertyAnswer(property, topic) {
  return `Ahora mismo no tengo confirmado *${topic}* de *${propertyPublicLabel(
    property
  )}*. Si quieres, te paso con un asesor para validártelo.`;
}

async function answerPropertyQuestionWithAI(property, userText) {
  const heuristic = answerPropertyQuestionHeuristic(property, userText);
  if (!OPENAI_API_KEY || !REAL_ESTATE_AI_ENABLED) return heuristic;

  try {
    const resp = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model: OPENAI_MODEL,
        temperature: 0.1,
        messages: [
          {
            role: "system",
            content:
              `Eres un asistente inmobiliario. Responde SOLO con base en la ficha de la propiedad suministrada. ` +
              `No inventes datos. Si algo no está confirmado, dilo claramente y ofrece pasar con un asesor. ` +
              `Responde en español, breve, natural y útil.`,
          },
          {
            role: "user",
            content: JSON.stringify({
              pregunta: userText,
              propiedad: propertyFaqSnapshot(property),
            }),
          },
        ],
      },
      {
        headers: { Authorization: `Bearer ${OPENAI_API_KEY}` },
      }
    );

    const text = resp.data?.choices?.[0]?.message?.content?.trim();
    return text || heuristic;
  } catch (e) {
    console.error("answerPropertyQuestionWithAI error:", e?.response?.data || e?.message || e);
    return heuristic;
  }
}

function answerPropertyQuestionHeuristic(property, userText) {
  const t = normalizeText(userText);
  const name = propertyPublicLabel(property);
  const searchText = propertySearchText(property);

  if (property?.faq && typeof property.faq === "object") {
    for (const [key, value] of Object.entries(property.faq)) {
      if (cleanText(key) && cleanText(value) && t.includes(normalizeText(key))) {
        return String(value);
      }
    }
  }

  if (hasAnyKeyword(t, ["precio", "cuanto cuesta", "cuánto cuesta", "valor", "costo"])) {
    return property?.price !== ""
      ? `El precio de *${name}* es *${formatMoney(property.price, property.currency)}*.`
      : buildUnknownPropertyAnswer(property, "el precio");
  }

  if (hasAnyKeyword(t, ["informacion", "información", "mas informacion", "más información", "detalles", "más detalles", "mas detalles"])) {
    if (property?.short_description) return `Claro ✅ Sobre *${name}*: ${property.short_description}`;
    return propertySummary(property);
  }

  if (
    hasAnyKeyword(t, [
      "metros",
      "metro cuadrado",
      "metraje",
      "m2",
      "mt2",
      "area",
      "área",
      "solar",
      "construccion",
      "construcción",
    ])
  ) {
    const parts = [];
    if (property?.lot_m2 !== "") parts.push(`solar de *${property.lot_m2} m²*`);
    if (property?.construction_m2 !== "") parts.push(`construcción de *${property.construction_m2} m²*`);
    if (!parts.length && property?.area_m2 !== "") parts.push(`área de *${property.area_m2} m²*`);

    if (parts.length) return `Esta propiedad tiene ${parts.join(" y ")}.`;

    const piece = propertyFindRelevantPiece(property, ["m2", "mts2", "metro", "metros", "solar", "construccion"]);
    return piece ? `Sobre los metros de *${name}*: ${piece}` : buildUnknownPropertyAnswer(property, "los metros cuadrados");
  }

  if (hasAnyKeyword(t, ["parqueo", "parqueos", "parking", "estacionamiento", "marquesina"])) {
    if (property?.parking !== "" && property?.parking !== null && property?.parking !== undefined) {
      return `*${name}* tiene *${property.parking}* parqueo(s).`;
    }
    if (searchText.includes("parqueo") || searchText.includes("parqueos") || searchText.includes("marquesina")) {
      const piece = propertyFindRelevantPiece(property, ["parqueo", "parqueos", "marquesina"]);
      return piece
        ? `Sí, según la información registrada de *${name}*: ${piece}`
        : `Sí, en la información registrada de *${name}* se menciona parqueo.`;
    }
    return buildUnknownPropertyAnswer(property, "si tiene parqueo");
  }

  if (hasAnyKeyword(t, ["banos comunes", "baños comunes", "bano comun", "baño común"])) {
    if (searchText.includes("banos comunes") || searchText.includes("baños comunes") || searchText.includes("bano comun") || searchText.includes("baño común")) {
      const piece = propertyFindRelevantPiece(property, ["banos comunes", "baños comunes", "bano comun", "baño común"]);
      return piece
        ? `Sobre *${name}*: ${piece}`
        : `Sí, en la información registrada de *${name}* se mencionan baños comunes.`;
    }
    return buildUnknownPropertyAnswer(property, "si tiene baños comunes");
  }

  if (hasAnyKeyword(t, ["habitacion", "habitaciones", "cuarto", "cuartos", "bano", "banos", "baño", "baños"])) {
    const parts = [];
    if (property?.bedrooms !== "") parts.push(`*${property.bedrooms}* habitación(es)`);
    if (property?.bathrooms !== "") parts.push(`*${property.bathrooms}* baño(s)`);

    if (parts.length) return `*${name}* tiene ${parts.join(" y ")}.`;

    const piece = propertyFindRelevantPiece(property, ["habitacion", "habitaciones", "cuarto", "cuartos", "bano", "banos", "baño", "baños"]);
    return piece
      ? `Sobre *${name}*: ${piece}`
      : buildUnknownPropertyAnswer(property, "la cantidad de habitaciones y baños");
  }

  if (hasAnyKeyword(t, ["mantenimiento", "incluye mantenimiento", "paga mantenimiento"])) {
    if (searchText.includes("no paga mantenimiento")) {
      return `No, *${name}* no paga mantenimiento.`;
    }
    if (searchText.includes("incluye mantenimiento")) {
      return `Sí, *${name}* incluye mantenimiento.`;
    }
    if (searchText.includes("mantenimiento")) {
      const piece = propertyFindRelevantPiece(property, ["mantenimiento"]);
      return piece
        ? `Sobre el mantenimiento de *${name}*: ${piece}`
        : `En la información registrada de *${name}* sí se menciona el mantenimiento.`;
    }
    return buildUnknownPropertyAnswer(property, "el mantenimiento");
  }

  if (hasAnyKeyword(t, ["agua", "servicio de agua", "entrada de agua", "salida de agua", "cisterna", "tinaco"])) {
    const water = toBoolOrNull(property?.water_service);
    if (water === true) return `Sí, *${name}* tiene servicio de agua.`;
    if (water === false) return `No, *${name}* no tiene servicio de agua registrado.`;

    if (searchText.includes("agua") || searchText.includes("cisterna") || searchText.includes("tinaco")) {
      const piece = propertyFindRelevantPiece(property, ["agua", "cisterna", "tinaco"]);
      return piece
        ? `Sobre el agua de *${name}*: ${piece}`
        : `Sí, en la información registrada de *${name}* se menciona agua / cisterna / tinaco.`;
    }
    return buildUnknownPropertyAnswer(property, "el servicio de agua");
  }

  if (
    hasAnyKeyword(t, [
      "luz",
      "energia",
      "energía",
      "electricidad",
      "electrica",
      "eléctrica",
      "servicio electrico",
      "servicio eléctrico",
    ])
  ) {
    const electric = toBoolOrNull(property?.electric_service);
    if (electric === true) return `Sí, *${name}* tiene servicio eléctrico.`;
    if (electric === false) return `No, *${name}* no tiene servicio eléctrico registrado.`;

    if (
      searchText.includes("energia electrica") ||
      searchText.includes("energía eléctrica") ||
      searchText.includes("servicio electrico") ||
      searchText.includes("servicio eléctrico")
    ) {
      const piece = propertyFindRelevantPiece(property, ["energia electrica", "energía eléctrica", "servicio electrico", "servicio eléctrico"]);
      return piece
        ? `Sobre la energía eléctrica de *${name}*: ${piece}`
        : `Sí, en la información registrada de *${name}* se menciona energía eléctrica.`;
    }
    return buildUnknownPropertyAnswer(property, "el servicio eléctrico");
  }

  if (hasAnyKeyword(t, ["nivel", "piso", "primer nivel", "segundo nivel", "tercer nivel", "4to nivel", "2do nivel"])) {
    if (property?.floor_level) {
      return `El nivel / piso registrado de *${name}* es: *${property.floor_level}*.`;
    }
    const piece = propertyFindRelevantPiece(property, ["nivel", "piso", "primer nivel", "segundo nivel", "tercer nivel", "4to nivel", "2do nivel"]);
    return piece
      ? `Sobre el nivel de *${name}*: ${piece}`
      : buildUnknownPropertyAnswer(property, "el nivel o piso");
  }

  if (
    hasAnyKeyword(t, ["balcon", "balcón", "patio", "terraza", "galeria", "galería", "jardin", "jardín", "piscina", "jacuzzi"])
  ) {
    const piece = propertyFindRelevantPiece(property, [
      "balcon",
      "balcón",
      "patio",
      "terraza",
      "galeria",
      "galería",
      "jardin",
      "jardín",
      "piscina",
      "jacuzzi",
    ]);
    if (piece) return `Sobre las amenidades de *${name}*: ${piece}`;
    return buildUnknownPropertyAnswer(property, "si tiene balcón, patio, terraza, jardín, piscina o jacuzzi");
  }

  if (
    hasAnyKeyword(t, [
      "intercom",
      "camara",
      "cámara",
      "camaras",
      "cámaras",
      "cerco electrico",
      "cerco eléctrico",
      "porton electrico",
      "portón eléctrico",
      "seguridad 24 horas",
      "seguridad",
    ])
  ) {
    if (property?.safety) return `Sobre la seguridad de *${name}*: ${property.safety}`;
    const piece = propertyFindRelevantPiece(property, [
      "intercom",
      "camara",
      "cámara",
      "camaras",
      "cámaras",
      "cerco electrico",
      "cerco eléctrico",
      "porton electrico",
      "portón eléctrico",
      "seguridad 24 horas",
      "seguridad",
    ]);
    return piece
      ? `Sobre la seguridad de *${name}*: ${piece}`
      : buildUnknownPropertyAnswer(property, "la seguridad");
  }

  if (hasAnyKeyword(t, ["cuarto de servicio", "area de lavado", "área de lavado", "lavado", "despensa", "desayunador"])) {
    const piece = propertyFindRelevantPiece(property, ["cuarto de servicio", "area de lavado", "área de lavado", "lavado", "despensa", "desayunador"]);
    return piece
      ? `Sobre los espacios interiores de *${name}*: ${piece}`
      : buildUnknownPropertyAnswer(property, "los espacios interiores");
  }

  if (hasAnyKeyword(t, ["ano construccion", "año construccion", "construida", "cuando se construyo", "cuándo se construyó"])) {
    return property?.year_built
      ? `La propiedad tiene registrado como año de construcción: *${property.year_built}*.`
      : buildUnknownPropertyAnswer(property, "el año de construcción");
  }

  if (hasAnyKeyword(t, ["terminada", "reparaciones", "estado", "condicion", "condición"])) {
    return property?.condition
      ? `Sobre el estado de *${name}*: ${property.condition}.`
      : buildUnknownPropertyAnswer(property, "el estado actual de la propiedad");
  }

  if (hasAnyKeyword(t, ["cloaca"])) {
    const sewer = toBoolOrNull(property?.sewer);
    if (sewer === true) return `Sobre la cloaca: *Sí*.`;
    if (sewer === false) return `Sobre la cloaca: *No*.`;
    const piece = propertyFindRelevantPiece(property, ["cloaca"]);
    return piece ? `Sobre la cloaca de *${name}*: ${piece}` : buildUnknownPropertyAnswer(property, "si tiene cloaca");
  }

  if (hasAnyKeyword(t, ["asfaltada", "calle asfaltada"])) {
    const paved = toBoolOrNull(property?.paved_street);
    if (paved === true) return `Sobre la calle: *Sí, es asfaltada*.`;
    if (paved === false) return `Sobre la calle: *No se registra como asfaltada*.`;
    const piece = propertyFindRelevantPiece(property, ["asfaltada", "calle asfaltada"]);
    return piece ? `Sobre la calle de *${name}*: ${piece}` : buildUnknownPropertyAnswer(property, "si la calle es asfaltada");
  }

  if (
    hasAnyKeyword(t, [
      "queda",
      "ubicacion",
      "ubicación",
      "ubicada",
      "direccion",
      "dirección",
      "por donde queda",
      "por dónde queda",
      "donde esta",
      "dónde está",
      "sector",
    ])
  ) {
    const parts = [];
    if (property?.location) parts.push(`zona: *${property.location}*`);
    if (property?.exact_address) parts.push(`dirección: *${property.exact_address}*`);
    if (property?.exact_location_reference) parts.push(`referencia: ${property.exact_location_reference}`);

    return parts.length
      ? `La ubicación registrada de *${name}* es ${parts.join(" · ")}.`
      : buildUnknownPropertyAnswer(property, "la ubicación exacta");
  }

  if (hasAnyKeyword(t, ["titulo", "título", "titulo deslindado", "título deslindado", "deslinde"])) {
    const titleVal = toBoolOrNull(property?.title_deed);
    if (titleVal === true) return `Título / deslinde: *Sí*.`;
    if (titleVal === false) return `Título / deslinde: *No*.`;

    if (searchText.includes("titulo deslindado") || searchText.includes("título deslindado") || searchText.includes("deslindado")) {
      const piece = propertyFindRelevantPiece(property, ["titulo deslindado", "título deslindado", "deslindado"]);
      return piece ? `Sobre el título de *${name}*: ${piece}` : `Sí, en la información registrada de *${name}* se menciona título deslindado.`;
    }

    return buildUnknownPropertyAnswer(property, "el título o deslinde");
  }

  if (hasAnyKeyword(t, ["hipoteca", "carga legal", "gravamen", "legal", "legalidad"])) {
    if (property?.legal_status) {
      return `Sobre la parte legal de *${name}*: ${property.legal_status}.`;
    }
    const mort = toBoolOrNull(property?.has_mortgage);
    if (mort === true) return `Hipoteca o carga legal: *Sí*.`;
    if (mort === false) return `Hipoteca o carga legal: *No*.`;
    return buildUnknownPropertyAnswer(property, "la parte legal");
  }

  if (hasAnyKeyword(t, ["documentos", "documentacion", "documentación", "papeles", "al dia", "al día"])) {
    const docs = toBoolOrNull(property?.documents_up_to_date);
    if (docs === true) return `Documentos al día: *Sí*.`;
    if (docs === false) return `Documentos al día: *No*.`;

    if (searchText.includes("documentacion al dia") || searchText.includes("documentación al día") || searchText.includes("documentos al dia") || searchText.includes("documentos al día")) {
      const piece = propertyFindRelevantPiece(property, ["documentacion", "documentación", "documentos", "al dia", "al día"]);
      return piece ? `Sobre la documentación de *${name}*: ${piece}` : `Sí, en la información registrada de *${name}* se menciona documentación al día.`;
    }

    return buildUnknownPropertyAnswer(property, "la documentación");
  }

  if (hasAnyKeyword(t, ["financiamiento", "financiacion", "financiación", "banco", "financiar", "se puede financiar"])) {
    const financing = toBoolOrNull(property?.bank_financing);
    if (financing === true) {
      const note = property?.bank_financing_note ? ` ${property.bank_financing_note}` : "";
      return `Financiamiento bancario: *Sí*.${note}`.trim();
    }
    if (financing === false) return `Financiamiento bancario: *No*.`;

    if (searchText.includes("financiamiento")) {
      const piece = propertyFindRelevantPiece(property, ["financiamiento", "banco", "financiar"]);
      return piece
        ? `Sobre el financiamiento de *${name}*: ${piece}`
        : `Sí, en la información registrada de *${name}* se menciona financiamiento.`;
    }

    return buildUnknownPropertyAnswer(property, "el financiamiento");
  }

  if (hasAnyKeyword(t, ["inicial", "separacion", "separación"])) {
    if (property?.down_payment) {
      return `El inicial / separación registrado para *${name}* es: *${property.down_payment}*.`;
    }
    const piece = propertyFindRelevantPiece(property, ["inicial", "separacion", "separación", "separalo", "sepáralo"]);
    return piece ? `Sobre la separación de *${name}*: ${piece}` : buildUnknownPropertyAnswer(property, "el inicial");
  }

  if (hasAnyKeyword(t, ["facilidades de pago", "pago con el propietario", "facilidad de pago"])) {
    return property?.payment_facilities
      ? `Facilidades de pago: ${property.payment_facilities}`
      : buildUnknownPropertyAnswer(property, "las facilidades de pago");
  }

  if (hasAnyKeyword(t, ["cuota", "mensual", "mensualidad"])) {
    return property?.estimated_monthly_fee
      ? `La cuota aproximada registrada es: *${property.estimated_monthly_fee}*.`
      : buildUnknownPropertyAnswer(property, "la cuota aproximada mensual");
  }

  if (hasAnyKeyword(t, ["traspaso", "costo de traspaso"])) {
    return property?.transfer_cost
      ? `El costo de traspaso registrado es: *${property.transfer_cost}*.`
      : buildUnknownPropertyAnswer(property, "el costo de traspaso");
  }

  if (hasAnyKeyword(t, ["zona segura", "segura", "seguridad"])) {
    return property?.safety
      ? `Sobre la zona de *${name}*: ${property.safety}`
      : buildUnknownPropertyAnswer(property, "el nivel de seguridad de la zona");
  }

  if (hasAnyKeyword(t, ["colegios", "hospitales", "supermercados", "cerca", "lugares cercanos"])) {
    if (property?.nearby_places?.length) {
      return `Cerca de *${name}* se tiene registrado: ${property.nearby_places.join(", ")}.`;
    }
    const piece = propertyFindRelevantPiece(property, ["colegio", "hospital", "supermercado", "cerca"]);
    return piece ? `Sobre los lugares cercanos de *${name}*: ${piece}` : buildUnknownPropertyAnswer(property, "los lugares cercanos");
  }

  if (hasAnyKeyword(t, ["acceso", "transporte"])) {
    return property?.transport_access
      ? `Sobre el acceso y transporte: ${property.transport_access}`
      : buildUnknownPropertyAnswer(property, "el acceso y transporte");
  }

  if (hasAnyKeyword(t, ["pasos para comprar", "proceso de compra", "como comprar", "cómo comprar"])) {
    return property?.purchase_steps
      ? `Pasos del proceso de compra: ${property.purchase_steps}`
      : buildUnknownPropertyAnswer(property, "los pasos del proceso de compra");
  }

  if (hasAnyKeyword(t, ["cuanto tiempo tarda", "cuánto tiempo tarda", "tiempo del proceso", "duracion del proceso", "duración del proceso"])) {
    return property?.purchase_timeline
      ? `Tiempo estimado del proceso: ${property.purchase_timeline}`
      : buildUnknownPropertyAnswer(property, "el tiempo del proceso de compra");
  }

  if (hasAnyKeyword(t, ["amenidades", "que incluye", "qué incluye", "que tiene", "qué tiene"])) {
    const features = propertyFeatureListText(property);
    if (features.length) {
      return `Esta propiedad incluye: ${features.join(", ")}.`;
    }
    if (property?.short_description) {
      return `Sobre *${name}*: ${property.short_description}`;
    }
    return buildUnknownPropertyAnswer(property, "las amenidades");
  }

  return property?.short_description
    ? `Sobre *${name}*: ${property.short_description}`
    : propertySummary(property);
}

function buildSelectedPropertyNextStep(session) {
  if (!session?.selectedProperty) return `\n\nTambién puedes escribir *inicio* para volver al catálogo.`;

  if (session.state === "await_day") {
    return `\n\nSi quieres agendar la visita de *${propertyPublicLabel(
      session.selectedProperty
    )}*, dime el día.\nEj: "mañana", "viernes", "14 de junio".\n\nTambién puedes escribir *inicio* para volver al catálogo.`;
  }

  if (session.state === "await_slot_choice" && session.lastSlots?.length) {
    return `\n\nCuando quieras continuar, responde con el *número* del horario o escribe otra fecha.\nTambién puedes escribir *inicio* para volver al catálogo.`;
  }

  if (session.state === "await_name" && session.selectedSlot) {
    return `\n\nSi quieres continuar con la reserva, envíame tu *nombre completo*.\nTambién puedes escribir *inicio* para volver al catálogo.`;
  }

  if (session.state === "await_phone" && session.selectedSlot && session.pendingName) {
    return `\n\nSi quieres continuar con la reserva, envíame tu *número de teléfono*.\nTambién puedes escribir *inicio* para volver al catálogo.`;
  }

  return `\n\nTambién puedes escribir *inicio* para volver al catálogo.`;
}

async function extractLeadCriteriaWithAI(userText, session) {
  const fallback = extractLeadCriteriaHeuristic(userText, session);
  if (!REAL_ESTATE_AI_ENABLED || !OPENAI_API_KEY || !shouldUseAdvisorSearch(userText)) return fallback;

  try {
    const messages = [
      {
        role: "system",
        content:
          `Extrae intención inmobiliaria del mensaje del usuario y responde SOLO JSON válido. ` +
          `Usa estas categorías exactas: alquiler, venta, solares, proyectos, locales_comerciales, casas, apartamentos. ` +
          `Campos: intent, operation, category, zone_interest, budget, budget_min, budget_max, bedrooms, bathrooms, purpose, timeline, wants_visit, summary. ` +
          `No inventes valores. Si no está claro usa cadena vacía o null.\n\n` +
          `Catálogo resumido:\n${summarizeCatalogForPrompt(30)}`,
      },
      { role: "user", content: String(userText || "") },
    ];

    const resp = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model: OPENAI_MODEL,
        messages,
        temperature: 0.1,
        response_format: { type: "json_object" },
      },
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
    );

    const raw = resp.data.choices?.[0]?.message?.content?.trim() || "{}";
    const parsed = safeJson(raw, null);
    if (!parsed || typeof parsed !== "object") return fallback;

    return mergeLeadProfile(fallback || {}, {
      intent: parsed.intent || fallback?.intent || "other",
      operation: parsed.operation || fallback?.operation || "",
      category: parsed.category || fallback?.category || "",
      zone_interest: parsed.zone_interest || fallback?.zone_interest || "",
      budget: parsed.budget || fallback?.budget || "",
      budget_min: Number.isFinite(Number(parsed.budget_min)) ? Number(parsed.budget_min) : fallback?.budget_min ?? null,
      budget_max: Number.isFinite(Number(parsed.budget_max)) ? Number(parsed.budget_max) : fallback?.budget_max ?? null,
      bedrooms: Number.isFinite(Number(parsed.bedrooms)) ? Number(parsed.bedrooms) : fallback?.bedrooms ?? null,
      bathrooms: Number.isFinite(Number(parsed.bathrooms)) ? Number(parsed.bathrooms) : fallback?.bathrooms ?? null,
      purpose: parsed.purpose || fallback?.purpose || "",
      timeline: parsed.timeline || fallback?.timeline || "",
      wants_visit: typeof parsed.wants_visit === "boolean" ? parsed.wants_visit : fallback?.wants_visit || false,
    });
  } catch (e) {
    console.error("extractLeadCriteriaWithAI error:", e?.response?.data || e?.message || e);
    return fallback;
  }
}

function extractLeadCriteriaHeuristic(userText, session) {
  const t = normalizeText(userText || "");
  if (!t) return null;

  const budgetRange = extractBudgetRange(userText);

  const criteria = {
    operation: "",
    category: "",
    zone_interest: "",
    budget: "",
    budget_min: budgetRange.budget_min,
    budget_max: budgetRange.budget_max,
    bedrooms: null,
    bathrooms: null,
    purpose: "",
    timeline: "",
    wants_visit: looksLikeNewVisit(t) || t.includes("visitar") || t.includes("agenda"),
    intent: shouldUseAdvisorSearch(t) ? "search" : "other",
  };

  const category = detectCategoryKeyFromUser(t);
  if (category) criteria.category = category;

  if (t.includes("alquiler") || t.includes("renta")) criteria.operation = "alquiler";
  if (t.includes("venta") || t.includes("comprar")) criteria.operation = "venta";

  const bedMatch = t.match(/(\d+)\s*(hab|habitacion|habitaciones|cuartos?)/);
  if (bedMatch) criteria.bedrooms = Number(bedMatch[1]);

  const bathMatch = t.match(/(\d+)\s*(bano|banos|bañ[oa]s?)/);
  if (bathMatch) criteria.bathrooms = Number(bathMatch[1]);

  if (budgetRange.budget_label) {
    criteria.budget = budgetRange.budget_label;
  } else {
    const budgetMatch = t.match(/(rd\$|us\$|usd)?\s*([\d.,]+)\s*(k|mil|millon|millones|mm)?/i);
    if (t.includes("presupuesto") || t.includes("maximo") || t.includes("máximo") || t.includes("hasta") || budgetMatch) {
      const rawBudget = budgetMatch ? `${budgetMatch[1] || ""}${budgetMatch[2] || ""}${budgetMatch[3] || ""}`.trim() : "";
      const parsedBudget = parseNumericValue(rawBudget);
      if (rawBudget) criteria.budget = rawBudget;
      if (Number.isFinite(parsedBudget)) criteria.budget_max = parsedBudget;
    }
  }

  const zonePatterns = [
    /(?:en|por|de|zona|sector)\s+([a-z0-9áéíóúñ\- ]{3,40})$/i,
    /(?:en|por|de|zona|sector)\s+([a-z0-9áéíóúñ\- ]{3,40})(?:\s+con|\s+y|\s+de\s+\d|\s+para|\s+maximo|\s+máximo|\s+hasta|\s+entre)/i,
  ];
  for (const rx of zonePatterns) {
    const m = String(userText || "").match(rx);
    if (m?.[1]) {
      criteria.zone_interest = m[1].trim();
      break;
    }
  }

  if (!criteria.zone_interest && session?.aiProfile?.zone_interest) criteria.zone_interest = session.aiProfile.zone_interest;
  if (!criteria.budget && session?.aiProfile?.budget) criteria.budget = session.aiProfile.budget;
  if (criteria.budget_min == null && session?.aiProfile?.budget_min != null) criteria.budget_min = session.aiProfile.budget_min;
  if (criteria.budget_max == null && session?.aiProfile?.budget_max != null) criteria.budget_max = session.aiProfile.budget_max;

  return criteria;
}

async function maybeHandleAdvisorSearch({ session, userText }) {
  const criteria = await extractLeadCriteriaWithAI(userText, session);
  if (!criteria) return { handled: false };

  const mergedProfile = mergeLeadProfile(session.aiProfile || {}, criteria);
  session.aiProfile = mergedProfile;

  const shouldSearch = criteria.intent === "search" || shouldUseAdvisorSearch(userText);
  if (!shouldSearch) return { handled: false, criteria };

  const matches = findMatchingProperties(mergedProfile, AI_PROPERTY_RECOMMENDATION_LIMIT);
  if (!matches.length) {
    return {
      handled: true,
      message:
        `No encontré una propiedad exacta con esos filtros 🙏\n\n` +
        `Puedo ayudarte mejor si me dices:\n` +
        `• zona\n• presupuesto\n• si buscas comprar o alquilar\n• tipo de propiedad\n\n` +
        `Ej: "Busco apartamento en Naco para alquiler con presupuesto de US$1,500"`,
    };
  }

  session.lastRecommendations = matches;
  session.state = "await_property_choice";

  if (matches.length === 1 && criteria.wants_visit) {
    const property = matches[0];
    session.selectedProperty = property;
    session.pendingCategory = property.category || null;
    session.lastRecommendations = [];
    session.state = "await_day";
    return {
      handled: true,
      property,
      autoSelected: true,
      message: `${propertySummary(property)}\n\nEntendí lo que buscas y esta opción parece encajar muy bien ✅\n¿Cuándo te gustaría visitarla?\nEj: "mañana", "viernes", "14 de junio".`,
    };
  }

  return {
    handled: true,
    criteria: mergedProfile,
    recommendations: matches,
    message: formatRecommendationMessage(mergedProfile, matches),
  };
}

function welcomeText() {
  return (
    `👋 Hola, soy el asistente de *${BUSINESS_NAME}*.\n\n` +
    `Te ayudo a ver propiedades disponibles y agendar una visita.\n\n` +
    `👇 Toca el botón de abajo para explorar el catálogo por categorías.`
  );
}

function quickHelpText() {
  return welcomeText();
}

function isGreeting(textNorm) {
  const t = textNorm || "";
  const greetings = ["hola", "buen dia", "buenos dias", "buenas", "buenas tardes", "buenas noches", "saludos", "hey", "hi"];
  return greetings.some((g) => t === g || t.startsWith(g + " ")) || /^(hola+|buenas+)\b/.test(t);
}

function isThanks(textNorm) {
  return ["gracias", "ok", "okay", "listo", "perfecto", "dale", "bien", "genial"].some(
    (k) => textNorm === k || textNorm.includes(k)
  );
}

function isChoice(textNorm, n) {
  const t = (textNorm || "").trim();
  return t === String(n) || t === `${n}.` || t.startsWith(`${n} `);
}

function looksLikeConfirm(textNorm) {
  return ["confirmar", "confirmo", "confirmada", "confirmado", "confirmacion", "confirmación"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeCancel(textNorm) {
  return ["cancelar", "cancela", "anular", "anula", "no puedo", "ya no", "cancelacion", "cancelación"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeReschedule(textNorm) {
  return ["reprogramar", "reprograma", "cambiar", "cambio", "mover", "posponer", "otro horario"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeNewVisit(textNorm) {
  return ["nueva visita", "otra visita", "agendar", "reservar", "visita nueva", "quiero visita"].some((k) =>
    (textNorm || "").includes(k)
  );
}

function looksLikeHuman(textNorm) {
  return ["asesor", "humano", "agente", "persona", "ejecutivo"].some((k) => (textNorm || "").includes(k));
}

function looksLikeCatalogRequest(textNorm) {
  return ["catalogo", "catálogo", "propiedades", "ver propiedades", "menu", "menú"].some((k) =>
    (textNorm || "").includes(normalizeText(k))
  );
}

function detectCategoryKeyFromUser(text) {
  const t = normalizeText(text);
  if (!t) return null;
  if (PROPERTY_CATEGORIES_BY_ID[text]) return PROPERTY_CATEGORIES_BY_ID[text].key;

  const byNormalizedId = PROPERTY_CATEGORIES.find((c) => normalizeText(c.id) === t);
  if (byNormalizedId) return byNormalizedId.key;

  for (const c of PROPERTY_CATEGORIES) {
    if (t === normalizeText(c.title)) return c.key;
    if (t.includes(normalizeText(c.title))) return c.key;
  }

  if (t.includes("alquiler")) return "alquiler";
  if (t.includes("venta")) return "venta";
  if (t.includes("solar")) return "solares";
  if (t.includes("proyecto")) return "proyectos";
  if (t.includes("local")) return "locales_comerciales";
  if (t.includes("casa")) return "casas";
  if (t.includes("apartamento") || t.includes("apto") || t.includes("apartaestudio") || t.includes("estudio")) return "apartamentos";

  return null;
}

function findPropertyByAny(value) {
  const raw = String(value || "").trim();
  if (!raw) return null;

  if (PROPERTY_BY_ID[raw]) return PROPERTY_BY_ID[raw];
  if (PROPERTY_BY_RETAILER_ID[raw]) return PROPERTY_BY_RETAILER_ID[raw];
  const codeNorm = normalizeText(raw);
  if (PROPERTY_BY_CODE[codeNorm]) return PROPERTY_BY_CODE[codeNorm];

  const direct =
    PROPERTY_CATALOG.find((p) => normalizeText(p.title) === codeNorm) ||
    PROPERTY_CATALOG.find((p) => codeNorm.includes(normalizeText(p.title))) ||
    PROPERTY_CATALOG.find((p) => codeNorm.includes(normalizeText(p.code))) ||
    PROPERTY_CATALOG.find((p) => p.location && codeNorm.includes(normalizeText(p.location))) ||
    null;

  if (direct) return direct;

  return findPropertyFromRichText(raw);
}

function detectPropertyFromUserText(text) {
  return findPropertyByAny(text);
}

function extractCatalogSelection(msg, userText = "") {
  const hints = [];

  if (msg?.type === "interactive" && msg?.interactive?.list_reply?.id) hints.push(msg.interactive.list_reply.id);
  if (msg?.type === "interactive" && msg?.interactive?.button_reply?.id) hints.push(msg.interactive.button_reply.id);
  if (msg?.type === "interactive" && msg?.interactive?.button_reply?.title) hints.push(msg.interactive.button_reply.title);
  if (msg?.type === "interactive" && msg?.interactive?.list_reply?.title) hints.push(msg.interactive.list_reply.title);

  if (msg?.type === "order" && Array.isArray(msg?.order?.product_items)) {
    for (const item of msg.order.product_items) {
      if (item?.product_retailer_id) hints.push(item.product_retailer_id);
    }
  }

  if (msg?.referral?.product_retailer_id) hints.push(msg.referral.product_retailer_id);
  if (msg?.referral?.product_id) hints.push(msg.referral.product_id);
  if (msg?.context?.referred_product?.product_retailer_id) hints.push(msg.context.referred_product.product_retailer_id);
  if (msg?.context?.referred_product?.product_id) hints.push(msg.context.referred_product.product_id);
  if (msg?.context?.referred_product?.body) hints.push(msg.context.referred_product.body);

  for (const hint of hints) {
    const property = findPropertyByAny(hint);
    if (property) return property;
  }

  return detectPropertyFromUserText(userText);
}

function getVisitDurationForProperty(property) {
  if (Number(property?.duration_min) > 0) return Number(property.duration_min);
  if (property?.category && Number(CATEGORY_DURATION[property.category]) > 0) return Number(CATEGORY_DURATION[property.category]);
  return Number(CATEGORY_DURATION.default || DEFAULT_VISIT_DURATION_MIN || 30);
}

function getNowPlusLeadUTC() {
  const now = new Date();
  const lead = Math.max(0, Number.isFinite(MIN_BOOKING_LEAD_MIN) ? MIN_BOOKING_LEAD_MIN : 60);
  return addMinutes(now, lead);
}

async function sendWhatsAppText(to, text, reportSource = "BOT") {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    { messaging_product: "whatsapp", to, type: "text", text: { body: text } },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: String(text),
    source: reportSource,
    kind: "TEXT",
  });
}

async function sendReminderWhatsAppToBestTarget(priv, fallbackPhoneDigits, text) {
  const candidates = [];
  if (priv?.wa_id) candidates.push(String(priv.wa_id).trim());
  if (priv?.wa_phone) candidates.push(toE164DigitsRD(priv.wa_phone));
  if (fallbackPhoneDigits) candidates.push(toE164DigitsRD(fallbackPhoneDigits));

  const tried = [];
  let lastErr = null;
  for (const c of candidates) {
    const to = String(c || "").replace(/[^\d]/g, "");
    if (!to || tried.includes(to)) continue;
    tried.push(to);
    try {
      await sendWhatsAppText(to, text, "BOT");
      return { ok: true, to };
    } catch (e) {
      lastErr = e;
      console.error("[reminder] send failed for:", to, e?.response?.data || e?.message || e);
    }
  }
  return { ok: false, tried, error: lastErr?.response?.data || lastErr?.message || lastErr };
}

async function sendPropertyCategoriesList(to) {
  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  const rows = PROPERTY_CATEGORIES.map((c) => ({ id: c.id, title: c.title, description: "" }));

  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "list",
        header: { type: "text", text: "Catálogo de propiedades" },
        body: { text: "Toca el botón para abrir el catálogo por categorías 👇" },
        footer: { text: BUSINESS_NAME },
        action: { button: "Abrir catálogo", sections: [{ title: "Categorías", rows }] },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: `Catálogo de propiedades\n${rows.map((r) => `• [${r.id}] ${r.title}`).join("\n")}`,
    source: "BOT",
    kind: "LIST",
    meta: { rows },
  });
}

async function sendWelcomeAndCatalog(to, introText = null) {
  await sendWhatsAppText(to, introText || welcomeText());
  await sendPropertyCategoriesList(to);
}

async function sendCatalogForCategory(to, categoryKey, session = null) {
  const properties = getPropertiesForMenuCategory(categoryKey).slice(0, 30);

  if (!properties.length) {
    await sendWhatsAppText(
      to,
      `Ahora mismo no veo propiedades cargadas en *${categoryTitle(categoryKey)}*. Puedes pedirme otra categoría o escribir *asesor* para ayudarte manualmente.`
    );
    return;
  }

  if (!WA_CATALOG_ID) {
    const preview = properties.slice(0, 12);
    if (session) {
      session.lastRecommendations = preview;
      session.state = "await_property_choice";
    }

    const lines = preview.map((p, i) => formatPropertyShortLine(p, i));
    await sendWhatsAppText(
      to,
      `Te encontré opciones en *${categoryTitle(categoryKey)}* ✅\n\n${lines.join("\n")}\n\nRespóndeme con el *número* o con el *nombre* de la propiedad que te interese y te ayudo a agendar la visita.`
    );
    return;
  }

  const url = `https://graph.facebook.com/v20.0/${PHONE_NUMBER_ID}/messages`;
  await axios.post(
    url,
    {
      messaging_product: "whatsapp",
      to,
      type: "interactive",
      interactive: {
        type: "product_list",
        header: { type: "text", text: categoryTitle(categoryKey) },
        body: { text: "Aquí tienes las propiedades disponibles. Elige una y te ayudo con la visita 👇" },
        footer: { text: BUSINESS_NAME },
        action: {
          catalog_id: WA_CATALOG_ID,
          sections: [
            {
              title: categoryTitle(categoryKey),
              product_items: properties.map((p) => ({ product_retailer_id: p.retailer_id })),
            },
          ],
        },
      },
    },
    { headers: { Authorization: `Bearer ${WA_TOKEN}` } }
  );

  await bothubReportMessage({
    direction: "OUTBOUND",
    to: String(to),
    body: `Catálogo enviado: ${categoryTitle(categoryKey)}`,
    source: "BOT",
    kind: "PRODUCT_LIST",
    meta: {
      category: categoryKey,
      productRetailerIds: properties.map((p) => p.retailer_id),
    },
  });
}

async function reportLeadEventToCrm({
  to,
  action,
  property,
  lead_name = "",
  phone = "",
  zone_interest = "",
  budget = "",
  visit_start = "",
  visit_id = "",
}) {
  try {
    const tags = getLeadTagsForProperty(property);
    const primaryTag = getPrimaryLeadTag(property);
    const body = [
      `CRM_EVENT: ${action}`,
      `ETIQUETA: ${primaryTag}`,
      property ? `PROPIEDAD: ${propertyLabel(property)}` : "",
      zone_interest ? `ZONA: ${zone_interest}` : "",
      budget ? `PRESUPUESTO: ${budget}` : "",
      visit_start ? `VISITA: ${visit_start}` : "",
    ]
      .filter(Boolean)
      .join(" | ");

    await bothubReportMessage({
      direction: "OUTBOUND",
      to: String(to || ""),
      body,
      source: "BOT",
      kind: "EVENT",
      meta: {
        action,
        primaryTag,
        tags,
        lead_name,
        phone,
        zone_interest,
        budget,
        visit_start,
        visit_id,
        property: property
          ? {
              id: property.id || "",
              retailer_id: property.retailer_id || "",
              code: property.code || "",
              title: property.title || "",
              category: property.category || "",
              operation: property.operation || "",
              location: property.location || "",
            }
          : undefined,
      },
    });
  } catch (e) {
    console.error("reportLeadEventToCrm error:", e?.response?.data || e?.message || e);
  }
}

async function notifyPersonalWhatsAppVisitSummary(visit) {
  try {
    if (!PERSONAL_WA_TO) return;
    const myTo = String(PERSONAL_WA_TO).replace(/[^\d]/g, "");
    if (!myTo) return;
    const leadPhone = String(visit?.phone || "").replace(/[^\d]/g, "");
    if (leadPhone && myTo === leadPhone) return;

    const primaryTag = getPrimaryLeadTag(visit);
    const summary =
      `🏡 *Nueva visita agendada*\n\n` +
      `🏢 Inmobiliaria: *${BUSINESS_NAME}*\n` +
      `🏷️ Etiqueta CRM: *${primaryTag}*\n` +
      `🏠 Propiedad: *${visit.property_title || visit.property_code || "—"}*\n` +
      `🆔 Código: *${visit.property_code || "—"}*\n` +
      `👤 Lead: *${visit.lead_name || "—"}*\n` +
      `📞 Tel: *${leadPhone || "—"}*\n` +
      `📅 Fecha: *${formatDateInTZ(visit.start, BUSINESS_TIMEZONE)}*\n` +
      `⏰ Hora: *${formatTimeInTZ(visit.start, BUSINESS_TIMEZONE)}*\n` +
      `📍 Dirección: ${BUSINESS_ADDRESS || "—"}\n` +
      `🆔 ID: ${visit.visit_id || "—"}`;

    await sendWhatsAppText(myTo, summary, "BOT");
  } catch (e) {
    console.error("notifyPersonalWhatsAppVisitSummary error:", e?.response?.data || e?.message || e);
  }
}

async function getBusyRanges(calendar, timeMinISO, timeMaxISO) {
  const fb = await calendar.freebusy.query({
    requestBody: {
      timeMin: timeMinISO,
      timeMax: timeMaxISO,
      timeZone: BUSINESS_TIMEZONE,
      items: [{ id: GOOGLE_CALENDAR_ID }],
    },
  });
  const busy = fb.data.calendars?.[GOOGLE_CALENDAR_ID]?.busy || [];
  return busy.map((b) => ({ start: new Date(b.start), end: new Date(b.end) }));
}

function overlaps(aStart, aEnd, bStart, bEnd) {
  return aStart < bEnd && aEnd > bStart;
}

function buildCandidateSlotsZoned({ fromISO, toISO, durationMin }) {
  const from = new Date(fromISO);
  const to = new Date(toISO);
  const fromP = getZonedParts(from, BUSINESS_TIMEZONE);
  const toP = getZonedParts(to, BUSINESS_TIMEZONE);

  let curUTC = zonedTimeToUtc({ year: fromP.year, month: fromP.month, day: fromP.day, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
  const endUTC = zonedTimeToUtc({ year: toP.year, month: toP.month, day: toP.day, hour: 23, minute: 59 }, BUSINESS_TIMEZONE);

  const slots = [];
  while (curUTC <= endUTC) {
    const curLocal = getZonedParts(curUTC, BUSINESS_TIMEZONE);
    const js = new Date(Date.UTC(curLocal.year, curLocal.month - 1, curLocal.day, 12, 0, 0));
    const isoWeekday = ((js.getUTCDay() + 6) % 7) + 1;
    const key = weekdayKeyFromISOWeekday(isoWeekday);
    const wh = WORK_HOURS[key];

    if (wh) {
      const [sh, sm] = wh.start.split(":").map((n) => parseInt(n, 10));
      const [eh, em] = wh.end.split(":").map((n) => parseInt(n, 10));
      let cursorMin = sh * 60 + sm;
      const endMin = eh * 60 + em;
      cursorMin = Math.ceil(cursorMin / SLOT_STEP_MIN) * SLOT_STEP_MIN;

      while (cursorMin + durationMin <= endMin) {
        const h = Math.floor(cursorMin / 60);
        const m = cursorMin % 60;
        const slotStartUTC = zonedTimeToUtc(
          { year: curLocal.year, month: curLocal.month, day: curLocal.day, hour: h, minute: m },
          BUSINESS_TIMEZONE
        );
        const slotEndUTC = new Date(slotStartUTC.getTime() + durationMin * 60000);
        if (slotStartUTC >= from && slotEndUTC <= to) {
          slots.push({ slot_id: `slot_${slotStartUTC.getTime()}`, start: slotStartUTC.toISOString(), end: slotEndUTC.toISOString() });
        }
        cursorMin += SLOT_STEP_MIN;
      }
    }

    const nextDayUTC = zonedTimeToUtc(
      { year: curLocal.year, month: curLocal.month, day: curLocal.day, hour: 0, minute: 0 },
      BUSINESS_TIMEZONE
    );
    curUTC = new Date(nextDayUTC.getTime() + 24 * 60 * 60000);
  }

  return slots;
}

async function getAvailableVisitSlotsTool({ property, from, to }) {
  const calendar = getCalendarClient();
  const durationMin = getVisitDurationForProperty(property);
  const busyRanges = await getBusyRanges(calendar, from, to);
  const candidates = buildCandidateSlotsZoned({ fromISO: from, toISO: to, durationMin });
  const nowPlusLead = getNowPlusLeadUTC();

  const free = candidates
    .filter((c) => {
      const cs = new Date(c.start);
      const ce = new Date(c.end);
      if (cs < nowPlusLead) return false;
      return !busyRanges.some((b) => overlaps(cs, ce, b.start, b.end));
    })
    .sort((a, b) => new Date(a.start).getTime() - new Date(b.start).getTime())
    .slice(0, MAX_SLOTS_RETURN)
    .map((slot) => ({
      ...slot,
      property_id: property?.id || "",
      property_code: property?.code || "",
      property_title: property?.title || "",
      property_retailer_id: property?.retailer_id || "",
      category: property?.category || "",
    }));

  return free;
}

async function bookVisitTool({ lead_name, phone, slot_id, property, zone_interest, budget, notes, slot_start, slot_end, wa_id }) {
  const calendar = getCalendarClient();
  if (!slot_start || !slot_end) throw new Error("Missing slot_start/slot_end");
  if (!property?.id) throw new Error("Missing property");

  const event = await calendar.events.insert({
    calendarId: GOOGLE_CALENDAR_ID,
    requestBody: {
      summary: `Visita - ${property.code || property.title} - ${lead_name}`,
      location: BUSINESS_ADDRESS || property.location || undefined,
      description:
        `Lead: ${lead_name}\n` +
        `Tel: ${phone}\n` +
        `Propiedad: ${property.title}\n` +
        `Código: ${property.code || ""}\n` +
        `Categoría: ${property.category || ""}\n` +
        `Ubicación: ${property.location || ""}\n` +
        `Zona de interés: ${zone_interest || ""}\n` +
        `Presupuesto: ${budget || ""}\n` +
        `Notas: ${notes || ""}\n` +
        `SlotId: ${slot_id}`,
      start: { dateTime: slot_start, timeZone: BUSINESS_TIMEZONE },
      end: { dateTime: slot_end, timeZone: BUSINESS_TIMEZONE },
      extendedProperties: {
        private: {
          wa_phone: phone,
          wa_id: wa_id || "",
          lead_name,
          property_id: property.id,
          property_code: property.code || "",
          property_title: property.title || "",
          property_retailer_id: property.retailer_id || "",
          category: property.category || "",
          operation: property.operation || "",
          lead_tag: getPrimaryLeadTag(property),
          lead_tags: getLeadTagsForProperty(property).join(","),
          zone_interest: zone_interest || "",
          budget: budget || "",
          slot_id,
          reminder24hSent: "false",
          reminder2hSent: "false",
        },
      },
    },
  });

  return {
    visit_id: event.data.id,
    start: slot_start,
    end: slot_end,
    lead_name,
    phone,
    property_id: property.id,
    property_code: property.code || "",
    property_title: property.title || "",
    property_retailer_id: property.retailer_id || "",
    category: property.category || "",
    operation: property.operation || "",
    lead_tag: getPrimaryLeadTag(property),
    lead_tags: getLeadTagsForProperty(property),
    zone_interest: zone_interest || "",
    budget: budget || "",
  };
}

async function rescheduleVisitTool({
  visit_id,
  new_slot_id,
  new_start,
  new_end,
  property,
  lead_name,
  phone,
  wa_id,
  zone_interest,
  budget,
}) {
  const calendar = getCalendarClient();
  if (!new_start || !new_end) throw new Error("Missing new_start/new_end");

  const current = await calendar.events.get({ calendarId: GOOGLE_CALENDAR_ID, eventId: visit_id });
  const priv = current.data.extendedProperties?.private || {};

  const nextProperty = property || {
    id: priv.property_id || "",
    code: priv.property_code || "",
    title: priv.property_title || "",
    retailer_id: priv.property_retailer_id || "",
    category: priv.category || "",
  };

  const nextLead = String(lead_name || priv.lead_name || "").trim();
  const nextPhone = String(phone || priv.wa_phone || "").trim();
  const nextWaId = String(wa_id || priv.wa_id || "").trim();
  const nextZone = String(zone_interest || priv.zone_interest || "").trim();
  const nextBudget = String(budget || priv.budget || "").trim();

  const nextPriv = {
    ...priv,
    slot_id: new_slot_id,
    lead_name: nextLead,
    wa_phone: nextPhone,
    wa_id: nextWaId,
    property_id: nextProperty.id || priv.property_id || "",
    property_code: nextProperty.code || priv.property_code || "",
    property_title: nextProperty.title || priv.property_title || "",
    property_retailer_id: nextProperty.retailer_id || priv.property_retailer_id || "",
    category: nextProperty.category || priv.category || "",
    operation: nextProperty.operation || priv.operation || "",
    lead_tag: getPrimaryLeadTag(nextProperty),
    lead_tags: getLeadTagsForProperty(nextProperty).join(","),
    zone_interest: nextZone,
    budget: nextBudget,
    reminder24hSent: "false",
    reminder2hSent: "false",
  };

  const updated = await calendar.events.patch({
    calendarId: GOOGLE_CALENDAR_ID,
    eventId: visit_id,
    requestBody: {
      summary: `Visita - ${nextPriv.property_code || nextPriv.property_title || "Propiedad"} - ${nextLead || "Lead"}`,
      start: { dateTime: new_start, timeZone: BUSINESS_TIMEZONE },
      end: { dateTime: new_end, timeZone: BUSINESS_TIMEZONE },
      extendedProperties: { private: nextPriv },
    },
  });

  return { ok: true, visit_id: updated.data.id, new_start, new_end };
}

async function cancelVisitTool({ visit_id, reason }) {
  const calendar = getCalendarClient();
  const event = await calendar.events.get({ calendarId: GOOGLE_CALENDAR_ID, eventId: visit_id });
  const summary = event.data.summary || "Visita";
  await calendar.events.patch({
    calendarId: GOOGLE_CALENDAR_ID,
    eventId: visit_id,
    requestBody: {
      summary: `CANCELADA - ${summary}`,
      description: (event.data.description || "") + `\n\nCancelación: ${reason || ""}`,
      extendedProperties: {
        private: { ...(event.data.extendedProperties?.private || {}), status: "cancelled" },
      },
    },
  });
  return { ok: true, visit_id };
}

async function handoffToHumanTool({ summary }) {
  return { ok: true, routed: true, summary };
}

async function findUpcomingVisitByPhone(phone, windowDays = 120) {
  try {
    const phoneDigits = String(phone || "").replace(/[^\d]/g, "");
    if (!phoneDigits) return null;

    const calendar = getCalendarClient();
    const now = new Date();
    const end = addMinutes(now, windowDays * 24 * 60);

    const list = await calendar.events.list({
      calendarId: GOOGLE_CALENDAR_ID,
      timeMin: now.toISOString(),
      timeMax: end.toISOString(),
      singleEvents: true,
      orderBy: "startTime",
      maxResults: 100,
    });

    const events = list.data.items || [];
    for (const ev of events) {
      const priv = ev.extendedProperties?.private || {};
      if (priv.status === "cancelled") continue;
      const wa = String(priv.wa_phone || "").replace(/[^\d]/g, "");
      if (!wa || wa !== phoneDigits) continue;
      const start = ev.start?.dateTime;
      const endDT = ev.end?.dateTime;
      if (!start || !endDT) continue;
      return {
        visit_id: ev.id,
        start,
        end: endDT,
        lead_name: String(priv.lead_name || "").trim(),
        phone: phoneDigits,
        property_id: String(priv.property_id || ""),
        property_code: String(priv.property_code || ""),
        property_title: String(priv.property_title || ""),
        property_retailer_id: String(priv.property_retailer_id || ""),
        category: String(priv.category || ""),
        operation: String(priv.operation || ""),
        lead_tag: String(priv.lead_tag || ""),
        lead_tags: String(priv.lead_tags || "").split(",").filter(Boolean),
        zone_interest: String(priv.zone_interest || ""),
        budget: String(priv.budget || ""),
      };
    }

    return null;
  } catch (e) {
    console.error("findUpcomingVisitByPhone error:", e?.response?.data || e?.message || e);
    return null;
  }
}

const DOW = {
  lunes: 1,
  martes: 2,
  miercoles: 3,
  miércoles: 3,
  jueves: 4,
  viernes: 5,
  sabado: 6,
  sábado: 6,
  domingo: 7,
};

const MONTHS = {
  enero: 1,
  febrero: 2,
  marzo: 3,
  abril: 4,
  mayo: 5,
  junio: 6,
  julio: 7,
  agosto: 8,
  septiembre: 9,
  setiembre: 9,
  octubre: 10,
  noviembre: 11,
  diciembre: 12,
};

function parseDateRangeFromText(userText) {
  const t = normalizeText(userText);

  if (t.includes("hoy")) {
    const from = startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "hoy" };
  }
  if (t.includes("pasado manana") || t.includes("pasado mañana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 2, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "pasado mañana" };
  }
  if (t.includes("manana") || t.includes("mañana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 1, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "mañana" };
  }

  if (t.includes("semana que viene") || t.includes("la semana que viene") || t.includes("siguiente semana")) {
    const from = addLocalDaysUTC(startOfLocalDayUTC(new Date(), BUSINESS_TIMEZONE), 1, BUSINESS_TIMEZONE);
    const to = addLocalDaysUTC(from, 7, BUSINESS_TIMEZONE);
    return { from: from.toISOString(), to: to.toISOString(), label: "la semana que viene" };
  }

  for (const [mname, mnum] of Object.entries(MONTHS)) {
    if (t === mname || t.includes(`en ${mname}`) || t.includes(`para ${mname}`)) {
      const nowP = getZonedParts(new Date(), BUSINESS_TIMEZONE);
      let year = nowP.year;
      if (mnum < nowP.month) year += 1;
      const r = rangeForWholeMonth(year, mnum, BUSINESS_TIMEZONE);
      return { ...r, label: mname };
    }
  }

  for (const [name, iso] of Object.entries(DOW)) {
    if (t.includes(name)) {
      const isNext = t.includes("proximo") || t.includes("próximo") || t.includes("que viene") || t.includes("siguiente");
      const fromDay = nextWeekdayFromTodayUTC(iso, BUSINESS_TIMEZONE, isNext);
      const toDay = addLocalDaysUTC(fromDay, 1, BUSINESS_TIMEZONE);
      return { from: fromDay.toISOString(), to: toDay.toISOString(), label: name };
    }
  }

  const m1 = t.match(/(\d{1,2})\s+de\s+([a-záéíóú]+)(\s+de\s+(\d{4}))?/);
  if (m1) {
    const day = parseInt(m1[1], 10);
    const monthName = normalizeText(m1[2]);
    const month = MONTHS[monthName];
    if (month) {
      const now = new Date();
      const nowP = getZonedParts(now, BUSINESS_TIMEZONE);
      let year = m1[4] ? parseInt(m1[4], 10) : nowP.year;
      if (!m1[4]) {
        const candidateUTC = zonedTimeToUtc({ year, month, day, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
        if (candidateUTC < startOfLocalDayUTC(now, BUSINESS_TIMEZONE)) year += 1;
      }
      const from = zonedTimeToUtc({ year, month, day, hour: 0, minute: 0 }, BUSINESS_TIMEZONE);
      const to = addLocalDaysUTC(from, 1, BUSINESS_TIMEZONE);
      return { from: from.toISOString(), to: to.toISOString(), label: `${day} de ${monthName}` };
    }
  }

  return null;
}

function parseUserTimeTo24h(userText) {
  const raw = String(userText || "").trim().toLowerCase();
  if (!raw) return null;
  let compact = raw.replace(/\./g, "").replace(/\s+/g, " ").trim();
  compact = compact.replace(/\b([ap])\s*m\b/g, "$1m");
  if (/^\d{1,2}$/.test(compact)) return null;
  const m = compact.match(/^(\d{1,2})(?::(\d{2}))?\s*(am|pm)?$/i);
  if (!m) return null;

  let hh = parseInt(m[1], 10);
  const mm = m[2] ? parseInt(m[2], 10) : 0;
  const mer = m[3] ? String(m[3]).toLowerCase() : "";
  if (Number.isNaN(hh) || Number.isNaN(mm) || mm < 0 || mm > 59) return null;

  if (mer === "am" || mer === "pm") {
    if (hh < 1 || hh > 12) return null;
    if (mer === "pm" && hh !== 12) hh += 12;
    if (mer === "am" && hh === 12) hh = 0;
  } else if (hh < 0 || hh > 23) {
    return null;
  }

  return { hh, mm, meridian: mer || null };
}

function buildHourlyDisplaySlotsAvailableOnly(allFreeSlots) {
  const out = [];
  for (let h = HOURLY_LIST_START; h <= HOURLY_LIST_END; h++) {
    const match = allFreeSlots.find((s) => {
      const parts = getZonedParts(new Date(s.start), BUSINESS_TIMEZONE);
      return parts.hour === h && parts.minute === 0;
    });
    if (match) out.push(match);
  }
  return out;
}

function formatSlotsList(property, slots, session) {
  if (!slots?.length) return null;
  const dateLabel = formatDateInTZ(slots[0].start, BUSINESS_TIMEZONE);
  const propertyText = propertyPublicLabel(property);

  if (HOURLY_LIST_MODE) {
    const displaySlots = buildHourlyDisplaySlotsAvailableOnly(slots);
    if (session) session.lastDisplaySlots = displaySlots;

    if (!displaySlots.length) {
      return `No veo horarios disponibles entre *8:00 am* y *5:00 pm* para visitar *${propertyText}* el *${dateLabel}*.\nDime otro día.`;
    }

    return (
      `Estos son los horarios disponibles para visitar *${propertyText}* el *${dateLabel}*:\n\n` +
      displaySlots
        .map((s, i) => `${i + 1}. ${formatTimeInTZ(s.start, BUSINESS_TIMEZONE)} - ${formatTimeInTZ(s.end, BUSINESS_TIMEZONE)}`)
        .join("\n") +
      `\n\nResponde con el *número* (1,2,3...) o escribe la *hora* (ej: 10:00 am / 3:00 pm).`
    );
  }

  const view = slots.slice(0, Math.max(1, DISPLAY_SLOTS_LIMIT));
  return (
    `Estos son los horarios disponibles para visitar *${propertyText}* el *${dateLabel}*:\n\n` +
    view
      .map((s, i) => `${i + 1}. ${formatTimeInTZ(s.start, BUSINESS_TIMEZONE)} - ${formatTimeInTZ(s.end, BUSINESS_TIMEZONE)}`)
      .join("\n") +
    `\n\nResponde con el *número* (1,2,3...) o escribe la *hora*.`
  );
}

function tryPickSlotFromUserText(session, userText) {
  const t = normalizeText(userText);

  if (/^\d+$/.test(t)) {
    const num = parseInt(t, 10);
    if (!Number.isNaN(num)) {
      if (HOURLY_LIST_MODE) {
        if (num >= 1 && num <= (session.lastDisplaySlots?.length || 0)) return session.lastDisplaySlots[num - 1] || null;
        return null;
      }
      if (num >= 1 && num <= Math.min(session.lastSlots.length, DISPLAY_SLOTS_LIMIT)) return session.lastSlots[num - 1];
    }
  }

  const parsed = parseUserTimeTo24h(userText);
  if (parsed) {
    const { hh, mm } = parsed;
    if (HOURLY_LIST_MODE) {
      if (mm !== 0 || hh < HOURLY_LIST_START || hh > HOURLY_LIST_END) return null;
      return (
        session.lastSlots.find((s) => {
          const parts = getZonedParts(new Date(s.start), BUSINESS_TIMEZONE);
          return parts.hour === hh && parts.minute === 0;
        }) || null
      );
    }
    return (
      session.lastSlots.find((s) => {
        const parts = getZonedParts(new Date(s.start), BUSINESS_TIMEZONE);
        return parts.hour === hh && parts.minute === mm;
      }) || null
    );
  }

  return null;
}

async function callOpenAIFallback({ session, userText, extraSystem = "" }) {
  if (!OPENAI_API_KEY) {
    return `Puedo ayudarte a ver el catálogo, entender lo que buscas, recomendar propiedades y agendar una visita. Escribe *catálogo* o dime qué tipo de propiedad buscas.`;
  }

  try {
    const selectedPropertyContext = session?.selectedProperty
      ? `\nPropiedad seleccionada:\n${propertySummary(session.selectedProperty)}`
      : "";
    const recommendationContext = session?.lastRecommendations?.length
      ? `\nÚltimas recomendaciones:\n${session.lastRecommendations.map((p, i) => formatPropertyShortLine(p, i)).join("\n")}`
      : "";
    const leadProfileContext = session?.aiProfile ? `\nPerfil actual del lead: ${JSON.stringify(session.aiProfile)}` : "";
    const mediaContext = session?.mediaContext
      ? `\nÚltimo audio transcrito: ${session.mediaContext.lastAudioText || ""}\nÚltimo texto extraído de imagen: ${session.mediaContext.lastImageText || ""}`
      : "";

    const system = {
      role: "system",
      content:
        `Eres el asistente de WhatsApp de ${BUSINESS_NAME}. ` +
        `Tu función es actuar como asesor inmobiliario inicial: entender lo que busca el cliente, filtrar, recomendar opciones reales del catálogo, responder dudas básicas con la información disponible y llevarlo a agendar la visita. ` +
        `No inventes propiedades, precios, disponibilidad ni horarios. ` +
        `Si no tienes el dato, dilo y ofrece pasar con un asesor. ` +
        `Mantén respuestas cortas, claras y orientadas a cerrar visita o recomendar el siguiente paso. ` +
        `${extraSystem}${selectedPropertyContext}${recommendationContext}${leadProfileContext}${mediaContext}`,
    };

    const messages = [system, ...session.messages.slice(-8), { role: "user", content: userText }];
    const resp = await axios.post(
      "https://api.openai.com/v1/chat/completions",
      {
        model: OPENAI_MODEL,
        messages,
        temperature: 0.2,
      },
      { headers: { Authorization: `Bearer ${OPENAI_API_KEY}` } }
    );

    const text = resp.data.choices?.[0]?.message?.content?.trim() || "";
    if (text) {
      session.messages.push({ role: "user", content: userText });
      session.messages.push({ role: "assistant", content: text });
      return text;
    }
  } catch (e) {
    console.error("callOpenAIFallback error:", e?.response?.data || e?.message || e);
  }

  return `Puedo ayudarte a ver el catálogo, entender lo que buscas, recomendar propiedades y agendar una visita. Escribe *catálogo* o dime qué tipo de propiedad buscas.`;
}

// =========================
// Inbound parsing CRM vs BOT
// =========================
function extractIncomingTextForCrm(msg) {
  if (!msg) return "";

  if (msg?.text?.body) return msg.text.body;

  if (msg?.type === "interactive" && msg?.interactive?.list_reply) {
    return msg.interactive.list_reply.id || msg.interactive.list_reply.title || "";
  }

  if (msg?.type === "interactive" && msg?.interactive?.button_reply) {
    return msg.interactive.button_reply.id || msg.interactive.button_reply.title || "";
  }

  if (msg?.type === "order" && msg?.order?.product_items?.length) {
    return msg.order.product_items[0]?.product_retailer_id || "[CATALOG_ORDER]";
  }

  if (msg?.type === "audio" && msg?.audio?.id) return "[AUDIO]";

  if (msg?.type === "location" && msg?.location) {
    const { latitude, longitude, name, address } = msg.location;
    return `📍 Ubicación: ${name || ""} ${address || ""} (${latitude}, ${longitude})`.trim();
  }

  if (msg?.type === "image" && msg?.image?.id) {
    return cleanText(msg?.image?.caption) || "[IMAGE]";
  }

  if (msg?.type === "video" && msg?.video?.id) {
    return cleanText(msg?.video?.caption) || "[VIDEO]";
  }

  if (msg?.type === "document" && msg?.document?.id) {
    return cleanText(msg?.document?.filename) || "[DOCUMENT]";
  }

  if (msg?.type === "sticker" && msg?.sticker?.id) return "[STICKER]";

  if (msg?.type === "contacts" && msg?.contacts?.length) {
    return formatSharedContactsForText(msg.contacts);
  }

  if (msg?.type === "reaction" && msg?.reaction) return `[REACTION] ${msg.reaction.emoji || ""}`.trim();

  return `[${(msg?.type || "UNKNOWN").toUpperCase()}]`;
}

async function resolveIncomingTextForBot(msg, session = null) {
  if (!msg) return "";

  if (msg?.text?.body) return msg.text.body;

  if (msg?.type === "interactive" && msg?.interactive?.list_reply) {
    return msg.interactive.list_reply.id || msg.interactive.list_reply.title || "";
  }

  if (msg?.type === "interactive" && msg?.interactive?.button_reply) {
    return msg.interactive.button_reply.id || msg.interactive.button_reply.title || "";
  }

  if (msg?.type === "order" && msg?.order?.product_items?.length) {
    return msg.order.product_items[0]?.product_retailer_id || "[CATALOG_ORDER]";
  }

  if (msg?.type === "audio" && msg?.audio?.id) {
    try {
      const downloaded = await downloadMetaMedia(msg.audio.id);
      const transcription = await transcribeAudioBuffer(
        downloaded.buffer,
        downloaded.mimeType || msg?.audio?.mime_type || "audio/ogg"
      );
      const finalText = cleanText(transcription || "");
      if (session) session.mediaContext.lastAudioText = finalText;
      return finalText || "[AUDIO]";
    } catch (e) {
      console.error("resolveIncomingTextForBot audio error:", e?.message || e);
      return "[AUDIO]";
    }
  }

  if (msg?.type === "location" && msg?.location) {
    const { latitude, longitude, name, address } = msg.location;
    return `📍 Ubicación: ${name || ""} ${address || ""} (${latitude}, ${longitude})`.trim();
  }

  if (msg?.type === "image" && msg?.image?.id) {
    const caption = cleanText(msg?.image?.caption || "");
    try {
      const downloaded = await downloadMetaMedia(msg.image.id);
      const analysis = await understandPropertyImageBuffer(
        downloaded.buffer,
        downloaded.mimeType || msg?.image?.mime_type || "image/jpeg",
        caption
      );

      const finalText = cleanText(analysis?.normalized_user_text || caption || "");
      if (session) session.mediaContext.lastImageText = cleanText(analysis?.extracted_text || finalText || "");
      return finalText || caption || "[IMAGE]";
    } catch (e) {
      console.error("resolveIncomingTextForBot image error:", e?.message || e);
      return caption || "[IMAGE]";
    }
  }

  if (msg?.type === "video" && msg?.video?.id) return cleanText(msg?.video?.caption) || "[VIDEO]";
  if (msg?.type === "document" && msg?.document?.id) return cleanText(msg?.document?.filename) || "[DOCUMENT]";
  if (msg?.type === "sticker" && msg?.sticker?.id) return "[STICKER]";

  if (msg?.type === "contacts" && msg?.contacts?.length) {
    const summary = formatSharedContactsForText(msg.contacts);
    const primaryName = getPrimarySharedContactName(msg.contacts);
    const primaryPhone = getPrimarySharedContactPhoneDigits(msg.contacts);

    if (session?.state === "await_phone" && primaryPhone) return primaryPhone;
    if (session?.state === "await_name" && primaryName) return primaryName;

    return summary;
  }

  if (msg?.type === "reaction" && msg?.reaction) return `[REACTION] ${msg.reaction.emoji || ""}`.trim();

  return `[${(msg?.type || "UNKNOWN").toUpperCase()}]`;
}

function clearVisitFlow(session, keepGreeting = true) {
  session.state = "idle";
  session.lastSlots = [];
  session.lastDisplaySlots = [];
  session.selectedSlot = null;
  session.selectedProperty = null;
  session.pendingCategory = null;
  session.pendingRange = null;
  session.pendingName = null;
  session.pendingPhone = null;
  session.pendingZone = null;
  session.pendingBudget = null;
  session.lastRecommendations = [];
  session.reschedule = defaultSession().reschedule;
  if (!keepGreeting) session.greeted = false;
}

async function finalizeVisitBookingAndNotify({ from, session }) {
  const visit = await bookVisitTool({
    lead_name: session.pendingName,
    phone: session.pendingPhone,
    slot_id: session.selectedSlot.slot_id,
    property: session.selectedProperty,
    zone_interest: session.aiProfile?.zone_interest || "",
    budget: session.aiProfile?.budget || "",
    notes: "",
    slot_start: session.selectedSlot.start,
    slot_end: session.selectedSlot.end,
    wa_id: from,
  });

  await sendWhatsAppText(
    from,
    `✅ *Visita reservada*\n\n` +
      `🏠 Propiedad: *${visit.property_title || "—"}*\n` +
      `👤 Lead: *${visit.lead_name}*\n` +
      `📞 Teléfono: *${visit.phone}*\n` +
      `📅 Fecha: *${formatDateInTZ(visit.start, BUSINESS_TIMEZONE)}*\n` +
      `⏰ Hora: *${formatTimeInTZ(visit.start, BUSINESS_TIMEZONE)}*\n` +
      `📍 Dirección: ${BUSINESS_ADDRESS || session.selectedProperty?.location || "—"}\n\n` +
      `Responde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`
  );

  await notifyPersonalWhatsAppVisitSummary(visit);
  await reportLeadEventToCrm({
    to: from,
    action: "visit_booked",
    property: session.selectedProperty,
    lead_name: visit.lead_name,
    phone: visit.phone,
    zone_interest: visit.zone_interest,
    budget: visit.budget,
    visit_start: visit.start,
    visit_id: visit.visit_id,
  });

  session.lastVisit = visit;
  session.state = "post_booking";
  session.lastSlots = [];
  session.lastDisplaySlots = [];
  session.lastRecommendations = [];
  session.selectedSlot = null;
  session.pendingName = null;
  session.pendingPhone = null;
  session.pendingZone = null;
  session.pendingBudget = null;
  session.pendingRange = null;
  session.reschedule = defaultSession().reschedule;

  return visit;
}

app.get("/webhook", (req, res) => {
  const mode = req.query["hub.mode"];
  const token = req.query["hub.verify_token"];
  const challenge = req.query["hub.challenge"];
  if (mode === "subscribe" && token === VERIFY_TOKEN) return res.status(200).send(challenge);
  return res.sendStatus(403);
});

app.post("/agent_message", async (req, res) => {
  try {
    if (!BOTHUB_WEBHOOK_SECRET) return res.status(400).json({ error: "BOTHUB_WEBHOOK_SECRET not configured" });
    const signature = getHubSignature(req);
    const okSig = verifyHubSignature(req.body, signature, BOTHUB_WEBHOOK_SECRET);
    if (!signature || !okSig) return res.status(401).json({ error: "Invalid signature" });

    const { waTo, text } = req.body || {};
    if (!waTo || !String(waTo).trim()) return res.status(400).json({ error: "waTo is required" });
    if (!text || !String(text).trim()) return res.status(400).json({ error: "text is required" });

    await sendWhatsAppText(String(waTo), String(text), "AGENT");
    return res.json({ ok: true });
  } catch (e) {
    console.error("agent_message error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "Internal error" });
  }
});

app.post("/conversation_mode", async (req, res) => {
  try {
    if (!BOTHUB_WEBHOOK_SECRET) {
      return res.status(400).json({ error: "BOTHUB_WEBHOOK_SECRET not configured" });
    }

    const signature = getHubSignature(req);
    const okSig = verifyHubSignature(req.body, signature, BOTHUB_WEBHOOK_SECRET);

    if (!signature || !okSig) {
      return res.status(401).json({ error: "Invalid signature" });
    }

    const { waTo, mode } = req.body || {};
    const phone = String(waTo || "").replace(/[^\d]/g, "");
    const normalizedMode = String(mode || "").toUpperCase().trim();

    if (!phone) {
      return res.status(400).json({ error: "waTo is required" });
    }

    if (!normalizedMode || !["HUMAN", "BOT"].includes(normalizedMode)) {
      return res.status(400).json({ error: "mode must be HUMAN or BOT" });
    }

    const session = await getSession(phone);
    session.humanTakeover = normalizedMode === "HUMAN";
    await saveSession(phone, session);

    return res.json({
      ok: true,
      phone,
      humanTakeover: session.humanTakeover,
    });
  } catch (e) {
    console.error("conversation_mode error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "Internal error" });
  }
});

app.get("/hub_media/:mediaId", async (req, res) => {
  try {
    const { mediaId } = req.params || {};
    const ts = String(req.query?.ts || "");
    const sig = String(req.query?.sig || "");

    if (!mediaId) return res.status(400).json({ error: "mediaId is required" });
    if (!verifyHubMediaToken(mediaId, ts, sig)) return res.status(401).json({ error: "Invalid or expired media signature" });
    if (!WA_TOKEN) return res.status(500).json({ error: "WA_TOKEN not configured in bot" });

    const downloaded = await downloadMetaMedia(mediaId);
    const mimeType = String(downloaded?.mimeType || "application/octet-stream");
    const filename = sanitizeFileName(
      downloaded?.meta?.filename || `media-${mediaId}${extFromMimeType(mimeType)}`,
      `media-${mediaId}${extFromMimeType(mimeType)}`
    );

    res.setHeader("Content-Type", mimeType);
    res.setHeader("Cache-Control", "private, max-age=300");
    res.setHeader("Content-Disposition", `inline; filename="${String(filename).replace(/"/g, "")}"`);
    return res.status(200).send(downloaded.buffer);
  } catch (e) {
    console.error("hub_media error:", e?.response?.data || e?.message || e);
    return res.status(500).json({ error: "hub_media_failed", detail: e?.response?.data || e?.message || "unknown" });
  }
});

app.post("/webhook", async (req, res) => {
  let from = "";
  let session = null;

  try {
    if (!verifyMetaSignature(req)) return res.sendStatus(403);

    const entry = req.body.entry?.[0];
    const change = entry?.changes?.[0];
    const value = change?.value;
    const msg = value?.messages?.[0];
    if (!msg) return res.sendStatus(200);

    from = msg.from;
    if (!from) return res.sendStatus(200);

    session = await getSession(from);
    const msgId = msg?.id;
    if (msgId && session.lastMsgId === msgId) return res.sendStatus(200);
    if (msgId) session.lastMsgId = msgId;

    const rawUserText = extractIncomingTextForCrm(msg);
    const resolvedUserText = await resolveIncomingTextForBot(msg, session);
    const userText = String(resolvedUserText || rawUserText || "").trim();
    const tNorm = normalizeText(userText);
    if (!userText) return res.sendStatus(200);

    const inboundMeta = extractInboundMeta(msg);
    const inboundMetaWithMediaUrl = attachHubMediaUrl(req, inboundMeta);

    await bothubReportMessage({
      direction: "INBOUND",
      from: String(from),
      body: String(rawUserText || ""),
      source: "WHATSAPP",
      waMessageId: msg?.id,
      name: value?.contacts?.[0]?.profile?.name,
      kind: inboundMetaWithMediaUrl?.kind || (msg?.type ? String(msg.type).toUpperCase() : "UNKNOWN"),
      meta: {
        ...inboundMetaWithMediaUrl,
        aiResolvedText:
          resolvedUserText && normalizeText(resolvedUserText) !== normalizeText(rawUserText)
            ? resolvedUserText
            : undefined,
      },
      mediaUrl: inboundMetaWithMediaUrl?.mediaUrl || undefined,
    });

    if (session.humanTakeover) {
      return res.sendStatus(200);
    }

    const wantsCancel = looksLikeCancel(tNorm) || isChoice(tNorm, 3);
    const wantsReschedule = looksLikeReschedule(tNorm) || isChoice(tNorm, 2);
    const wantsConfirm = looksLikeConfirm(tNorm) || isChoice(tNorm, 1);
    const wantsRestart = [
      "reiniciar",
      "reset",
      "resetear",
      "inicio",
      "ir al inicio",
      "volver al inicio",
      "empezar de nuevo",
      "menu principal",
      "menú principal",
    ].some((k) => tNorm === k || tNorm.includes(k));

    const isPropertyQuestionLike = looksLikePropertyQuestion(tNorm) || looksLikeGeneralQuestion(userText);

    if (wantsRestart) {
      session.lastVisit = null;
      clearVisitFlow(session);
      session.greeted = true;
      await sendWelcomeAndCatalog(from, welcomeText());
      return res.sendStatus(200);
    }

    if ((wantsCancel || wantsReschedule || wantsConfirm) && !session.lastVisit) {
      const found = await findUpcomingVisitByPhone(from);
      if (found) {
        session.lastVisit = found;
        session.state = "post_booking";
      }
    }

    const detectedPropertyEarly = extractCatalogSelection(msg, userText);
    const detectedCategoryEarly = detectCategoryKeyFromUser(userText);
    const detectedRangeEarly = parseDateRangeFromText(userText);
    const hasEarlyIntent =
      !!detectedPropertyEarly ||
      !!detectedCategoryEarly ||
      !!detectedRangeEarly ||
      tNorm.includes("visita") ||
      tNorm.includes("agendar") ||
      tNorm.includes("reservar") ||
      tNorm.includes("reprogram") ||
      tNorm.includes("cancel") ||
      looksLikeCatalogRequest(tNorm);

    if (
      !detectedPropertyEarly &&
      !session.selectedProperty &&
      isPropertyQuestionLike &&
      !shouldUseAdvisorSearch(userText)
    ) {
      await sendWhatsAppText(
        from,
        `Puedo responderte eso 😊\n\nPrimero necesito saber *de cuál propiedad hablas*.\nToca el botón del catálogo y elige una propiedad, o escríbeme el nombre.`
      );
      await sendPropertyCategoriesList(from);
      return res.sendStatus(200);
    }

    if (session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      await sendWelcomeAndCatalog(from, quickHelpText());
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle" && isGreeting(tNorm) && !hasEarlyIntent) {
      session.greeted = true;
      await sendWelcomeAndCatalog(from, welcomeText());
      return res.sendStatus(200);
    }

    if (!session.greeted && session.state === "idle") session.greeted = true;

    if (looksLikeHuman(tNorm)) {
      await handoffToHumanTool({ summary: `Solicitud de asesor: ${userText}` });
      await sendWhatsAppText(from, `Perfecto ✅ Te paso con un asesor para continuar.`);
      return res.sendStatus(200);
    }

    if (session.state === "post_booking" && session.lastVisit) {
      const v = session.lastVisit;
      if (wantsConfirm) {
        await sendWhatsAppText(
          from,
          `✅ ¡Confirmado!\n\n🏠 Propiedad: *${v.property_title || v.property_code || "—"}*\n📅 Fecha: ${formatDateInTZ(v.start, BUSINESS_TIMEZONE)}\n⏰ Hora: ${formatTimeInTZ(v.start, BUSINESS_TIMEZONE)}\n\nResponde:\n2) Reprogramar\n3) Cancelar`
        );
        return res.sendStatus(200);
      }

      if (wantsCancel) {
        await cancelVisitTool({ visit_id: v.visit_id, reason: userText });
        await sendWhatsAppText(from, `✅ Listo. Tu visita fue cancelada.\n\nSi deseas agendar otra, escribe *catálogo* y te muestro opciones.`);
        session.lastVisit = null;
        clearVisitFlow(session);
        return res.sendStatus(200);
      }

      if (wantsReschedule) {
        session.reschedule.active = true;
        session.reschedule.visit_id = v.visit_id;
        session.reschedule.phone = v.phone || String(from).replace(/[^\d]/g, "");
        session.reschedule.lead_name = v.lead_name || "";
        session.reschedule.property_id = v.property_id || "";
        session.reschedule.property_code = v.property_code || "";
        session.reschedule.property_title = v.property_title || "";
        session.reschedule.property_retailer_id = v.property_retailer_id || "";
        session.reschedule.zone_interest = v.zone_interest || "";
        session.reschedule.budget = v.budget || "";
        session.reschedule.category = v.category || "";

        session.selectedProperty =
          PROPERTY_BY_ID[v.property_id] ||
          PROPERTY_BY_RETAILER_ID[v.property_retailer_id] || {
            id: v.property_id,
            code: v.property_code,
            title: v.property_title,
            retailer_id: v.property_retailer_id,
            category: v.category,
          };

        session.state = "await_day";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;
        session.pendingName = null;
        session.pendingPhone = null;
        session.pendingZone = null;
        session.pendingBudget = null;

        await sendWhatsAppText(
          from,
          `Perfecto ✅ Vamos a reprogramar la visita de *${propertyPublicLabel(session.selectedProperty)}*.\n\n¿Para qué día te funciona?\nEj: "mañana", "viernes", "próximo martes".`
        );
        return res.sendStatus(200);
      }

      if (looksLikeNewVisit(tNorm)) {
        session.lastVisit = null;
        clearVisitFlow(session);
        await sendWhatsAppText(from, `Claro ✅ Vamos a agendar una nueva visita.`);
        await sendPropertyCategoriesList(from);
        return res.sendStatus(200);
      }

      if (isThanks(tNorm)) {
        await sendWhatsAppText(
          from,
          `¡Perfecto! ✅\nTu visita queda confirmada.\n\n🏠 Propiedad: *${v.property_title || v.property_code || "—"}*\n📅 Fecha: ${formatDateInTZ(v.start, BUSINESS_TIMEZONE)}\n⏰ Hora: ${formatTimeInTZ(v.start, BUSINESS_TIMEZONE)}\n\nSi necesitas *reprogramar* o *cancelar*, escríbelo aquí.`
        );
        return res.sendStatus(200);
      }

      await sendWhatsAppText(
        from,
        `Estoy aquí ✅\nSi deseas *reprogramar* o *cancelar* tu visita, responde:\n2) Reprogramar\n3) Cancelar\n\nSi deseas otra propiedad, escribe *catálogo*.`
      );
      return res.sendStatus(200);
    }

    if (
      session.selectedProperty &&
      !detectedPropertyEarly &&
      isPropertyQuestionLike &&
      !detectedRangeEarly &&
      !wantsCancel &&
      !wantsReschedule &&
      !wantsConfirm &&
      !wantsRestart
    ) {
      const faqAnswer = await answerPropertyQuestionWithAI(session.selectedProperty, userText);
      await sendWhatsAppText(from, `${faqAnswer}${buildSelectedPropertyNextStep(session)}`);
      return res.sendStatus(200);
    }

    if (session.state === "await_property_choice" && session.lastRecommendations?.length) {
      const pickedProperty = tryPickRecommendedPropertyFromUserText(session, userText);
      if (!pickedProperty) {
        await sendWhatsAppText(from, `Responde con el *número* o con el *nombre* de la propiedad que te interesa y te ayudo con la visita.`);
        return res.sendStatus(200);
      }

      session.selectedProperty = pickedProperty;
      session.pendingCategory = pickedProperty.category || null;
      session.lastRecommendations = [];
      session.state = "await_day";
      await reportLeadEventToCrm({
        to: from,
        action: "property_selected",
        property: pickedProperty,
      });
      await sendWhatsAppText(
        from,
        `${propertySummary(pickedProperty)}\n\nExcelente elección ✅\n¿Cuándo te gustaría visitar esta propiedad?\nEj: "mañana", "viernes", "14 de junio".`
      );
      return res.sendStatus(200);
    }

    if (session.state === "await_slot_choice" && session.lastSlots?.length) {
      if (["reiniciar", "reset", "resetear", "empezar", "inicio"].some((k) => tNorm.includes(k))) {
        session.lastVisit = null;
        clearVisitFlow(session);
        session.greeted = true;
        await sendWelcomeAndCatalog(from, welcomeText());
        return res.sendStatus(200);
      }

      if (["reprogramar", "cambiar", "otro dia", "otro día"].some((k) => tNorm.includes(k))) {
        session.state = "await_day";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;
        await sendWhatsAppText(
          from,
          `Perfecto ✅ Vamos a elegir *otro día* para *${propertyPublicLabel(session.selectedProperty)}*.\n\n¿Para qué día?`
        );
        return res.sendStatus(200);
      }

      const picked = tryPickSlotFromUserText(session, userText);
      if (!picked) {
        await sendWhatsAppText(
          from,
          `No entendí el horario 🙏\nResponde con el *número* (1,2,3...) o la *hora* (ej: 10:00 am / 3:00 pm).`
        );
        return res.sendStatus(200);
      }

      if (session.reschedule?.active && session.reschedule.visit_id) {
        const property =
          session.selectedProperty ||
          PROPERTY_BY_ID[session.reschedule.property_id] ||
          PROPERTY_BY_RETAILER_ID[session.reschedule.property_retailer_id] || {
            id: session.reschedule.property_id,
            code: session.reschedule.property_code,
            title: session.reschedule.property_title,
            retailer_id: session.reschedule.property_retailer_id,
            category: session.reschedule.category,
          };

        await rescheduleVisitTool({
          visit_id: session.reschedule.visit_id,
          new_slot_id: picked.slot_id,
          new_start: picked.start,
          new_end: picked.end,
          property,
          lead_name: session.reschedule.lead_name,
          phone: session.reschedule.phone || from,
          wa_id: from,
          zone_interest: session.reschedule.zone_interest,
          budget: session.reschedule.budget,
        });

        session.lastVisit = {
          visit_id: session.reschedule.visit_id,
          start: picked.start,
          end: picked.end,
          lead_name: session.reschedule.lead_name || "",
          phone: session.reschedule.phone || String(from).replace(/[^\d]/g, ""),
          property_id: property.id || "",
          property_code: property.code || "",
          property_title: property.title || "",
          property_retailer_id: property.retailer_id || "",
          category: property.category || "",
          zone_interest: session.reschedule.zone_interest || "",
          budget: session.reschedule.budget || "",
        };

        session.state = "post_booking";
        session.lastSlots = [];
        session.lastDisplaySlots = [];
        session.selectedSlot = null;
        session.pendingRange = null;
        session.pendingName = null;
        session.pendingPhone = null;
        session.pendingZone = null;
        session.pendingBudget = null;
        session.reschedule = defaultSession().reschedule;

        await sendWhatsAppText(
          from,
          `✅ *Visita reprogramada*\n\n🏠 Propiedad: *${propertyPublicLabel(property)}*\n📅 Fecha: *${formatDateInTZ(
            picked.start,
            BUSINESS_TIMEZONE
          )}*\n⏰ Hora: *${formatTimeInTZ(picked.start, BUSINESS_TIMEZONE)}*\n\nResponde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`
        );
        await reportLeadEventToCrm({
          to: from,
          action: "visit_rescheduled",
          property,
          lead_name: session.lastVisit.lead_name,
          phone: session.lastVisit.phone,
          zone_interest: session.lastVisit.zone_interest,
          budget: session.lastVisit.budget,
          visit_start: picked.start,
          visit_id: session.lastVisit.visit_id,
        });
        return res.sendStatus(200);
      }

      session.selectedSlot = picked;
      session.state = "await_name";
      await sendWhatsAppText(
        from,
        `Perfecto ✅ Queda seleccionado el horario ${formatTimeInTZ(
          picked.start,
          BUSINESS_TIMEZONE
        )}.\nAhora indícame tu *nombre completo* para reservar la visita.`
      );
      return res.sendStatus(200);
    }

    if (session.state === "await_name" && session.selectedSlot) {
      if (tNorm.length < 3 || ["si", "sí", "ok", "listo"].includes(tNorm)) {
        await sendWhatsAppText(from, `Por favor, envíame tu *nombre completo* 🙂`);
        return res.sendStatus(200);
      }
      session.pendingName = userText;
      session.state = "await_phone";
      await sendWhatsAppText(from, `Gracias. Ahora envíame tu *número de teléfono* (ej: 829XXXXXXX).`);
      return res.sendStatus(200);
    }

    if (session.state === "await_phone" && session.selectedSlot && session.pendingName) {
      const phoneDigits = userText.replace(/[^\d]/g, "");
      if (phoneDigits.length < 8) {
        await sendWhatsAppText(from, `Ese número parece incompleto 🙏\nEnvíame el teléfono así: 829XXXXXXX`);
        return res.sendStatus(200);
      }
      session.pendingPhone = phoneDigits;
      await finalizeVisitBookingAndNotify({ from, session });
      return res.sendStatus(200);
    }

    if (session.state === "await_zone" && session.selectedSlot && session.pendingName && session.pendingPhone) {
      await finalizeVisitBookingAndNotify({ from, session });
      return res.sendStatus(200);
    }

    if (session.state === "await_budget" && session.selectedSlot && session.pendingName && session.pendingPhone) {
      await finalizeVisitBookingAndNotify({ from, session });
      return res.sendStatus(200);
    }

    if (looksLikeCatalogRequest(tNorm)) {
      await sendPropertyCategoriesList(from);
      return res.sendStatus(200);
    }

    if (detectedCategoryEarly && !detectedPropertyEarly) {
      session.pendingCategory = detectedCategoryEarly;
      await sendCatalogForCategory(from, detectedCategoryEarly, session);
      return res.sendStatus(200);
    }

    if (detectedPropertyEarly) {
      session.selectedProperty = detectedPropertyEarly;
      session.pendingCategory = detectedPropertyEarly.category || null;
      session.lastRecommendations = [];

      await reportLeadEventToCrm({
        to: from,
        action: "property_selected",
        property: detectedPropertyEarly,
      });

      const range = parseDateRangeFromText(userText);

      if (!range) {
        session.state = "await_day";

        if (isPropertyQuestionLike) {
          const faqAnswer = await answerPropertyQuestionWithAI(detectedPropertyEarly, userText);
          await sendWhatsAppText(
            from,
            `${propertySummary(detectedPropertyEarly)}\n\n${faqAnswer}\n\nSi quieres verla en persona, dime qué día te gustaría visitarla.\nEj: "mañana", "viernes", "14 de junio".\n\nTambién puedes escribir *inicio* para volver al catálogo.`
          );
          return res.sendStatus(200);
        }

        await sendWhatsAppText(
          from,
          `${propertySummary(detectedPropertyEarly)}\n\nExcelente elección ✅\n¿Cuándo te gustaría visitar esta propiedad?\nEj: "mañana", "viernes", "14 de junio".\n\nTambién puedes escribir *inicio* para volver al catálogo.`
        );
        return res.sendStatus(200);
      }

      const slots = await getAvailableVisitSlotsTool({
        property: detectedPropertyEarly,
        from: range.from,
        to: range.to,
      });

      if (!slots.length) {
        session.state = "await_day";
        await sendWhatsAppText(
          from,
          `Reconocí la propiedad *${propertyPublicLabel(detectedPropertyEarly)}* ✅\nPero no veo espacios disponibles para ese rango.\nDime otro día.\n\nTambién puedes escribir *inicio* para volver al catálogo.`
        );
        return res.sendStatus(200);
      }

      session.pendingRange = range;
      session.lastSlots = slots;
      session.state = "await_slot_choice";

      await sendWhatsAppText(
        from,
        `${propertySummary(detectedPropertyEarly)}\n\n${formatSlotsList(detectedPropertyEarly, slots, session)}`
      );
      return res.sendStatus(200);
    }

    if (!detectedPropertyEarly && session.selectedProperty) {
      const range = parseDateRangeFromText(userText);

      if (!range && session.state === "await_day" && isPropertyQuestionLike) {
        const faqAnswer = await answerPropertyQuestionWithAI(session.selectedProperty, userText);
        await sendWhatsAppText(
          from,
          `${faqAnswer}\n\nSi quieres agendar la visita de *${propertyPublicLabel(
            session.selectedProperty
          )}*, dime el día.\nEj: "mañana", "viernes", "14 de junio".\n\nTambién puedes escribir *inicio* para volver al catálogo.`
        );
        return res.sendStatus(200);
      }

      if (range) {
        const slots = await getAvailableVisitSlotsTool({
          property: session.selectedProperty,
          from: range.from,
          to: range.to,
        });
        if (!slots.length) {
          await sendWhatsAppText(
            from,
            `No veo espacios disponibles para *${propertyPublicLabel(session.selectedProperty)}* en ese rango 🙏\nDime otro día.\n\nTambién puedes escribir *inicio* para volver al catálogo.`
          );
          return res.sendStatus(200);
        }
        session.pendingRange = range;
        session.lastSlots = slots;
        session.state = "await_slot_choice";
        await sendWhatsAppText(from, formatSlotsList(session.selectedProperty, slots, session));
        return res.sendStatus(200);
      }

      if (session.state === "await_day") {
        if (isPropertyQuestionLike) {
          const faqAnswer = await answerPropertyQuestionWithAI(session.selectedProperty, userText);
          await sendWhatsAppText(
            from,
            `${faqAnswer}\n\nSi quieres agendar la visita de *${propertyPublicLabel(
              session.selectedProperty
            )}*, dime el día.\nEj: "mañana", "viernes", "14 de junio".\n\nTambién puedes escribir *inicio* para volver al catálogo.`
          );
          return res.sendStatus(200);
        }

        await sendWhatsAppText(
          from,
          `Para elegir el día, puedes escribir: "mañana", "viernes", "próximo martes", "14 de junio" o "en junio".\n\nTambién puedes hacer una pregunta sobre esta propiedad o escribir *inicio* para volver al catálogo.`
        );
        return res.sendStatus(200);
      }
    }

    if (looksLikeHuman(tNorm)) {
      await reportLeadEventToCrm({
        to: from,
        action: "human_requested",
        property: session.selectedProperty,
        lead_name: session.pendingName || session.lastVisit?.lead_name || "",
        phone: session.pendingPhone || session.lastVisit?.phone || "",
        zone_interest: session.pendingZone || session.aiProfile?.zone_interest || session.lastVisit?.zone_interest || "",
        budget: session.pendingBudget || session.aiProfile?.budget || session.lastVisit?.budget || "",
      });
      await sendWhatsAppText(
        from,
        `Perfecto ✅ Voy a dejar tu caso listo para que un asesor te continúe ayudando. Mientras tanto, también puedo recomendarte propiedades o agendar una visita si ya viste una que te interese en el catálogo.`
      );
      return res.sendStatus(200);
    }

    const advisor = await maybeHandleAdvisorSearch({ session, userText });
    if (advisor?.handled) {
      if (advisor?.property) {
        await reportLeadEventToCrm({
          to: from,
          action: "property_selected",
          property: advisor.property,
          zone_interest: session.aiProfile?.zone_interest || "",
          budget: session.aiProfile?.budget || "",
        });
      }
      await sendWhatsAppText(from, advisor.message);
      return res.sendStatus(200);
    }

    const fallback = await callOpenAIFallback({
      session,
      userText,
      extraSystem: session.selectedProperty ? `La propiedad seleccionada actualmente es ${propertyLabel(session.selectedProperty)}.` : "",
    });

    await sendWhatsAppText(from, fallback);
    return res.sendStatus(200);
  } catch (e) {
    console.error("Webhook error:", e?.response?.data || e?.message || e);
    return res.sendStatus(200);
  } finally {
    try {
      if (from && session) await saveSession(from, session);
    } catch (e) {
      console.error("saveSession error:", e?.message || e);
    }
  }
});

app.get("/", (_req, res) => res.send("OK"));
app.get("/health", (_req, res) => res.status(200).send("ok"));

async function reminderLoop() {
  try {
    const calendar = getCalendarClient();
    const now = new Date();
    const in26h = addMinutes(now, 26 * 60);

    const list = await calendar.events.list({
      calendarId: GOOGLE_CALENDAR_ID,
      timeMin: now.toISOString(),
      timeMax: in26h.toISOString(),
      singleEvents: true,
      orderBy: "startTime",
      maxResults: 50,
    });

    const events = list.data.items || [];
    for (const ev of events) {
      const priv = ev.extendedProperties?.private || {};
      if (priv.status === "cancelled") continue;

      const phone = priv.wa_phone;
      const startISO = ev.start?.dateTime;
      if (!phone || !startISO) continue;

      const start = new Date(startISO);
      const minutesToStart = Math.round((start.getTime() - now.getTime()) / 60000);
      const in24hWindow = minutesToStart <= 25 * 60 && minutesToStart >= 23 * 60;
      const in2hWindow = minutesToStart <= 135 && minutesToStart >= 90;
      const propertyText = priv.property_title || priv.property_code || "tu propiedad";

      if (REMINDER_24H && in24hWindow && priv.reminder24hSent !== "true") {
        const msg = `Recordatorio 🏡: tienes una visita mañana a las ${formatTimeInTZ(startISO, BUSINESS_TIMEZONE)} para *${propertyText}*.\n\nResponde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`;
        const sendRes = await sendReminderWhatsAppToBestTarget(priv, phone, msg);
        if (sendRes.ok) {
          await calendar.events.patch({
            calendarId: GOOGLE_CALENDAR_ID,
            eventId: ev.id,
            requestBody: { extendedProperties: { private: { ...priv, reminder24hSent: "true" } } },
          });
        }
      }

      if (REMINDER_2H && in2hWindow && priv.reminder2hSent !== "true") {
        const msg = `Recordatorio 🏡: tu visita es hoy a las ${formatTimeInTZ(startISO, BUSINESS_TIMEZONE)} para *${propertyText}*.\nDirección: ${BUSINESS_ADDRESS || "—"}\n\nResponde:\n1) Confirmar\n2) Reprogramar\n3) Cancelar`;
        const sendRes = await sendReminderWhatsAppToBestTarget(priv, phone, msg);
        if (sendRes.ok) {
          await calendar.events.patch({
            calendarId: GOOGLE_CALENDAR_ID,
            eventId: ev.id,
            requestBody: { extendedProperties: { private: { ...priv, reminder2hSent: "true" } } },
          });
        }
      }
    }
  } catch (e) {
    console.error("Reminder loop error:", e?.response?.data || e?.message || e);
  }
}

app.get("/tick", async (_req, res) => {
  try {
    await reminderLoop();
  } catch {}
  return res.status(200).send("tick ok");
});

async function startServer() {
  try {
    await catalogAdmin.init();
  } catch (e) {
    console.error("Catalog admin init error:", e?.message || e);
  }

  app.listen(PORT, () => console.log(`Bot running on :${PORT}`));
}

startServer();
