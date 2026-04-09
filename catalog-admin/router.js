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

function digitsOnly(value) {
  return String(value || "").replace(/[^\d]/g, "");
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
  const n = Number(String(t).replace(/,/g, "."));
  return Number.isFinite(n) ? n : t;
}

function parseMaybeBoolean(value) {
  if (value === true || value === false) return value;
  const t = normalizeText(value);
  if (!t) return "";
  if (["si", "sí", "true", "1", "yes", "aplica", "incluye", "tiene"].includes(t)) return true;
  if (["no", "false", "0", "none", "no aplica", "no tiene"].includes(t)) return false;
  return cleanText(value);
}

function slugify(value, maxLen = 28) {
  const slug = normalizeText(value)
    .replace(/[^a-z0-9 ]/g, " ")
    .replace(/\s+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, maxLen)
    .replace(/-$/g, "");
  return slug || "propiedad";
}

function toTitleCase(value) {
  return String(value || "")
    .toLowerCase()
    .replace(/(^|\s|[-/])([a-záéíóúñ])/g, (_m, p1, p2) => `${p1}${p2.toUpperCase()}`)
    .trim();
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
  const direct = Number(String(raw).replace(/,/g, "."));
  if (Number.isFinite(direct)) return direct;
  const word = parseWordNumber(raw);
  if (word !== null) return word;
  const match = raw.match(/(\d+(?:[.,]\d+)?)/);
  if (!match) return "";
  const n = Number(match[1].replace(/,/g, "."));
  return Number.isFinite(n) ? n : "";
}

function parsePriceNumber(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";
  let text = normalizeText(raw);
  const hasMillion = text.includes("millon") || text.includes("millones") || /\bmm\b/.test(text);
  const hasThousand = /\bk\b/.test(text) || text.includes(" mil") || /^mil\b/.test(text);
  text = text
    .replace(/rd\$/g, "")
    .replace(/us\$/g, "")
    .replace(/usd/g, "")
    .replace(/dop/g, "")
    .replace(/,/g, "");
  const match = text.match(/\d+(?:\.\d+)?/);
  if (!match) return "";
  let n = Number(match[0]);
  if (!Number.isFinite(n)) return "";
  if (hasMillion) n *= 1000000;
  else if (hasThousand && n < 100000) n *= 1000;
  return String(Math.round(n));
}

function formatCatalogPrice(value, currency = "DOP") {
  const digits = parsePriceNumber(value);
  if (!digits) return cleanText(value);
  const prefix = String(currency || "DOP").toUpperCase() === "USD" ? "US$" : "RD$";
  return `${prefix}${Number(digits).toLocaleString("es-DO")}`;
}

function detectOperationFromText(value, fallback = "venta") {
  const t = normalizeText(value);
  if (t.includes("alquila") || t.includes("alquiler") || t.includes("renta") || t.includes("rent")) return "alquiler";
  if (t.includes("vende") || t.includes("venta") || t.includes("sale") || t.includes("comprar")) return "venta";
  return cleanText(fallback || "venta") || "venta";
}

function detectCategoryFromText(value, fallback = "apartamentos") {
  const t = normalizeText(value);
  if (t.includes("solar")) return "solares";
  if (t.includes("local") || t.includes("nave")) return "locales_comerciales";
  if (t.includes("proyecto") && !t.includes("apartamento")) return "proyectos";
  if (t.includes("casa")) return "casas";
  if (t.includes("apartaestudio") || t.includes("apartastudio") || t.includes("aparta estudio")) return "apartamentos";
  if (t.includes("apartamento") || t.includes("apto") || t.includes("aparta")) return "apartamentos";
  return cleanText(fallback || "apartamentos") || "apartamentos";
}

function categoryAbbr(category) {
  const key = normalizeText(category || "apartamentos");
  if (key === "apartamentos") return "APT";
  if (key === "casas") return "CASA";
  if (key === "solares") return "SOLAR";
  if (key === "locales_comerciales") return "LOCAL";
  if (key === "proyectos") return "PROY";
  return "PROP";
}

function operationAbbr(operation) {
  return normalizeText(operation || "venta") === "alquiler" ? "ALQUILER" : "VENTA";
}

function detectCurrency(value, fallback = "DOP") {
  const t = normalizeText(value);
  if (t.includes("us$") || t.includes("usd") || t.includes("dolar")) return "USD";
  if (t.includes("rd$") || t.includes("dop") || t.includes("peso")) return "DOP";
  return cleanText(fallback || "DOP") || "DOP";
}

function extractMetric(text, patterns = []) {
  const raw = String(text || "");
  for (const pattern of patterns) {
    const match = raw.match(pattern);
    if (match?.[1]) {
      const parsed = parseFlexibleNumber(match[1]);
      if (parsed !== "") return parsed;
    }
  }
  return "";
}

function cleanLineForParsing(value) {
  return cleanText(String(value || "").replace(/[\u{1F300}-\u{1FAFF}]/gu, " ").replace(/[✅✔️•▪■▫◆◇⭐✨📍📌☎️📞📲🏠🏡🏢🛏🛁🚗📐💰📝🔹🔸]/gu, " ").replace(/\s+/g, " "));
}

function stripLinkLines(lines = []) {
  return lines.filter((line) => !/^https?:\/\//i.test(cleanText(line)) && !normalizeText(line).includes("instagram.com") && !normalizeText(line).includes("whatsapp.com/channel") && !normalizeText(line).includes("lvinmobiliarias.com") && !normalizeText(line).includes("e-mail"));
}

function extractTitleFromText(raw, operation, category, location) {
  const lines = stripLinkLines(String(raw || "").split(/\r?\n/g).map((line) => cleanText(line)).filter(Boolean));
  const candidate = lines.find((line) => {
    const t = normalizeText(line);
    return t.includes("lv inmobiliaria") || t.includes("vende") || t.includes("alquila") || t.includes("venta") || t.includes("alquiler");
  }) || lines[0] || "";

  let title = cleanLineForParsing(candidate)
    .replace(/^lv inmobiliaria\s*/i, "")
    .replace(/^(vende|venta|alquila|alquiler)\s*/i, "")
    .replace(/^[:|\-]+/, "")
    .trim();

  if (!title) {
    title = category === "casas" ? "Casa" : category === "solares" ? "Solar" : category === "locales_comerciales" ? "Local comercial" : category === "proyectos" ? "Proyecto inmobiliario" : "Apartamento";
  }

  if (location && !normalizeText(title).includes(normalizeText(location))) {
    title = `${title} en ${location}`;
  }

  return toTitleCase(title.replace(/\s+/g, " "));
}

function extractLocationFromText(raw, fallback = "") {
  const lines = stripLinkLines(String(raw || "").split(/\r?\n/g).map((line) => cleanText(line)).filter(Boolean));
  const byPin = lines.find((line) => /📍/.test(line) || /resd\.|residencial|sector|urbanizaci[oó]n|villa|pont[oó]n|jerem[ií]as|la vega|arenoso|ciudad/i.test(normalizeText(line)));
  if (byPin) {
    const cleaned = cleanLineForParsing(byPin)
      .replace(/^(ubicacion|ubicación)\s*:?/i, "")
      .trim();
    if (cleaned && cleaned.length <= 120) return toTitleCase(cleaned);
  }

  const bodyMatch = String(raw || "").match(/(?:ubicaci[oó]n|sector|resd\.|residencial)[:\s-]*([^\n]+)/i);
  if (bodyMatch?.[1]) return toTitleCase(cleanLineForParsing(bodyMatch[1]));
  return cleanText(fallback);
}

function extractPriceFromText(raw, currency = "DOP", fallback = "") {
  const patterns = [
    /(precio[^\n:]*[:\s]+[^\n]+)/i,
    /(pagar[^\n:]*[:\s]+[^\n]+)/i,
    /((?:rd\$|us\$|usd)\s*[\d.,]+(?:\s*(?:mil|millones|millon))?)/i,
  ];

  for (const pattern of patterns) {
    const match = String(raw || "").match(pattern);
    if (match?.[1]) {
      const numeric = parsePriceNumber(match[1]);
      if (numeric) return formatCatalogPrice(match[1], detectCurrency(match[1], currency));
    }
  }

  if (fallback) return cleanText(fallback);
  return "";
}

function extractAgentName(raw, fallback = "") {
  const match = String(raw || "").match(/agente\s+inmobiliari[ao]\s*[-:–]?\s*([^\n]+)/i);
  if (match?.[1]) return cleanText(match[1]);
  return cleanText(fallback);
}

function extractAgentPhone(raw, fallback = "") {
  const match = String(raw || "").match(/(?:\+?1[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}/);
  if (match?.[0]) return cleanText(match[0]);
  return cleanText(fallback);
}

function splitSectionsFromText(raw) {
  const text = String(raw || "").replace(/\r/g, "");
  const lines = text.split("\n");
  const sections = { featureLines: [], requirementLines: [] };
  let inRequirements = false;

  for (const originalLine of lines) {
    const line = cleanText(originalLine);
    if (!line) continue;
    const norm = normalizeText(line);
    if (norm.startsWith("requisitos")) {
      inRequirements = true;
      continue;
    }
    if (norm.includes("informacion y alquiler") || norm.includes("información y alquiler") || norm.includes("agente inmobiliaria") || norm.includes("seguir canal")) {
      inRequirements = false;
      continue;
    }
    if (/^https?:\/\//i.test(line) || norm.includes("instagram.com") || norm.includes("lvinmobiliarias.com") || norm.includes("whatsapp.com/channel") || norm.includes("e-mail")) {
      continue;
    }
    if (inRequirements) sections.requirementLines.push(line);
    else sections.featureLines.push(line);
  }

  return sections;
}

function extractFeaturesFromText(raw, category = "") {
  const { featureLines } = splitSectionsFromText(raw);
  const ignored = [
    "lv inmobiliaria",
    "precio",
    "pagar",
    "requisitos",
    "descripcion del inmueble",
    "descripción del inmueble",
    "informacion y alquiler",
    "información y alquiler",
    "agente inmobiliaria",
    "seguir canal",
  ];

  const featureSet = new Set();
  for (const line of featureLines) {
    const cleaned = cleanLineForParsing(line)
      .replace(/^descripcion del inmueble$/i, "")
      .replace(/^descripci[oó]n$/i, "")
      .trim();
    const norm = normalizeText(cleaned);
    if (!cleaned) continue;
    if (ignored.some((k) => norm.startsWith(k))) continue;
    if (norm.includes("lv inmobiliaria")) continue;
    if (norm.includes("precio") || norm.includes("pagar") || norm.includes("trabajo estable")) continue;
    if (cleaned.length < 3 || cleaned.length > 140) continue;
    featureSet.add(cleaned);
  }

  const fallbackFeature = category === "solares" ? "Solar disponible" : category === "locales_comerciales" ? "Local comercial disponible" : category === "casas" ? "Casa disponible" : "Propiedad disponible";
  return [...featureSet].slice(0, 40).filter(Boolean).length ? [...featureSet].slice(0, 40) : [fallbackFeature];
}

function buildShortDescriptionFromText(raw, features = [], fallback = "") {
  const featurePreview = [...features].slice(0, 8).join(", ");
  const location = extractLocationFromText(raw, "");
  const operation = detectOperationFromText(raw, "venta");
  const category = detectCategoryFromText(raw, "apartamentos");
  const categoryLabel = category === "casas" ? "Casa" : category === "solares" ? "Solar" : category === "locales_comerciales" ? "Local comercial" : category === "proyectos" ? "Proyecto" : "Apartamento";
  const opLabel = operation === "alquiler" ? "en alquiler" : "en venta";
  const base = `${categoryLabel} ${opLabel}${location ? ` en ${location}` : ""}.`;
  const final = `${base}${featurePreview ? ` Incluye ${featurePreview}.` : ""}`.replace(/\s+/g, " ").trim();
  return final.length > 18 ? final : cleanText(fallback);
}

function extractRequirementsText(raw, fallback = "") {
  const { requirementLines } = splitSectionsFromText(raw);
  const joined = requirementLines.map((line) => cleanLineForParsing(line)).filter(Boolean).join("\n");
  return cleanText(joined || fallback || "");
}

function detectFloorLevel(raw, fallback = "") {
  const match = String(raw || "").match(/(1er|primer|2do|segundo|3er|tercer|4to|cuarto|5to|quinto)\s+nivel/i);
  return cleanText(match?.[0] || fallback);
}

function buildIdentifiersForProperty(property = {}, existingItems = [], currentId = "") {
  if (cleanText(property.id) && cleanText(property.retailer_id) && cleanText(property.code)) {
    return { id: cleanText(property.id), retailer_id: cleanText(property.retailer_id), code: cleanText(property.code) };
  }

  const op = operationAbbr(property.operation || "venta");
  const cat = categoryAbbr(property.category || "apartamentos");
  const baseSlug = slugify(`${property.location || property.title || property.category || "propiedad"}`.replace(/\b(resd|residencial|sector|urbanizacion|urbanización)\b/gi, ""), 24)
    .toUpperCase()
    .replace(/-/g, "");

  const existingCodes = new Set(
    existingItems
      .filter((item) => !currentId || cleanText(item.id) !== cleanText(currentId))
      .map((item) => normalizeText(item.retailer_id || item.code || item.id))
  );

  let index = 1;
  let candidate = "";
  while (index < 9999) {
    candidate = `LV-${op}-${cat}-${baseSlug || "PROP"}-${String(index).padStart(3, "0")}`;
    if (!existingCodes.has(normalizeText(candidate))) break;
    index += 1;
  }

  return { id: candidate, retailer_id: candidate, code: candidate };
}

function normalizeMediaGallery(raw = {}) {
  const galleryFromRaw = Array.isArray(raw.media_gallery)
    ? raw.media_gallery
    : safeJson(raw.media_gallery, null);

  const imageList = parseList(raw.image_urls || raw.meta_additional_image_urls || []);
  const videoList = parseList(raw.video_urls || raw.meta_video_urls || []);
  const primaryImageUrl = cleanText(raw.primary_image_url || raw.meta_image_url || "");

  let gallery = [];
  if (Array.isArray(galleryFromRaw)) {
    gallery = galleryFromRaw.map((item, index) => ({
      id: cleanText(item?.id || `${index + 1}`),
      url: cleanText(item?.url || item),
      type: normalizeText(item?.type || "image") === "video" ? "video" : "image",
      primary: item?.primary === true || cleanText(item?.url || item) === primaryImageUrl,
    })).filter((item) => item.url);
  }

  if (!gallery.length) {
    const dedupe = new Set();
    const pushItem = (url, type, primary = false) => {
      const key = `${type}|${url}`;
      if (!url || dedupe.has(key)) return;
      dedupe.add(key);
      gallery.push({ id: String(gallery.length + 1), url, type, primary });
    };

    if (primaryImageUrl) pushItem(primaryImageUrl, "image", true);
    imageList.forEach((url) => pushItem(cleanText(url), "image", cleanText(url) === primaryImageUrl));
    videoList.forEach((url) => pushItem(cleanText(url), "video", false));
  }

  if (gallery.length && !gallery.some((item) => item.type === "image" && item.primary)) {
    const firstImage = gallery.find((item) => item.type === "image");
    if (firstImage) firstImage.primary = true;
  }

  const imageUrls = gallery.filter((item) => item.type === "image").map((item) => item.url);
  const videoUrls = gallery.filter((item) => item.type === "video").map((item) => item.url);
  const primaryImage = gallery.find((item) => item.type === "image" && item.primary) || gallery.find((item) => item.type === "image") || null;

  return {
    media_gallery: gallery,
    image_urls: imageUrls,
    video_urls: videoUrls,
    primary_image_url: primaryImage?.url || "",
    meta_image_url: cleanText(raw.meta_image_url || primaryImage?.url || ""),
    meta_additional_image_urls: imageUrls.filter((url) => url !== (primaryImage?.url || "")),
    meta_video_urls: videoUrls,
  };
}

function normalizeProperty(raw, index = 0, existingItems = [], currentId = "") {
  const nowIso = new Date().toISOString();
  const base = {
    title: cleanText(raw?.title || raw?.name || ""),
    category: cleanText(raw?.category || raw?.type || "apartamentos") || "apartamentos",
    operation: cleanText(raw?.operation || "venta") || "venta",
    location: cleanText(raw?.location || raw?.zone || ""),
    retailer_id: cleanText(raw?.retailer_id || raw?.product_retailer_id || raw?.code || raw?.id || ""),
    code: cleanText(raw?.code || raw?.retailer_id || raw?.product_retailer_id || raw?.id || ""),
    id: cleanText(raw?.id || raw?.retailer_id || raw?.product_retailer_id || raw?.code || `prop_${index + 1}`),
  };

  const ids = buildIdentifiersForProperty(base, existingItems, currentId);
  const media = normalizeMediaGallery(raw || {});

  return {
    id: cleanText(base.id || ids.id),
    retailer_id: cleanText(base.retailer_id || ids.retailer_id),
    product_retailer_id: cleanText(base.retailer_id || ids.retailer_id),
    code: cleanText(base.code || ids.code),
    title: cleanText(base.title || ids.code),
    category: base.category,
    operation: base.operation,
    price: cleanText(raw?.price || ""),
    currency: cleanText(raw?.currency || "DOP") || "DOP",
    location: base.location,
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
    faq: raw?.faq && typeof raw.faq === "object" ? raw.faq : safeJson(raw?.faq, {}) || {},
    status: cleanText(raw?.status || "available") || "available",
    duration_min: parseMaybeNumber(raw?.duration_min || ""),
    active: raw?.active !== false && raw?.active !== "false",
    agent_name: cleanText(raw?.agent_name || ""),
    agent_phone: cleanText(raw?.agent_phone || ""),
    meta_url: cleanText(raw?.meta_url || raw?.url || ""),
    meta_availability: cleanText(raw?.meta_availability || "in stock") || "in stock",
    raw_post_text: cleanText(raw?.raw_post_text || raw?.source_text || ""),
    requirements_text: cleanText(raw?.requirements_text || ""),
    cloudinary_folder: cleanText(raw?.cloudinary_folder || ""),
    ...media,
    updated_at: cleanText(raw?.updated_at || nowIso),
    created_at: cleanText(raw?.created_at || nowIso),
  };
}

function getPropertyMergeKey(item = {}) {
  return normalizeText(item?.retailer_id || item?.product_retailer_id || item?.code || item?.id || "");
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
  const map = new Map();
  for (const item of Array.isArray(items) ? items : []) {
    const normalized = normalizeProperty(item, map.size, [...map.values()], cleanText(item?.id || ""));
    const key = getPropertyMergeKey(normalized);
    if (!key) continue;
    map.set(key, normalized);
  }
  return sortProperties([...map.values()]);
}

function mergePropertyCollections(baseItems = [], overlayItems = []) {
  const map = new Map();
  const upsert = (item) => {
    const normalized = normalizeProperty(item, map.size, [...map.values()], cleanText(item?.id || ""));
    const key = getPropertyMergeKey(normalized);
    if (!key) return;
    const current = map.get(key);
    if (!current) {
      map.set(key, normalized);
      return;
    }
    map.set(key, normalizeProperty({ ...current, ...normalized, created_at: current.created_at || normalized.created_at }, map.size, [...map.values()], current.id));
  };
  dedupeProperties(baseItems).forEach(upsert);
  dedupeProperties(overlayItems).forEach(upsert);
  return sortProperties([...map.values()]);
}

function isPlaceholderProperty(item = {}) {
  const key = getPropertyMergeKey(item);
  const title = normalizeText(item?.title || "");
  return key === "test-1" || title === "prueba";
}

function buildMetaName(property) {
  return `${normalizeText(property.operation || "").includes("alquiler") ? "ALQUILER" : "VENTA"} | ${cleanText(property.title || property.code || property.id)}`.slice(0, 150);
}

function formatMetaDescription(property) {
  const parts = [];
  const operation = normalizeText(property.operation || "").includes("alquiler") ? "alquiler" : "venta";
  parts.push(`Propiedad en ${operation}.`);
  if (property.short_description) parts.push(cleanText(property.short_description));
  if (property.location) parts.push(`Ubicación: ${cleanText(property.location)}.`);
  if (property.requirements_text) parts.push(`Requisitos: ${cleanText(property.requirements_text).replace(/\n+/g, "; ")}.`);
  return parts.join(" ").replace(/\s+/g, " ").trim().slice(0, 5000);
}

function buildMetaPayload(property, defaults = {}, options = {}) {
  const gallery = normalizeMediaGallery(property);
  const extraImages = [...gallery.meta_additional_image_urls].filter(Boolean);
  return {
    retailer_id: cleanText(property.retailer_id || property.code || property.id),
    name: buildMetaName(property),
    description: formatMetaDescription(property),
    price: parsePriceNumber(property.price),
    currency: cleanText(property.currency || "DOP") || "DOP",
    availability: cleanText(property.meta_availability || defaults.metaAvailability || "in stock") || "in stock",
    url: cleanText(property.meta_url || defaults.metaUrl || ""),
    image_url: cleanText(property.meta_image_url || gallery.meta_image_url || defaults.metaImageUrl || ""),
    ...(extraImages.length ? { additional_image_urls: JSON.stringify(extraImages.slice(0, 20)) } : {}),
    ...(options.extraMetaFields || {}),
  };
}

function computeStats(items = []) {
  const total = items.length;
  const active = items.filter((p) => p.active !== false).length;
  const alquiler = items.filter((p) => normalizeText(p.operation) === "alquiler").length;
  const venta = items.filter((p) => normalizeText(p.operation) === "venta").length;
  const withMedia = items.filter((p) => Array.isArray(p.image_urls) && p.image_urls.length).length;
  const metaReady = items.filter((p) => {
    const payload = buildMetaPayload(p, {}, {});
    return payload.url && payload.image_url && payload.price;
  }).length;
  return { total, active, alquiler, venta, withMedia, metaReady };
}

function serializeCatalogJson(items = []) {
  return JSON.stringify(
    items.map((item) => ({ ...item, product_retailer_id: item.retailer_id })),
    null,
    2
  );
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

function sanitizeMetaImportedDescription(value = "") {
  return cleanText(String(value || "").replace(/\bpropiedad en (venta|alquiler)\.?/gi, "").replace(/\s+/g, " "));
}

function normalizeMetaUrlArray(value) {
  if (Array.isArray(value)) return value.map((item) => cleanText(item?.url || item)).filter(Boolean);
  const parsed = typeof value === "string" ? safeJson(value, null) : null;
  if (Array.isArray(parsed)) return parsed.map((item) => cleanText(item?.url || item)).filter(Boolean);
  if (parsed && typeof parsed === "object") {
    return Object.values(parsed).map((item) => cleanText(item?.url || item)).filter(Boolean);
  }
  return parseList(value).map((item) => cleanText(item)).filter(Boolean);
}

function uniqueUrlList(...groups) {
  const seen = new Set();
  const out = [];
  for (const group of groups) {
    for (const item of normalizeMetaUrlArray(group)) {
      const key = cleanText(item);
      if (!key || seen.has(key)) continue;
      seen.add(key);
      out.push(key);
    }
  }
  return out;
}

function buildImportedPropertyFromMetaProduct(product = {}, existing = null, currentItems = []) {
  const description = cleanText(product?.description || existing?.short_description || "");
  const operation = detectOperationFromText(`${product?.name || ""} ${description}`, existing?.operation || "venta");
  const category = detectCategoryFromText(`${product?.name || ""} ${description}`, existing?.category || "apartamentos");
  const location = extractLocationFromText(description, existing?.location || "");
  const title = extractTitleFromText(product?.name || existing?.title || "", operation, category, location);
  const primaryMetaImage = cleanText(product?.image_url || existing?.meta_image_url || existing?.primary_image_url || "");
  const additionalMetaImages = uniqueUrlList(
    product?.additional_image_urls,
    product?.additional_image_cdn_urls,
    product?.additional_images,
    existing?.meta_additional_image_urls
  ).filter((url) => url !== primaryMetaImage);
  const mergedImages = uniqueUrlList(primaryMetaImage ? [primaryMetaImage] : [], additionalMetaImages, existing?.image_urls);

  const base = {
    ...(existing || {}),
    id: cleanText(existing?.id || product?.retailer_id || product?.id || ""),
    retailer_id: cleanText(product?.retailer_id || existing?.retailer_id || existing?.code || existing?.id || ""),
    code: cleanText(existing?.code || product?.retailer_id || product?.id || ""),
    title,
    operation,
    category,
    currency: cleanText(product?.currency || existing?.currency || "DOP") || "DOP",
    price: formatCatalogPrice(product?.price || existing?.price || "", product?.currency || existing?.currency || "DOP"),
    location,
    short_description: sanitizeMetaImportedDescription(description) || cleanText(existing?.short_description || ""),
    meta_url: cleanText(product?.url || existing?.meta_url || ""),
    meta_image_url: primaryMetaImage,
    meta_additional_image_urls: additionalMetaImages,
    meta_availability: cleanText(product?.availability || existing?.meta_availability || "in stock") || "in stock",
    bedrooms: existing?.bedrooms !== undefined && existing?.bedrooms !== "" ? existing.bedrooms : extractMetric(description, [/([\d.,]+)\s*(?:hab|habitaciones?|cuartos?)/i]),
    bathrooms: existing?.bathrooms !== undefined && existing?.bathrooms !== "" ? existing.bathrooms : extractMetric(description, [/([\d.,]+)\s*(?:baños?|banos?)/i]),
    parking: existing?.parking !== undefined && existing?.parking !== "" ? existing.parking : extractMetric(description, [/([\d.,]+)\s*(?:parqueos?|vehiculos?|vehículos?|marquesinas?)/i]),
    area_m2: existing?.area_m2 !== undefined && existing?.area_m2 !== "" ? existing.area_m2 : extractMetric(description, [/([\d.,]+)\s*(?:m2|mt2|mts2|m²)/i]),
    agent_phone: cleanText(existing?.agent_phone || extractAgentPhone(description, "")),
    agent_name: cleanText(existing?.agent_name || extractAgentName(description, "")),
    image_urls: mergedImages,
    primary_image_url: cleanText(existing?.primary_image_url || primaryMetaImage || mergedImages[0] || ""),
    video_urls: existing?.video_urls || [],
    updated_at: new Date().toISOString(),
    created_at: cleanText(existing?.created_at || new Date().toISOString()),
  };
  return normalizeProperty(base, 0, currentItems, cleanText(existing?.id || base.id));
}

function parseListingTextToProperty(rawText = "", current = {}, existingItems = []) {
  const raw = cleanText(rawText || current?.raw_post_text || "");
  const operation = detectOperationFromText(raw, current?.operation || "venta");
  const category = detectCategoryFromText(raw, current?.category || "apartamentos");
  const location = extractLocationFromText(raw, current?.location || "");
  const title = extractTitleFromText(raw, operation, category, location || current?.location || "");
  const currency = detectCurrency(raw, current?.currency || "DOP");
  const features = extractFeaturesFromText(raw, category);
  const shortDescription = buildShortDescriptionFromText(raw, features, current?.short_description || "");
  const requirementsText = extractRequirementsText(raw, current?.requirements_text || "");

  const partial = {
    ...current,
    raw_post_text: raw,
    title,
    operation,
    category,
    location: location || current?.location || "",
    price: extractPriceFromText(raw, currency, current?.price || ""),
    currency,
    bedrooms: extractMetric(raw, [/([\d.,]+)\s*(?:hab|habitaciones?|cuartos?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez)\s*(?:hab|habitaciones?|cuartos?)/i]) || current?.bedrooms || "",
    bathrooms: extractMetric(raw, [/([\d.,]+)\s*(?:baños?|banos?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis|siete|ocho|nueve|diez|medio|media)\s*(?:baños?|banos?)/i]) || current?.bathrooms || "",
    parking: extractMetric(raw, [/([\d.,]+)\s*(?:parqueos?|vehículos?|vehiculos?|marquesinas?)/i, /\b(un|uno|una|dos|tres|cuatro|cinco|seis)\s*(?:parqueos?|vehículos?|vehiculos?|marquesinas?)/i]) || current?.parking || "",
    area_m2: extractMetric(raw, [/([\d.,]+)\s*(?:m2|mt2|mts2|m²)/i]) || current?.area_m2 || "",
    lot_m2: extractMetric(raw, [/([\d.,]+)\s*mts?2\s*de\s*solar/i]) || current?.lot_m2 || "",
    construction_m2: extractMetric(raw, [/([\d.,]+)\s*mts?2\s*de\s*construcci[oó]n/i]) || current?.construction_m2 || "",
    floor_level: detectFloorLevel(raw, current?.floor_level || ""),
    short_description: shortDescription,
    features,
    requirements_text: requirementsText,
    agent_phone: extractAgentPhone(raw, current?.agent_phone || ""),
    agent_name: extractAgentName(raw, current?.agent_name || ""),
    meta_url: cleanText(current?.meta_url || ""),
    meta_image_url: cleanText(current?.meta_image_url || current?.primary_image_url || ""),
  };

  const normalized = normalizeProperty(partial, 0, existingItems, cleanText(current?.id || ""));
  return normalized;
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

  router.use(express.json({ limit: "8mb" }));
  router.use(express.urlencoded({ extended: true, limit: "8mb" }));

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
    res.setHeader("Set-Cookie", `${SESSION_COOKIE}=${encodeURIComponent(token)}; Path=${ADMIN_BASE_PATH}; HttpOnly; SameSite=Lax; Max-Age=${maxAge}`);
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
    if (!RENDER_API_KEY || !RENDER_BOT_SERVICE_ID) {
      return { ok: true, skipped: true, message: "Sin credenciales Render" };
    }

    const headers = { Authorization: `Bearer ${RENDER_API_KEY}`, "Content-Type": "application/json" };
    const attempts = [
      { method: "patch", url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}`, body: { envVars: [{ key: RENDER_BOT_ENV_KEY, value: catalogJson }] } },
      { method: "patch", url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars`, body: [{ key: RENDER_BOT_ENV_KEY, value: catalogJson }] },
      { method: "put", url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars/${encodeURIComponent(RENDER_BOT_ENV_KEY)}`, body: { value: catalogJson } },
      { method: "post", url: `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/env-vars`, body: { key: RENDER_BOT_ENV_KEY, value: catalogJson } },
    ];

    let lastError = null;
    for (const attempt of attempts) {
      try {
        const res = await axios({ method: attempt.method, url: attempt.url, headers, data: attempt.body, timeout: 25000, validateStatus: () => true });
        if (res.status >= 200 && res.status < 300) return { ok: true, response: res.data, attempt: `${attempt.method.toUpperCase()} ${attempt.url}` };
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
    for (const url of [`${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/deploys`, `${RENDER_API_BASE_URL}/services/${RENDER_BOT_SERVICE_ID}/deploy`]) {
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
    const makeParams = (data) => {
      const params = new URLSearchParams();
      Object.entries(data).forEach(([key, value]) => {
        if (value !== undefined && value !== null && String(value).trim() !== "") params.append(key, String(value));
      });
      return params;
    };

    const attempts = [payload];
    if (payload.additional_image_urls) {
      const fallback = { ...payload };
      delete fallback.additional_image_urls;
      attempts.push(fallback);
    }

    let lastError = null;
    for (const attempt of attempts) {
      const res = await axios.post(url, makeParams(attempt), {
        headers: { Authorization: `Bearer ${META_ACCESS_TOKEN}`, "Content-Type": "application/x-www-form-urlencoded" },
        timeout: 30000,
        validateStatus: () => true,
      });
      if (res.status >= 200 && res.status < 300) return res.data;
      lastError = new Error(`Meta respondió ${res.status}: ${JSON.stringify(res.data)}`);
    }

    throw lastError || new Error("No se pudo sincronizar en Meta");
  }

  async function syncMetaCatalog(properties) {
    if (!META_ACCESS_TOKEN) throw new Error("Falta META_ACCESS_TOKEN");
    if (!META_CATALOG_ID) throw new Error("Falta META_CATALOG_ID");

    const results = [];
    for (const property of properties) {
      const payload = buildMetaPayload(property, { metaUrl: META_DEFAULT_URL, metaImageUrl: META_DEFAULT_IMAGE_URL, metaAvailability: META_DEFAULT_AVAILABILITY }, { extraMetaFields });
      if (!payload.retailer_id || !payload.name || !payload.price || !payload.currency) {
        results.push({ id: property.id, ok: false, message: "Faltan campos mínimos para Meta (retailer_id, name, price, currency)." });
        continue;
      }
      if (!payload.url || !payload.image_url) {
        results.push({ id: property.id, ok: false, message: "Falta URL principal o imagen principal para Meta." });
        continue;
      }
      try {
        const data = await postMetaProduct(payload);
        results.push({ id: property.id, ok: true, data });
      } catch (error) {
        results.push({ id: property.id, ok: false, message: error.message });
      }
    }

    const okCount = results.filter((item) => item.ok).length;
    const failCount = results.length - okCount;
    syncState.lastMetaSyncAt = new Date().toISOString();
    syncState.lastMetaSyncOk = failCount === 0;
    syncState.lastMetaSyncMessage = `Meta sincronizado: ${okCount} OK, ${failCount} con error.`;
    return { ok: failCount === 0, results, message: syncState.lastMetaSyncMessage };
  }

  async function fetchMetaCatalogProducts() {
    if (!META_ACCESS_TOKEN) throw new Error("Falta META_ACCESS_TOKEN");
    if (!META_CATALOG_ID) throw new Error("Falta META_CATALOG_ID");

    const baseUrl = `https://graph.facebook.com/${META_GRAPH_VERSION}`;
    const headers = { Authorization: `Bearer ${META_ACCESS_TOKEN}` };
    const fieldAttempts = [
      "id,retailer_id,name,description,price,currency,url,image_url,availability,additional_image_urls",
      "id,retailer_id,name,description,price,currency,url,image_url,availability",
    ];

    async function fetchProductDetails(productId) {
      if (!productId) return null;
      for (const fields of fieldAttempts) {
        const res = await axios.get(`${baseUrl}/${productId}`, {
          headers,
          params: { fields },
          timeout: 30000,
          validateStatus: () => true,
        });
        if (res.status >= 200 && res.status < 300) return res.data || null;
      }
      return null;
    }

    const items = [];
    let after = "";
    let pageCount = 0;
    let successfulFields = fieldAttempts[0];

    while (pageCount < 25) {
      pageCount += 1;
      let response = null;
      let lastError = null;

      for (const fields of fieldAttempts) {
        const res = await axios.get(`${baseUrl}/${META_CATALOG_ID}/products`, {
          headers,
          params: { fields, limit: 100, ...(after ? { after } : {}) },
          timeout: 30000,
          validateStatus: () => true,
        });
        if (res.status >= 200 && res.status < 300) {
          response = res;
          successfulFields = fields;
          break;
        }
        lastError = new Error(`Meta respondió ${res.status}: ${JSON.stringify(res.data)}`);
      }

      if (!response) throw lastError || new Error("No se pudo leer el catálogo de Meta");

      const pageItems = Array.isArray(response.data?.data) ? response.data.data : [];
      items.push(...pageItems);
      const nextAfter = cleanText(response.data?.paging?.cursors?.after || "");
      if (!nextAfter || !pageItems.length) break;
      after = nextAfter;
    }

    const shouldEnrich = !successfulFields.includes("additional_image_urls") || items.some((item) => !normalizeMetaUrlArray(item?.additional_image_urls).length);
    if (!shouldEnrich) {
      return items.map((item) => ({
        ...item,
        additional_image_urls: uniqueUrlList(item?.additional_image_urls).filter((url) => cleanText(url) !== cleanText(item?.image_url || "")),
      }));
    }

    const enriched = [];
    for (const item of items) {
      const details = await fetchProductDetails(item?.id);
      const merged = {
        ...(item || {}),
        ...(details || {}),
      };
      merged.additional_image_urls = uniqueUrlList(
        merged?.additional_image_urls,
        item?.additional_image_urls,
        details?.additional_image_urls,
        details?.additional_image_cdn_urls,
        merged?.image_url ? [merged.image_url] : []
      ).filter((url) => cleanText(url) !== cleanText(merged?.image_url || ""));
      enriched.push(merged);
    }

    return enriched;
  }

  async function importMetaCatalogIntoPanel() {
    const metaItems = await fetchMetaCatalogProducts();
    const current = dedupeProperties(store.properties);
    const currentByKey = new Map(current.map((item) => [getPropertyMergeKey(item), item]));
    const imported = metaItems.map((product) => buildImportedPropertyFromMetaProduct(product, currentByKey.get(getPropertyMergeKey(product)) || null, current)).filter(Boolean);
    const merged = mergePropertyCollections(current, imported);
    await updateSharedCatalog(merged, true);

    const importedCount = imported.length;
    const createdCount = imported.filter((item) => !currentByKey.has(getPropertyMergeKey(item))).length;
    const updatedCount = importedCount - createdCount;
    syncState.lastMetaImportAt = new Date().toISOString();
    syncState.lastMetaImportOk = true;
    const withMediaCount = imported.filter((item) => Array.isArray(item.image_urls) && item.image_urls.length).length;
    syncState.lastMetaImportMessage = `Meta importado: ${importedCount} leídas, ${createdCount} nuevas, ${updatedCount} actualizadas, ${withMediaCount} con galería.`;
    return { ok: true, items: store.properties, importedCount, createdCount, updatedCount, message: syncState.lastMetaImportMessage };
  }

  async function maybeAutoSync() {
    if (!AUTO_SYNC_ON_SAVE) return { ok: true, skipped: true };
    const bot = await syncBotCatalog(store.properties);
    let meta = { ok: false, skipped: true };
    if (META_ACCESS_TOKEN && META_CATALOG_ID) meta = await syncMetaCatalog(store.properties);
    return { ok: true, bot, meta };
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

    const duplicate = store.properties.find((item) => {
      if (currentId && cleanText(item.id) === cleanText(currentId)) return false;
      return normalizeText(item.id) === normalizeText(property.id) || normalizeText(item.retailer_id) === normalizeText(property.retailer_id) || normalizeText(item.code) === normalizeText(property.code);
    });
    if (duplicate) errors.push("Ya existe una propiedad con el mismo id, retailer_id o code.");
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
        item.raw_post_text,
        item.agent_name,
        item.agent_phone,
        ...(Array.isArray(item.features) ? item.features : []),
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
    if (username !== ADMIN_USERNAME || password !== ADMIN_PASSWORD) return res.status(401).json({ ok: false, error: "Credenciales inválidas" });
    const token = createSessionToken(username, SESSION_SECRET, SESSION_TTL_MS);
    setSessionCookie(res, token);
    return res.json({ ok: true, username });
  });

  router.post("/api/auth/logout", (_req, res) => {
    clearSessionCookie(res);
    return res.json({ ok: true });
  });

  router.get("/api/config/status", requireAuth, (_req, res) => {
    res.json({
      ok: true,
      businessName: BUSINESS_NAME,
      stats: computeStats(store.properties),
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
    res.json({ ok: true, items: sortProperties(items), stats: computeStats(store.properties) });
  });

  router.get("/api/properties/export", requireAuth, (_req, res) => {
    res.setHeader("Content-Type", "application/json");
    res.setHeader("Content-Disposition", 'attachment; filename="PROPERTY_CATALOG_JSON.json"');
    res.send(serializeCatalogJson(store.properties));
  });

  router.post("/api/properties/parse-text", requireAuth, (req, res) => {
    const text = cleanText(req.body?.text || req.body?.raw_post_text || "");
    const current = req.body?.current && typeof req.body.current === "object" ? req.body.current : {};
    const parsed = parseListingTextToProperty(text, current, store.properties);
    res.json({ ok: true, item: parsed, metaPreview: buildMetaPayload(parsed, { metaUrl: META_DEFAULT_URL, metaImageUrl: META_DEFAULT_IMAGE_URL, metaAvailability: META_DEFAULT_AVAILABILITY }, { extraMetaFields }) });
  });

  router.post("/api/properties/import", requireAuth, async (req, res) => {
    const jsonText = cleanText(req.body?.jsonText || "");
    const parsed = safeJson(jsonText, null);
    if (!Array.isArray(parsed)) return res.status(400).json({ ok: false, error: "Debes pegar un JSON array válido." });
    await updateSharedCatalog(parsed, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    res.json({ ok: true, items: store.properties, stats: computeStats(store.properties), auto });
  });

  router.post("/api/properties", requireAuth, async (req, res) => {
    const property = normalizeProperty(req.body || {}, store.properties.length, store.properties);
    const errors = validateProperty(property);
    if (errors.length) return res.status(400).json({ ok: false, errors });
    store.properties.unshift(property);
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    res.json({ ok: true, item: property, stats: computeStats(store.properties), auto });
  });

  router.put("/api/properties/:id", requireAuth, async (req, res) => {
    const id = cleanText(req.params.id);
    const current = store.properties.find((item) => cleanText(item.id) === id);
    if (!current) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    const property = normalizeProperty({ ...current, ...req.body, created_at: current.created_at, updated_at: new Date().toISOString() }, 0, store.properties, id);
    const errors = validateProperty(property, id);
    if (errors.length) return res.status(400).json({ ok: false, errors });
    store.properties = store.properties.map((item) => (cleanText(item.id) === id ? property : item));
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    res.json({ ok: true, item: property, stats: computeStats(store.properties), auto });
  });

  router.delete("/api/properties/:id", requireAuth, async (req, res) => {
    const id = cleanText(req.params.id);
    if (!store.properties.some((item) => cleanText(item.id) === id)) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    store.properties = store.properties.filter((item) => cleanText(item.id) !== id);
    await updateSharedCatalog(store.properties, true);
    const auto = await maybeAutoSync().catch((error) => ({ ok: false, error: error.message }));
    res.json({ ok: true, stats: computeStats(store.properties), auto });
  });

  router.post("/api/sync/bot", requireAuth, async (_req, res) => {
    try {
      const result = await syncBotCatalog(store.properties);
      res.json({ ok: true, ...result });
    } catch (error) {
      syncState.lastBotSyncAt = new Date().toISOString();
      syncState.lastBotSyncOk = false;
      syncState.lastBotSyncMessage = error.message;
      res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/meta/import", requireAuth, async (_req, res) => {
    try {
      const result = await importMetaCatalogIntoPanel();
      res.json({ ok: true, ...result, stats: computeStats(store.properties), syncState });
    } catch (error) {
      syncState.lastMetaImportAt = new Date().toISOString();
      syncState.lastMetaImportOk = false;
      syncState.lastMetaImportMessage = error.message;
      res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/sync/meta", requireAuth, async (_req, res) => {
    try {
      const result = await syncMetaCatalog(store.properties);
      res.json({ ok: result.ok, ...result, syncState });
    } catch (error) {
      syncState.lastMetaSyncAt = new Date().toISOString();
      syncState.lastMetaSyncOk = false;
      syncState.lastMetaSyncMessage = error.message;
      res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.post("/api/sync/all", requireAuth, async (_req, res) => {
    try {
      const bot = await syncBotCatalog(store.properties);
      const meta = META_ACCESS_TOKEN && META_CATALOG_ID ? await syncMetaCatalog(store.properties) : { ok: false, skipped: true };
      res.json({ ok: bot.ok && (meta.ok || meta.skipped), bot, meta, syncState });
    } catch (error) {
      res.status(500).json({ ok: false, error: error.message, syncState });
    }
  });

  router.get("/api/meta-preview/:id", requireAuth, (req, res) => {
    const property = store.properties.find((item) => cleanText(item.id) === cleanText(req.params.id));
    if (!property) return res.status(404).json({ ok: false, error: "Propiedad no encontrada." });
    res.json({ ok: true, property, metaPayload: buildMetaPayload(property, { metaUrl: META_DEFAULT_URL, metaImageUrl: META_DEFAULT_IMAGE_URL, metaAvailability: META_DEFAULT_AVAILABILITY }, { extraMetaFields }) });
  });

  router.get("/styles.css", (_req, res) => res.sendFile(path.join(PUBLIC_DIR, "styles.css")));
  router.get("/app.js", (_req, res) => res.sendFile(path.join(PUBLIC_DIR, "app.js")));
  router.get("/", async (_req, res) => {
    const html = await fs.readFile(path.join(PUBLIC_DIR, "index.html"), "utf8");
    res.type("html").send(html.replaceAll("__ADMIN_BASE_PATH__", ADMIN_BASE_PATH).replaceAll("__BUSINESS_NAME__", BUSINESS_NAME));
  });

  return { router, init };
}
