const ADMIN_BASE = window.ADMIN_BASE_PATH || "/admin";
const API_BASE = `${ADMIN_BASE}/api`;

const state = {
  authenticated: false,
  username: null,
  properties: [],
  stats: { total: 0, active: 0, alquiler: 0, venta: 0, withMedia: 0, metaReady: 0 },
  config: null,
  filters: { q: "", category: "", operation: "", active: "" },
  selectedId: null,
  uploadBusy: false,
  editing: null,
  importOpen: false,
  importText: "",
};

const defaultForm = {
  id: "",
  retailer_id: "",
  code: "",
  title: "",
  category: "apartamentos",
  operation: "venta",
  price: "",
  currency: "DOP",
  location: "",
  exact_address: "",
  exact_location_reference: "",
  bedrooms: "",
  bathrooms: "",
  parking: "",
  floor_level: "",
  area_m2: "",
  lot_m2: "",
  construction_m2: "",
  short_description: "",
  features: "",
  requirements_text: "",
  year_built: "",
  condition: "",
  title_deed: "",
  has_mortgage: "",
  legal_status: "",
  documents_up_to_date: "",
  bank_financing: "",
  bank_financing_note: "",
  down_payment: "",
  payment_facilities: "",
  estimated_monthly_fee: "",
  transfer_cost: "",
  sewer: "",
  paved_street: "",
  water_service: "",
  electric_service: "",
  nearby_places: "",
  safety: "",
  transport_access: "",
  purchase_steps: "",
  purchase_timeline: "",
  status: "available",
  duration_min: "",
  active: true,
  agent_name: "",
  agent_phone: "",
  raw_post_text: "",
  cloudinary_folder: "",
  image_urls: "",
  video_urls: "",
  primary_image_url: "",
  meta_url: "",
  meta_image_url: "",
  meta_availability: "in stock",
};

const appRoot = document.getElementById("app");
const toastLayer = document.createElement("div");
toastLayer.className = "toast-layer";
document.body.appendChild(toastLayer);

function escapeHtml(value) {
  return String(value ?? "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/\"/g, "&quot;")
    .replace(/'/g, "&#039;");
}

function showToast(message, type = "info") {
  const toast = document.createElement("div");
  toast.className = `toast ${type}`;
  toast.textContent = message;
  toastLayer.appendChild(toast);
  setTimeout(() => {
    toast.style.opacity = "0";
    toast.style.transform = "translateY(-4px)";
    toast.style.transition = "0.25s ease";
    setTimeout(() => toast.remove(), 250);
  }, 3500);
}

async function api(url, options = {}) {
  const finalUrl = url.startsWith("http")
    ? url
    : url.startsWith("/api/")
    ? `${ADMIN_BASE}${url}`
    : url.startsWith("/")
    ? `${ADMIN_BASE}${url}`
    : `${ADMIN_BASE}/${url}`;

  const res = await fetch(finalUrl, {
    credentials: "same-origin",
    headers: {
      ...(options.body instanceof FormData ? {} : { "Content-Type": "application/json" }),
      ...(options.headers || {}),
    },
    ...options,
  });

  const contentType = res.headers.get("content-type") || "";
  const data = contentType.includes("application/json") ? await res.json() : await res.text();
  if (!res.ok) {
    throw new Error(typeof data === "string" ? data : data?.error || data?.errors?.join("\n") || "Ocurrió un error");
  }
  return data;
}

function parseList(value) {
  if (Array.isArray(value)) return value.filter(Boolean);
  return String(value || "")
    .split(/[\n,|]+/g)
    .map((item) => item.trim())
    .filter(Boolean);
}

function uniqueTextList(values = []) {
  return [...new Set((Array.isArray(values) ? values : parseList(values)).map((item) => String(item || "").trim()).filter(Boolean))];
}

function joinTextList(values = []) {
  return uniqueTextList(values).join("\n");
}

function removeMediaFromEditing(url, type = "image") {
  if (!state.editing) return;
  const currentImages = uniqueTextList(parseList(state.editing.image_urls || []));
  const currentVideos = uniqueTextList(parseList(state.editing.video_urls || []));

  if (type === "video") {
    updateEditingField("video_urls", joinTextList(currentVideos.filter((item) => item !== url)));
    render();
    return;
  }

  const nextImages = currentImages.filter((item) => item !== url);
  updateEditingField("image_urls", joinTextList(nextImages));

  const currentPrimary = String(state.editing.primary_image_url || state.editing.meta_image_url || "").trim();
  if (!currentPrimary || currentPrimary === url) {
    const nextPrimary = nextImages[0] || "";
    updateEditingField("primary_image_url", nextPrimary);
    updateEditingField("meta_image_url", nextPrimary);
  }

  render();
}

function applyUploadedMedia(uploaded = []) {
  if (!state.editing || !Array.isArray(uploaded) || !uploaded.length) return;

  const nextImages = uniqueTextList([
    ...parseList(state.editing.image_urls || []),
    ...uploaded.filter((item) => item.type === "image").map((item) => item.url),
  ]);
  const nextVideos = uniqueTextList([
    ...parseList(state.editing.video_urls || []),
    ...uploaded.filter((item) => item.type === "video").map((item) => item.url),
  ]);

  updateEditingField("image_urls", joinTextList(nextImages));
  updateEditingField("video_urls", joinTextList(nextVideos));

  const currentPrimary = String(state.editing.primary_image_url || state.editing.meta_image_url || "").trim();
  const nextPrimary = currentPrimary || nextImages[0] || "";
  updateEditingField("primary_image_url", nextPrimary);
  updateEditingField("meta_image_url", nextPrimary);

  render();
}

function normalizeNumericChunk(value) {
  const raw = String(value || "").replace(/\s+/g, "").trim();
  if (!raw) return "";

  const dotCount = (raw.match(/\./g) || []).length;
  const commaCount = (raw.match(/,/g) || []).length;
  const totalSeparators = dotCount + commaCount;

  if (!totalSeparators) return raw;

  if (dotCount && commaCount) {
    const lastDot = raw.lastIndexOf(".");
    const lastComma = raw.lastIndexOf(",");
    const decimalPos = Math.max(lastDot, lastComma);
    const decimalLen = raw.length - decimalPos - 1;

    if (decimalLen >= 1 && decimalLen <= 2) {
      const decimalSep = raw[decimalPos];
      const thousandsRx = decimalSep === "." ? /,/g : /\./g;
      return raw.replace(thousandsRx, "").replace(decimalSep, ".");
    }

    return raw.replace(/[.,]/g, "");
  }

  if (totalSeparators > 1) return raw.replace(/[.,]/g, "");

  const sep = dotCount ? "." : ",";
  const pos = raw.lastIndexOf(sep);
  const decimalLen = raw.length - pos - 1;

  if (decimalLen === 3 || decimalLen > 3) return raw.replace(/[.,]/g, "");
  if (decimalLen >= 1 && decimalLen <= 2) return raw.replace(sep, ".");
  return raw.replace(/[.,]/g, "");
}

function parsePriceNumber(value) {
  const raw = String(value || "").trim();
  if (!raw) return "";

  let text = raw
    .toLowerCase()
    .replace(/rd\$/g, "")
    .replace(/us\$/g, "")
    .replace(/usd/g, "")
    .replace(/dop\$/g, "")
    .replace(/dop/g, "")
    .replace(/pesos?/g, "")
    .replace(/mensual(?:es)?/g, "")
    .replace(/precio\s*/g, "")
    .replace(/pagar\s*/g, "")
    .trim();

  const hasMillion = text.includes("millon") || text.includes("millones") || /\bmm\b/.test(text);
  const hasThousand = /\bk\b/.test(text) || text.includes(" mil") || /^mil\b/.test(text);
  const match = text.match(/\d[\d.,]*/);
  if (!match) return "";

  const normalized = normalizeNumericChunk(match[0]);
  let n = Number(normalized);
  if (!Number.isFinite(n)) return "";

  if (hasMillion) n *= 1000000;
  else if (hasThousand && n < 100000) n *= 1000;

  return String(Math.round(n));
}

function formatMoneyDisplay(value, currency = "DOP") {
  const digits = parsePriceNumber(value);
  if (!digits) return String(value || "-").trim() || "-";
  const prefix = String(currency || "DOP").toUpperCase() === "USD" ? "US$" : "RD$";
  return `${prefix}${Number(digits).toLocaleString("es-DO")}`;
}

function propertyToForm(property) {
  if (!property) return { ...defaultForm };
  return {
    ...defaultForm,
    ...property,
    features: Array.isArray(property.features) ? property.features.join("\n") : property.features || "",
    nearby_places: Array.isArray(property.nearby_places) ? property.nearby_places.join("\n") : property.nearby_places || "",
    image_urls: Array.isArray(property.image_urls) ? property.image_urls.join("\n") : property.image_urls || "",
    video_urls: Array.isArray(property.video_urls) ? property.video_urls.join("\n") : property.video_urls || "",
  };
}

function currentEditingForm() {
  return state.editing ? { ...state.editing } : { ...defaultForm };
}

function getSelectedProperty() {
  return state.properties.find((item) => item.id === state.selectedId) || state.properties[0] || null;
}

function getMediaGallery(property) {
  const imageUrls = parseList(property?.image_urls || []);
  const videoUrls = parseList(property?.video_urls || []);
  const primary = String(property?.primary_image_url || property?.meta_image_url || imageUrls[0] || "").trim();
  return [
    ...imageUrls.map((url) => ({ url, type: "image", primary: url === primary || (!primary && imageUrls[0] === url) })),
    ...videoUrls.map((url) => ({ url, type: "video", primary: false })),
  ];
}

function buildMetaPreview(property) {
  const imageUrls = parseList(property?.image_urls || []);
  const primary = String(property?.meta_image_url || property?.primary_image_url || imageUrls[0] || "").trim();
  const additional = imageUrls.filter((url) => url && url !== primary);
  return {
    retailer_id: property?.retailer_id || property?.code || property?.id || "",
    name: `${String(property?.operation || "venta").toLowerCase() === "alquiler" ? "ALQUILER" : "VENTA"} | ${property?.title || property?.code || property?.id || ""}`,
    price: parsePriceNumber(property?.price || ""),
    currency: property?.currency || "DOP",
    url: property?.meta_url || "",
    image_url: primary,
    additional_image_urls: additional,
  };
}

function badgeClass(operation) {
  return operation === "alquiler" ? "alquiler" : operation === "venta" ? "venta" : "neutral";
}

function renderLogin() {
  return `
    <div class="login-shell">
      <div class="login-card">
        <div class="brand-row">
          <div class="brand-badge">LV</div>
          <div class="brand-copy">
            <h1>${escapeHtml(window.ADMIN_BUSINESS_NAME || "LV Inmobiliaria")}</h1>
            <p>Panel visual para administrar propiedades, pegar publicaciones, manejar galería Cloudinary y sincronizar bot + Meta.</p>
          </div>
        </div>
        <form id="login-form" class="form-grid" style="grid-template-columns:1fr">
          <label class="label">Usuario
            <input class="input" name="username" autocomplete="username" placeholder="admin" />
          </label>
          <label class="label">Contraseña
            <input class="input" name="password" type="password" autocomplete="current-password" placeholder="••••••••" />
          </label>
          <div class="button-row">
            <button class="btn btn-primary" type="submit">Entrar al panel</button>
          </div>
          <div class="info-box">Puedes crear propiedades rápido pegando el texto completo de la publicación y luego añadir los links de Cloudinary en bloque.</div>
        </form>
      </div>
    </div>
  `;
}

function statCard(label, value) {
  return `<div class="stat-card"><small>${escapeHtml(label)}</small><strong>${escapeHtml(value)}</strong></div>`;
}

function renderRows() {
  if (!state.properties.length) {
    return `<tr><td colspan="8"><div class="empty-state">No hay propiedades cargadas todavía.</div></td></tr>`;
  }

  return state.properties.map((item) => {
    const selected = item.id === state.selectedId ? "is-selected" : "";
    const image = item.primary_image_url || item.meta_image_url || parseList(item.image_urls || [])[0] || "";
    return `
      <tr class="${selected}" data-select-id="${escapeHtml(item.id)}">
        <td>
          ${image ? `<div style="display:flex;gap:12px;align-items:center"><img src="${escapeHtml(image)}" alt="" style="width:64px;height:50px;object-fit:cover;border-radius:12px;border:1px solid rgba(255,255,255,0.08)" /><div><strong>${escapeHtml(item.title || item.code)}</strong><small>${escapeHtml(item.id)}</small></div></div>` : `<strong>${escapeHtml(item.title || item.code)}</strong><small>${escapeHtml(item.id)}</small>`}
        </td>
        <td><span class="badge ${badgeClass(item.operation)}">${escapeHtml(item.operation || "-")}</span></td>
        <td>${escapeHtml(item.category || "-")}</td>
        <td>${escapeHtml(item.location || "-")}</td>
        <td>${escapeHtml(formatMoneyDisplay(item.price, item.currency))}</td>
        <td>${parseList(item.image_urls || []).length} fotos / ${parseList(item.video_urls || []).length} videos</td>
        <td><span class="badge ${item.active ? "active" : "inactive"}">${item.active ? "Activa" : "Inactiva"}</span></td>
        <td>
          <div class="button-row">
            <button class="btn btn-secondary btn-small" data-edit-id="${escapeHtml(item.id)}">Editar</button>
            <button class="btn btn-danger btn-small" data-delete-id="${escapeHtml(item.id)}">Eliminar</button>
          </div>
        </td>
      </tr>
    `;
  }).join("");
}

function renderPropertyCards() {
  if (!state.properties.length) {
    return `<div class="empty-state">No hay propiedades cargadas todavía.</div>`;
  }

  return state.properties.map((item) => {
    const image = item.primary_image_url || item.meta_image_url || parseList(item.image_urls || [])[0] || "";
    const selected = item.id === state.selectedId ? " is-selected" : "";
    return `
      <article class="property-card-mobile${selected}" data-select-id="${escapeHtml(item.id)}">
        <div class="property-card-mobile__media">
          ${image ? `<img src="${escapeHtml(image)}" alt="${escapeHtml(item.title || item.code)}" />` : `<div class="property-card-mobile__placeholder">Sin portada</div>`}
        </div>
        <div class="property-card-mobile__body">
          <div class="property-card-mobile__head">
            <div>
              <strong>${escapeHtml(item.title || item.code)}</strong>
              <small>${escapeHtml(item.id)}</small>
            </div>
            <span class="badge ${item.active ? "active" : "inactive"}">${item.active ? "Activa" : "Inactiva"}</span>
          </div>
          <div class="property-card-mobile__meta">
            <span class="badge ${badgeClass(item.operation)}">${escapeHtml(item.operation || "-")}</span>
            <span>${escapeHtml(item.category || "-")}</span>
            <span>${escapeHtml(item.location || "-")}</span>
            <span class="property-card-mobile__price">${escapeHtml(formatMoneyDisplay(item.price, item.currency))}</span>
            <span>${parseList(item.image_urls || []).length} fotos / ${parseList(item.video_urls || []).length} videos</span>
          </div>
          <div class="button-row property-card-mobile__actions">
            <button class="btn btn-secondary btn-small" data-edit-id="${escapeHtml(item.id)}">Editar</button>
            <button class="btn btn-danger btn-small" data-delete-id="${escapeHtml(item.id)}">Eliminar</button>
          </div>
        </div>
      </article>
    `;
  }).join("");
}

function renderSelectedDetail() {
  const selected = getSelectedProperty();
  if (!selected) {
    return `<div class="detail-panel"><div class="empty-state">Selecciona una propiedad para ver el detalle.</div></div>`;
  }

  const gallery = getMediaGallery(selected);
  const image = selected.primary_image_url || selected.meta_image_url || gallery.find((item) => item.type === "image")?.url || "";
  const metaPreview = buildMetaPreview(selected);

  return `
    <div class="detail-grid">
      <div class="detail-panel">
        ${image ? `<div class="preview-photo"><img src="${escapeHtml(image)}" alt="${escapeHtml(selected.title || "")}" /></div>` : `<div class="preview-photo" style="display:grid;place-items:center;color:var(--muted)">Sin portada</div>`}
        <div class="section-head">
          <div>
            <h3>${escapeHtml(selected.title || selected.code)}</h3>
            <p>${escapeHtml(selected.location || "Sin ubicación")}</p>
          </div>
          <span class="badge ${selected.active ? "active" : "inactive"}">${selected.active ? "Activa" : "Inactiva"}</span>
        </div>
        <div class="media-summary-grid">
          <div class="card-panel"><small class="muted">Código</small><strong>${escapeHtml(selected.code || "-")}</strong></div>
          <div class="card-panel"><small class="muted">Precio</small><strong>${escapeHtml(formatMoneyDisplay(selected.price, selected.currency))}</strong></div>
          <div class="card-panel"><small class="muted">Habitaciones / baños</small><strong>${escapeHtml(selected.bedrooms || "-")} / ${escapeHtml(selected.bathrooms || "-")}</strong></div>
        </div>
        <div class="meta-list" style="margin-top:14px">
          <div class="meta-item">
            <strong>Descripción corta</strong>
            <div class="muted">${escapeHtml(selected.short_description || "Sin descripción")}</div>
          </div>
          <div class="meta-item">
            <strong>Características</strong>
            <div class="feature-list">
              ${(Array.isArray(selected.features) ? selected.features : []).length ? (selected.features || []).map((feature) => `<span class="feature-pill">${escapeHtml(feature)}</span>`).join("") : `<span class="muted">Sin características registradas</span>`}
            </div>
          </div>
          ${selected.requirements_text ? `<div class="meta-item"><strong>Requisitos</strong><pre class="raw-preview">${escapeHtml(selected.requirements_text)}</pre></div>` : ""}
        </div>
      </div>
      <div class="detail-panel">
        <div class="meta-item">
          <strong>Sincronización e IDs</strong>
          <div><code>id</code>: ${escapeHtml(selected.id)}</div>
          <div><code>retailer_id</code>: ${escapeHtml(selected.retailer_id)}</div>
          <div><code>code</code>: ${escapeHtml(selected.code)}</div>
        </div>
        <div class="meta-item">
          <strong>Preview para Meta</strong>
          <pre class="json-preview">${escapeHtml(JSON.stringify(metaPreview, null, 2))}</pre>
        </div>
        <div class="meta-item">
          <strong>Galería</strong>
          ${gallery.length ? `<div class="gallery-grid">${gallery.map((item, index) => `
            <div class="gallery-card">
              <div class="gallery-thumb">${item.type === "video" ? `<video src="${escapeHtml(item.url)}" controls playsinline></video>` : `<img src="${escapeHtml(item.url)}" alt="media ${index + 1}" />`}</div>
              <div class="gallery-card-body">
                <small>${item.type === "video" ? "Video" : item.primary ? "Portada" : "Imagen adicional"}</small>
                <a class="btn btn-ghost btn-small" href="${escapeHtml(item.url)}" target="_blank" rel="noopener">Abrir</a>
              </div>
            </div>
          `).join("")}</div>` : `<div class="empty-state">Sin fotos ni videos todavía.</div>`}
        </div>
      </div>
    </div>
  `;
}

function syncCard(title, description, ready, message, ok, action) {
  const buttonLabel = action === "bot" ? "Sincronizar bot" : action === "meta" ? "Sincronizar Meta" : action === "all" ? "Sincronizar todo" : "Importar desde Meta";
  return `
    <div class="sync-card">
      <div class="section-head">
        <div>
          <h4>${escapeHtml(title)}</h4>
          <p>${escapeHtml(description)}</p>
        </div>
        <span class="badge ${ready ? "active" : "inactive"}">${ready ? "Configurado" : "Pendiente"}</span>
      </div>
      <div class="info-box">${escapeHtml(message || "Sin actividad")}</div>
      <div class="button-row">
        <button class="btn ${ready ? "btn-primary" : "btn-secondary"}" data-sync-action="${escapeHtml(action)}" ${ready ? "" : "disabled"}>${escapeHtml(buttonLabel)}</button>
      </div>
      ${ok === true ? `<div class="success-box">Última ejecución correcta.</div>` : ok === false ? `<div class="error-box">Último resultado con error.</div>` : ""}
    </div>
  `;
}

function renderDashboard() {
  const integrations = state.config?.integrations || {};
  const syncState = state.config?.syncState || {};

  return `
    <div class="dashboard-shell">
      <aside class="sidebar">
        <div class="brand-row">
          <div class="brand-badge">LV</div>
          <div class="brand-copy">
            <h2>${escapeHtml(window.ADMIN_BUSINESS_NAME || "LV Inmobiliaria")}</h2>
            <p>Panel Admin</p>
          </div>
        </div>
        <div class="sidebar-nav">
          <div class="nav-chip active">Propiedades</div>
          <div class="nav-chip">Sincronización</div>
          <div class="nav-chip">Vista previa Meta</div>
        </div>
        <div class="info-stack">
          <div class="info-box">
            <div><strong>Usuario:</strong> ${escapeHtml(state.username || "-")}</div>
            <div><strong>Bot env:</strong> ${escapeHtml(integrations.renderBotEnvKey || "PROPERTY_CATALOG_JSON")}</div>
          </div>
          <div class="info-box">Pega la publicación completa, deja que el panel autocompletar y luego añade links o sube fotos y videos directo a Cloudinary.</div>
        </div>
        <div class="button-row">
          <button class="btn btn-ghost" id="logout-btn">Cerrar sesión</button>
        </div>
      </aside>
      <main class="main">
        <section class="topbar">
          <div class="topbar-row">
            <div>
              <h1>Catálogo centralizado</h1>
              <p>Administra el catálogo del bot y el catálogo de Meta desde un mismo panel.</p>
            </div>
            <div class="button-row">
              <button class="btn btn-secondary" id="import-btn">Importar JSON</button>
              <button class="btn btn-secondary" id="import-meta-btn" ${integrations.metaImportReady ? "" : "disabled"}>Cargar desde Meta</button>
              <button class="btn btn-primary" id="new-btn">Nueva propiedad</button>
            </div>
          </div>
          <div class="stats-grid">
            ${statCard("Total", state.stats.total || 0)}
            ${statCard("Activas", state.stats.active || 0)}
            ${statCard("Alquiler", state.stats.alquiler || 0)}
            ${statCard("Venta", state.stats.venta || 0)}
            ${statCard("Con media", state.stats.withMedia || 0)}
          </div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Sincronización centralizada</h3>
              <p>Aplica cambios al bot y al catálogo de Meta desde este mismo panel.</p>
            </div>
          </div>
          <div class="sync-grid">
            ${syncCard("Bot / Render", "Actualiza PROPERTY_CATALOG_JSON y deja el bot alineado con el panel.", true, syncState.lastBotSyncMessage, syncState.lastBotSyncOk, "bot")}
            ${syncCard("Meta Catalog", "Empuja la portada + imágenes extra compatibles con Meta.", integrations.metaReady, syncState.lastMetaSyncMessage, syncState.lastMetaSyncOk, "meta")}
            ${syncCard("Importar desde Meta", "Trae al panel lo que ya existe en el catálogo de Meta.", integrations.metaImportReady, syncState.lastMetaImportMessage, syncState.lastMetaImportOk, "meta-import")}
          </div>
          <div class="button-row" style="margin-top:14px">
            <button class="btn btn-primary" data-sync-action="all">Sincronizar todo</button>
          </div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Propiedades</h3>
              <p>Busca, filtra, edita y abre la vista previa técnica.</p>
            </div>
          </div>
          <div class="form-grid-3" style="margin-bottom:12px">
            <label class="label">Buscar
              <input class="input" id="filter-q" value="${escapeHtml(state.filters.q)}" placeholder="nombre, código, ubicación o texto" />
            </label>
            <label class="label">Categoría
              <select class="select" id="filter-category">
                <option value="">Todas</option>
                <option value="apartamentos" ${state.filters.category === "apartamentos" ? "selected" : ""}>Apartamentos</option>
                <option value="casas" ${state.filters.category === "casas" ? "selected" : ""}>Casas</option>
                <option value="solares" ${state.filters.category === "solares" ? "selected" : ""}>Solares</option>
                <option value="proyectos" ${state.filters.category === "proyectos" ? "selected" : ""}>Proyectos</option>
                <option value="locales_comerciales" ${state.filters.category === "locales_comerciales" ? "selected" : ""}>Locales comerciales</option>
              </select>
            </label>
            <label class="label">Operación
              <select class="select" id="filter-operation">
                <option value="">Todas</option>
                <option value="venta" ${state.filters.operation === "venta" ? "selected" : ""}>Venta</option>
                <option value="alquiler" ${state.filters.operation === "alquiler" ? "selected" : ""}>Alquiler</option>
              </select>
            </label>
          </div>
          <div class="table-wrap table-wrap-desktop">
            <table class="table">
              <thead>
                <tr>
                  <th>Propiedad</th>
                  <th>Operación</th>
                  <th>Categoría</th>
                  <th>Ubicación</th>
                  <th>Precio</th>
                  <th>Media</th>
                  <th>Estado</th>
                  <th>Acciones</th>
                </tr>
              </thead>
              <tbody>${renderRows()}</tbody>
            </table>
          </div>
          <div class="mobile-cards">${renderPropertyCards()}</div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Detalle y vista previa</h3>
              <p>Revisa la ficha, la galería y el payload que saldrá para Meta.</p>
            </div>
          </div>
          ${renderSelectedDetail()}
        </section>
      </main>
      ${renderPropertyModal()}
      ${renderImportModal()}
    </div>
  `;
}

function renderMediaPreview(form) {
  const gallery = getMediaGallery(form);
  if (!gallery.length) return `<div class="empty-state">Puedes pegar links de Cloudinary o subir fotos y videos directo desde el panel.</div>`;
  return `<div class="gallery-grid">${gallery.map((item, index) => `
    <div class="gallery-card">
      <div class="gallery-thumb">${item.type === "video" ? `<video src="${escapeHtml(item.url)}" controls playsinline></video>` : `<img src="${escapeHtml(item.url)}" alt="${escapeHtml(form.title || "media")}" />`}</div>
      <div class="gallery-card-body">
        <small>${item.type === "video" ? "Video" : item.primary ? "Portada" : "Imagen extra"}</small>
        <div class="button-row">
          ${item.type === "image" ? `<button type="button" class="btn btn-secondary btn-small" data-set-primary="${escapeHtml(item.url)}">Usar como portada</button>` : `<span class="muted">No se usa como portada</span>`}
          <button type="button" class="btn btn-ghost btn-small" data-remove-media-url="${escapeHtml(item.url)}" data-remove-media-type="${escapeHtml(item.type)}">Quitar</button>
        </div>
      </div>
    </div>
  `).join("")}</div>`;
}

function renderPropertyModal() {
  if (!state.editing) return `<div class="modal-backdrop" id="property-modal"></div>`;
  const form = state.editing;
  const preview = buildMetaPreview(form);
  return `
    <div class="modal-backdrop open" id="property-modal">
      <div class="modal">
        <div class="modal-head">
          <div class="modal-title">
            <div>
              <h2>${form.__mode === "edit" ? "Editar propiedad" : "Nueva propiedad"}</h2>
              <p class="muted">Pega la publicación completa y deja que el panel autocompletar lo más pesado.</p>
            </div>
            <button class="btn btn-ghost" type="button" id="close-modal-btn">Cerrar</button>
          </div>
        </div>
        <form id="property-form">
          <div class="modal-body">
            <div class="editor-grid">
              <div class="editor-stack">
                <section class="raw-panel">
                  <div class="section-head">
                    <div>
                      <h3>1. Texto completo de la publicación</h3>
                      <p>Pega aquí el texto tal como te lo envían por WhatsApp o como sale en el canal.</p>
                    </div>
                    <button type="button" class="btn btn-primary" id="parse-post-btn">Autocompletar desde texto</button>
                  </div>
                  <label class="label">Texto bruto
                    <textarea class="textarea" name="raw_post_text" id="raw-post-text" style="min-height:250px">${escapeHtml(form.raw_post_text || "")}</textarea>
                  </label>
                  <div class="info-box">Este botón intenta sacar operación, categoría, ubicación, precio, habitaciones, baños, parqueos, agente, requisitos y un código base.</div>
                </section>

                <section class="media-panel">
                  <div class="section-head">
                    <div>
                      <h3>2. Galería Cloudinary</h3>
                      <p>Sube fotos y videos directo desde el panel o pega los links en bloque.</p>
                    </div>
                    <span class="badge ${(state.config?.integrations?.cloudinaryReady) ? "active" : "inactive"}">${(state.config?.integrations?.cloudinaryReady) ? "Cloudinary listo" : "Cloudinary pendiente"}</span>
                  </div>
                  <div class="upload-grid">
                    <label class="label">Seleccionar fotos
                      <input class="input input-file" type="file" id="upload-images-input" accept="image/*" multiple ${state.uploadBusy || !(state.config?.integrations?.cloudinaryReady) ? "disabled" : ""} />
                    </label>
                    <div class="upload-action-box">
                      <button type="button" class="btn btn-primary" id="upload-images-btn" ${state.uploadBusy || !(state.config?.integrations?.cloudinaryReady) ? "disabled" : ""}>${state.uploadBusy ? "Subiendo..." : "Subir fotos"}</button>
                      <small class="muted">Puedes elegir varias imágenes a la vez.</small>
                    </div>
                    <label class="label">Seleccionar videos
                      <input class="input input-file" type="file" id="upload-videos-input" accept="video/*" multiple ${state.uploadBusy || !(state.config?.integrations?.cloudinaryReady) ? "disabled" : ""} />
                    </label>
                    <div class="upload-action-box">
                      <button type="button" class="btn btn-secondary" id="upload-videos-btn" ${state.uploadBusy || !(state.config?.integrations?.cloudinaryReady) ? "disabled" : ""}>${state.uploadBusy ? "Subiendo..." : "Subir videos"}</button>
                      <small class="muted">Ideal para recorridos o clips de la propiedad.</small>
                    </div>
                  </div>
                  <div class="two-col">
                    <label class="label">URLs de imágenes (Cloudinary)
                      <textarea class="textarea" name="image_urls" id="image-urls" style="min-height:180px">${escapeHtml(form.image_urls || "")}</textarea>
                    </label>
                    <label class="label">URLs de videos (Cloudinary)
                      <textarea class="textarea" name="video_urls" id="video-urls" style="min-height:180px">${escapeHtml(form.video_urls || "")}</textarea>
                    </label>
                  </div>
                  <div class="two-col">
                    <label class="label">Portada principal
                      <input class="input" name="primary_image_url" id="primary-image-url" value="${escapeHtml(form.primary_image_url || form.meta_image_url || "")}" placeholder="https://res.cloudinary.com/..." />
                    </label>
                    <label class="label">Folder Cloudinary (opcional)
                      <input class="input" name="cloudinary_folder" value="${escapeHtml(form.cloudinary_folder || state.config?.integrations?.cloudinaryDefaultFolder || "")}" placeholder="lv/rivas-148" />
                    </label>
                  </div>
                  ${!(state.config?.integrations?.cloudinaryReady) ? `<div class="error-box">Configura CLOUDINARY_CLOUD_NAME, CLOUDINARY_API_KEY y CLOUDINARY_API_SECRET en Render para habilitar la subida directa.</div>` : `<div class="info-box">La primera foto subida se usará como portada si aún no has elegido una.</div>`}
                  <div id="media-preview-slot">${renderMediaPreview(form)}</div>
                </section>
              </div>

              <div class="editor-stack">
                <section class="card-panel">
                  <div class="section-head">
                    <div>
                      <h3>3. Datos clave</h3>
                      <p>Puedes ajustar manualmente lo que haga falta.</p>
                    </div>
                  </div>
                  <div class="form-grid-3">
                    <label class="label">ID
                      <input class="input" name="id" value="${escapeHtml(form.id || "")}" />
                    </label>
                    <label class="label">retailer_id
                      <input class="input" name="retailer_id" value="${escapeHtml(form.retailer_id || "")}" />
                    </label>
                    <label class="label">code
                      <input class="input" name="code" value="${escapeHtml(form.code || "")}" />
                    </label>
                  </div>
                  <div class="form-grid">
                    <label class="label">Título
                      <input class="input" name="title" value="${escapeHtml(form.title || "")}" />
                    </label>
                    <label class="label">Ubicación
                      <input class="input" name="location" value="${escapeHtml(form.location || "")}" />
                    </label>
                  </div>
                  <div class="form-grid-3">
                    <label class="label">Categoría
                      <select class="select" name="category">
                        <option value="apartamentos" ${form.category === "apartamentos" ? "selected" : ""}>Apartamentos</option>
                        <option value="casas" ${form.category === "casas" ? "selected" : ""}>Casas</option>
                        <option value="solares" ${form.category === "solares" ? "selected" : ""}>Solares</option>
                        <option value="proyectos" ${form.category === "proyectos" ? "selected" : ""}>Proyectos</option>
                        <option value="locales_comerciales" ${form.category === "locales_comerciales" ? "selected" : ""}>Locales comerciales</option>
                      </select>
                    </label>
                    <label class="label">Operación
                      <select class="select" name="operation">
                        <option value="venta" ${form.operation === "venta" ? "selected" : ""}>Venta</option>
                        <option value="alquiler" ${form.operation === "alquiler" ? "selected" : ""}>Alquiler</option>
                      </select>
                    </label>
                    <label class="label">Moneda
                      <select class="select" name="currency">
                        <option value="DOP" ${form.currency === "DOP" ? "selected" : ""}>DOP</option>
                        <option value="USD" ${form.currency === "USD" ? "selected" : ""}>USD</option>
                      </select>
                    </label>
                  </div>
                  <div class="form-grid-3">
                    <label class="label">Precio
                      <input class="input" name="price" value="${escapeHtml(form.price || "")}" placeholder="RD$25,900" />
                    </label>
                    <label class="label">Habitaciones
                      <input class="input" name="bedrooms" value="${escapeHtml(form.bedrooms || "")}" />
                    </label>
                    <label class="label">Baños
                      <input class="input" name="bathrooms" value="${escapeHtml(form.bathrooms || "")}" />
                    </label>
                  </div>
                  <div class="form-grid-3">
                    <label class="label">Parqueos
                      <input class="input" name="parking" value="${escapeHtml(form.parking || "")}" />
                    </label>
                    <label class="label">Nivel / piso
                      <input class="input" name="floor_level" value="${escapeHtml(form.floor_level || "")}" />
                    </label>
                    <label class="label">Duración visita (min)
                      <input class="input" name="duration_min" value="${escapeHtml(form.duration_min || "")}" />
                    </label>
                  </div>
                  <div class="form-grid-3">
                    <label class="label">Área m²
                      <input class="input" name="area_m2" value="${escapeHtml(form.area_m2 || "")}" />
                    </label>
                    <label class="label">Solar m²
                      <input class="input" name="lot_m2" value="${escapeHtml(form.lot_m2 || "")}" />
                    </label>
                    <label class="label">Construcción m²
                      <input class="input" name="construction_m2" value="${escapeHtml(form.construction_m2 || "")}" />
                    </label>
                  </div>
                  <div class="form-grid">
                    <label class="label">Agente
                      <input class="input" name="agent_name" value="${escapeHtml(form.agent_name || "")}" />
                    </label>
                    <label class="label">Teléfono agente
                      <input class="input" name="agent_phone" value="${escapeHtml(form.agent_phone || "")}" />
                    </label>
                  </div>
                  <label class="label">Descripción corta
                    <textarea class="textarea" name="short_description">${escapeHtml(form.short_description || "")}</textarea>
                  </label>
                  <div class="two-col">
                    <label class="label">Características
                      <textarea class="textarea" name="features" style="min-height:160px">${escapeHtml(form.features || "")}</textarea>
                    </label>
                    <label class="label">Requisitos
                      <textarea class="textarea" name="requirements_text" style="min-height:160px">${escapeHtml(form.requirements_text || "")}</textarea>
                    </label>
                  </div>
                </section>

                <section class="meta-panel">
                  <div class="section-head">
                    <div>
                      <h3>4. Meta y extras</h3>
                      <p>La portada sale de primary_image_url y las demás fotos se envían como imágenes adicionales.</p>
                    </div>
                  </div>
                  <div class="form-grid">
                    <label class="label">URL pública
                      <input class="input" name="meta_url" value="${escapeHtml(form.meta_url || "")}" placeholder="https://lvinmobiliarias.com/..." />
                    </label>
                    <label class="label">Disponibilidad Meta
                      <input class="input" name="meta_availability" value="${escapeHtml(form.meta_availability || "in stock")}" />
                    </label>
                  </div>
                  <div class="checkbox-row">
                    <input type="checkbox" id="field-active" name="active" ${form.active ? "checked" : ""} />
                    <label for="field-active">Propiedad activa</label>
                  </div>
                  <div class="meta-item">
                    <strong>Preview técnico</strong>
                    <pre class="json-preview">${escapeHtml(JSON.stringify(preview, null, 2))}</pre>
                  </div>
                </section>
              </div>
            </div>
          </div>
          <div class="modal-foot">
            <div class="button-row">
              <button type="button" class="btn btn-secondary" id="cancel-modal-btn">Cancelar</button>
              <button type="submit" class="btn btn-primary">Guardar propiedad</button>
            </div>
          </div>
        </form>
      </div>
    </div>
  `;
}

function renderImportModal() {
  if (!state.importOpen) return `<div class="modal-backdrop" id="import-modal"></div>`;
  return `
    <div class="modal-backdrop open" id="import-modal">
      <div class="modal" style="width:min(980px,100%)">
        <div class="modal-head">
          <div class="modal-title">
            <div>
              <h2>Importar PROPERTY_CATALOG_JSON</h2>
              <p class="muted">Pega el array completo. Esto reemplaza el contenido actual del panel.</p>
            </div>
            <button class="btn btn-ghost" id="close-import-btn">Cerrar</button>
          </div>
        </div>
        <div class="modal-body">
          <textarea class="textarea" id="import-json" style="min-height:420px">${escapeHtml(state.importText || "")}</textarea>
        </div>
        <div class="modal-foot">
          <div class="button-row">
            <button class="btn btn-secondary" id="cancel-import-btn" type="button">Cancelar</button>
            <button class="btn btn-primary" id="confirm-import-btn" type="button">Importar catálogo</button>
          </div>
        </div>
      </div>
    </div>
  `;
}

function render() {
  document.body.classList.toggle("modal-open", !!state.editing || state.importOpen);
  appRoot.innerHTML = state.authenticated ? renderDashboard() : renderLogin();
  bindEvents();
}

async function loadSession() {
  const session = await api("/api/session");
  state.authenticated = !!session.authenticated;
  state.username = session.username;
}

async function refreshData() {
  const params = new URLSearchParams();
  Object.entries(state.filters).forEach(([key, value]) => {
    if (value) params.set(key, value);
  });
  const [list, config] = await Promise.all([
    api(`/api/properties?${params.toString()}`),
    api("/api/config/status"),
  ]);
  state.properties = list.items || [];
  state.stats = list.stats || state.stats;
  state.config = config;
  if (!state.selectedId && state.properties[0]) state.selectedId = state.properties[0].id;
  if (state.selectedId && !state.properties.some((item) => item.id === state.selectedId)) state.selectedId = state.properties[0]?.id || null;
}

async function boot() {
  try {
    await loadSession();
    if (state.authenticated) await refreshData();
  } catch (error) {
    showToast(error.message, "error");
  }
  render();
}

function debounce(fn, delay = 280) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

const debouncedRefresh = debounce(async () => {
  await refreshData();
  render();
}, 320);

async function handleLogin(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  try {
    await api("/api/auth/login", { method: "POST", body: JSON.stringify({ username: formData.get("username"), password: formData.get("password") }) });
    await loadSession();
    await refreshData();
    render();
    showToast("Sesión iniciada correctamente", "success");
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function handleLogout() {
  await api("/api/auth/logout", { method: "POST" });
  state.authenticated = false;
  state.username = null;
  state.properties = [];
  state.selectedId = null;
  state.config = null;
  render();
}

function openCreateModal() {
  state.editing = { ...defaultForm, __mode: "create" };
  render();
}

function openEditModal(id) {
  const item = state.properties.find((property) => property.id === id);
  if (!item) return;
  state.editing = { ...propertyToForm(item), __mode: "edit", __currentId: id };
  render();
}

function closePropertyModal() {
  state.editing = null;
  render();
}

function closeImportModal() {
  state.importOpen = false;
  render();
}

function collectPropertyPayload(form) {
  const formData = new FormData(form);
  const payload = Object.fromEntries(formData.entries());
  payload.active = formData.get("active") === "on";
  return payload;
}

async function handleSaveProperty(event) {
  event.preventDefault();
  const payload = collectPropertyPayload(event.currentTarget);
  try {
    if (state.editing.__mode === "create") {
      await api("/api/properties", { method: "POST", body: JSON.stringify(payload) });
      showToast("Propiedad creada", "success");
    } else {
      await api(`/api/properties/${encodeURIComponent(state.editing.__currentId)}`, { method: "PUT", body: JSON.stringify(payload) });
      showToast("Propiedad actualizada", "success");
    }
    state.editing = null;
    await refreshData();
    render();
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function handleDelete(id) {
  if (!confirm("¿Seguro que deseas eliminar esta propiedad?")) return;
  try {
    await api(`/api/properties/${encodeURIComponent(id)}`, { method: "DELETE" });
    showToast("Propiedad eliminada", "success");
    await refreshData();
    render();
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function handleSync(action) {
  const endpoint = action === "all" ? "/api/sync/all" : action === "meta" ? "/api/sync/meta" : action === "meta-import" ? "/api/meta/import" : "/api/sync/bot";
  try {
    const result = await api(endpoint, { method: "POST" });
    await refreshData();
    render();
    showToast(result.message || "Operación completada", "success");
  } catch (error) {
    await refreshData().catch(() => {});
    render();
    showToast(error.message, "error");
  }
}

async function handleImportConfirm() {
  const textarea = document.getElementById("import-json");
  const jsonText = textarea?.value || "";
  try {
    await api("/api/properties/import", { method: "POST", body: JSON.stringify({ jsonText }) });
    state.importOpen = false;
    state.importText = "";
    await refreshData();
    render();
    showToast("Catálogo importado", "success");
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function handleParsePostText() {
  const textarea = document.getElementById("raw-post-text");
  if (!textarea) return;
  const text = textarea.value || "";
  if (!text.trim()) {
    showToast("Pega primero el texto de la publicación", "error");
    return;
  }

  const current = collectPropertyPayload(document.getElementById("property-form"));
  try {
    const result = await api("/api/properties/parse-text", {
      method: "POST",
      body: JSON.stringify({ text, current: { ...current, __mode: undefined, __currentId: undefined } }),
    });
    state.editing = { ...propertyToForm(result.item), __mode: state.editing.__mode, __currentId: state.editing.__currentId };
    render();
    showToast("Texto procesado y campos autocompletados", "success");
  } catch (error) {
    showToast(error.message, "error");
  }
}

async function handleCloudinaryUpload(kind = "image") {
  if (!state.editing) return;
  const input = document.getElementById(kind === "video" ? "upload-videos-input" : "upload-images-input");
  if (!input) return;

  const files = Array.from(input.files || []);
  if (!files.length) {
    showToast(kind === "video" ? "Selecciona al menos un video." : "Selecciona al menos una foto.", "error");
    return;
  }

  const folder = document.querySelector('input[name="cloudinary_folder"]')?.value || state.editing.cloudinary_folder || "";
  const formData = new FormData();
  formData.append("kind", kind);
  if (folder) formData.append("folder", folder);
  files.forEach((file) => formData.append("files", file));

  state.uploadBusy = true;
  render();

  try {
    const result = await api("/api/uploads/cloudinary", { method: "POST", body: formData });
    applyUploadedMedia(result.uploaded || []);
    input.value = "";
    showToast(`${result.count || 0} archivo(s) subido(s) a Cloudinary.`, "success");
  } catch (error) {
    showToast(error.message, "error");
  } finally {
    state.uploadBusy = false;
    render();
  }
}

function updateEditingField(name, value) {
  if (!state.editing) return;
  state.editing[name] = value;
}

function bindLiveEditor() {
  const propertyForm = document.getElementById("property-form");
  if (!propertyForm) return;

  propertyForm.querySelectorAll("input, textarea, select").forEach((element) => {
    const handler = () => {
      if (!element.name) return;
      if (element.type === "checkbox") updateEditingField(element.name, element.checked);
      else updateEditingField(element.name, element.value);
      if (["image_urls", "video_urls", "primary_image_url", "meta_url", "meta_image_url"].includes(element.name)) {
        render();
      }
    };
    element.addEventListener("input", handler);
    element.addEventListener("change", handler);
  });
}

function bindEvents() {
  const loginForm = document.getElementById("login-form");
  if (loginForm) loginForm.addEventListener("submit", handleLogin);

  const logoutBtn = document.getElementById("logout-btn");
  if (logoutBtn) logoutBtn.addEventListener("click", handleLogout);

  const newBtn = document.getElementById("new-btn");
  if (newBtn) newBtn.addEventListener("click", openCreateModal);

  const importBtn = document.getElementById("import-btn");
  if (importBtn) importBtn.addEventListener("click", () => {
    state.importOpen = true;
    state.importText = "";
    render();
  });

  const importMetaBtn = document.getElementById("import-meta-btn");
  if (importMetaBtn) importMetaBtn.addEventListener("click", () => handleSync("meta-import"));

  const propertyForm = document.getElementById("property-form");
  if (propertyForm) propertyForm.addEventListener("submit", handleSaveProperty);

  const parseBtn = document.getElementById("parse-post-btn");
  if (parseBtn) parseBtn.addEventListener("click", handleParsePostText);

  const uploadImagesBtn = document.getElementById("upload-images-btn");
  if (uploadImagesBtn) uploadImagesBtn.addEventListener("click", () => handleCloudinaryUpload("image"));

  const uploadVideosBtn = document.getElementById("upload-videos-btn");
  if (uploadVideosBtn) uploadVideosBtn.addEventListener("click", () => handleCloudinaryUpload("video"));

  const closeModalBtn = document.getElementById("close-modal-btn");
  if (closeModalBtn) closeModalBtn.addEventListener("click", closePropertyModal);
  const cancelModalBtn = document.getElementById("cancel-modal-btn");
  if (cancelModalBtn) cancelModalBtn.addEventListener("click", closePropertyModal);

  const closeImportBtn = document.getElementById("close-import-btn");
  if (closeImportBtn) closeImportBtn.addEventListener("click", closeImportModal);
  const cancelImportBtn = document.getElementById("cancel-import-btn");
  if (cancelImportBtn) cancelImportBtn.addEventListener("click", closeImportModal);
  const confirmImportBtn = document.getElementById("confirm-import-btn");
  if (confirmImportBtn) confirmImportBtn.addEventListener("click", handleImportConfirm);

  const filterQ = document.getElementById("filter-q");
  if (filterQ) filterQ.addEventListener("input", (event) => {
    state.filters.q = event.target.value;
    debouncedRefresh();
  });
  const filterCategory = document.getElementById("filter-category");
  if (filterCategory) filterCategory.addEventListener("change", async (event) => {
    state.filters.category = event.target.value;
    await refreshData();
    render();
  });
  const filterOperation = document.getElementById("filter-operation");
  if (filterOperation) filterOperation.addEventListener("change", async (event) => {
    state.filters.operation = event.target.value;
    await refreshData();
    render();
  });

  document.querySelectorAll("[data-select-id]").forEach((row) => {
    row.addEventListener("click", () => {
      state.selectedId = row.dataset.selectId;
      render();
    });
  });

  document.querySelectorAll("[data-edit-id]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      openEditModal(btn.dataset.editId);
    });
  });

  document.querySelectorAll("[data-delete-id]").forEach((btn) => {
    btn.addEventListener("click", (event) => {
      event.stopPropagation();
      handleDelete(btn.dataset.deleteId);
    });
  });

  document.querySelectorAll("[data-sync-action]").forEach((btn) => {
    btn.addEventListener("click", () => handleSync(btn.dataset.syncAction));
  });

  document.querySelectorAll("[data-set-primary]").forEach((btn) => {
    btn.addEventListener("click", () => {
      updateEditingField("primary_image_url", btn.dataset.setPrimary);
      updateEditingField("meta_image_url", btn.dataset.setPrimary);
      render();
    });
  });

  document.querySelectorAll("[data-remove-media-url]").forEach((btn) => {
    btn.addEventListener("click", () => {
      removeMediaFromEditing(btn.dataset.removeMediaUrl, btn.dataset.removeMediaType || "image");
    });
  });

  bindLiveEditor();
}

boot();
