const ADMIN_BASE = window.ADMIN_BASE_PATH || "/admin";
const API_BASE = `${ADMIN_BASE}/api`;

const state = {
  authenticated: false,
  username: null,
  loading: false,
  properties: [],
  stats: { total: 0, active: 0, alquiler: 0, venta: 0, metaReady: 0 },
  config: null,
  filters: { q: "", category: "", operation: "", active: "" },
  selectedId: null,
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
  meta_url: "",
  meta_image_url: "",
  meta_availability: "in stock",
};

const app = document.getElementById("app");
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
  const el = document.createElement("div");
  el.className = `toast ${type}`;
  el.textContent = message;
  toastLayer.appendChild(el);
  setTimeout(() => {
    el.style.opacity = "0";
    el.style.transform = "translateY(-6px)";
    el.style.transition = "0.25s ease";
    setTimeout(() => el.remove(), 250);
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
      "Content-Type": "application/json",
      ...(options.headers || {}),
    },
    ...options,
  });

  const contentType = res.headers.get("content-type") || "";
  const data = contentType.includes("application/json") ? await res.json() : await res.text();
  if (!res.ok) {
    const message = typeof data === "string" ? data : data?.error || data?.errors?.join("\n") || "Ocurrió un error";
    throw new Error(message);
  }
  return data;
}

function propertyToForm(property) {
  if (!property) return { ...defaultForm };
  return {
    ...defaultForm,
    ...property,
    features: Array.isArray(property.features) ? property.features.join("\n") : "",
    nearby_places: Array.isArray(property.nearby_places) ? property.nearby_places.join("\n") : "",
  };
}

function findSelected() {
  return state.properties.find((item) => item.id === state.selectedId) || state.properties[0] || null;
}

function render() {
  document.body.classList.toggle("modal-open", !!state.editing || state.importOpen);
  app.innerHTML = state.authenticated ? renderDashboard() : renderLogin();
  bindEvents();
}

function renderLogin() {
  return `
    <div class="login-shell">
      <div class="login-card">
        <div class="brand-row">
          <div class="brand-badge">LV</div>
          <div class="brand-copy">
            <h1>Panel Admin</h1>
            <p>Panel visual para gestionar propiedades, sincronizar con el bot y actualizar Meta.</p>
          </div>
        </div>
        <form id="login-form" class="form-grid">
          <label class="label">Usuario
            <input class="input" name="username" placeholder="admin" autocomplete="username" />
          </label>
          <label class="label">Contraseña
            <input class="input" type="password" name="password" placeholder="••••••••" autocomplete="current-password" />
          </label>
          <div class="button-row">
            <button class="btn btn-primary" type="submit">Entrar al panel</button>
          </div>
          <div class="info-box">Este panel funciona dentro del mismo servicio del bot, con vista oscura, compacta y sincronización centralizada.</div>
          <p class="login-hint">Usa credenciales seguras antes de ponerlo en producción.</p>
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
  return state.properties
    .map((item) => {
      const selected = item.id === state.selectedId ? 'style="background: rgba(255,138,28,0.08);"' : "";
      return `
      <tr ${selected} data-select-id="${escapeHtml(item.id)}">
        <td>
          <strong>${escapeHtml(item.title || item.code)}</strong>
          <small>${escapeHtml(item.id)}</small>
        </td>
        <td><span class="badge ${escapeHtml(item.operation || "neutral")}">${escapeHtml(item.operation || "-")}</span></td>
        <td>${escapeHtml(item.category || "-")}</td>
        <td>${escapeHtml(item.location || "-")}</td>
        <td>${escapeHtml(item.price || "-")} ${escapeHtml(item.currency || "")}</td>
        <td>${escapeHtml(item.agent_name || "-")}</td>
        <td><span class="badge ${item.active ? "active" : "inactive"}">${item.active ? "Activa" : "Inactiva"}</span></td>
        <td>
          <div class="button-row">
            <button class="btn btn-secondary btn-small" data-edit-id="${escapeHtml(item.id)}">Editar</button>
            <button class="btn btn-danger btn-small" data-delete-id="${escapeHtml(item.id)}">Eliminar</button>
          </div>
        </td>
      </tr>`;
    })
    .join("");
}

function renderPreviewPanel() {
  const selected = findSelected();
  if (!selected) {
    return `<div class="preview-panel"><div class="empty-state">Selecciona una propiedad para ver el detalle.</div></div>`;
  }

  const meta = {
    retailer_id: selected.retailer_id || selected.code || selected.id,
    name: `${selected.operation === "alquiler" ? "ALQUILER" : "VENTA"} | ${selected.title || selected.code}`,
    price: String(selected.price || "").replace(/[^\d]/g, ""),
    currency: selected.currency || "DOP",
    url: selected.meta_url || "",
    image_url: selected.meta_image_url || "",
  };

  return `
    <div class="preview-panel">
      <div class="section-head">
        <div>
          <h3>${escapeHtml(selected.title || selected.code)}</h3>
          <p class="muted">${escapeHtml(selected.location || "Sin ubicación")}</p>
        </div>
        <span class="badge ${selected.active ? "active" : "inactive"}">${selected.active ? "Activa" : "Inactiva"}</span>
      </div>
      <div class="meta-list">
        <div class="meta-item">
          <strong>Resumen rápido</strong>
          <div class="muted">${escapeHtml(selected.short_description || "Sin descripción")}</div>
        </div>
        <div class="meta-item">
          <strong>IDs sincronizados</strong>
          <div><code>id:</code> ${escapeHtml(selected.id)}</div>
          <div><code>retailer_id:</code> ${escapeHtml(selected.retailer_id)}</div>
          <div><code>code:</code> ${escapeHtml(selected.code)}</div>
        </div>
        <div class="meta-item">
          <strong>Preview para Meta</strong>
          <pre>${escapeHtml(JSON.stringify(meta, null, 2))}</pre>
        </div>
      </div>
    </div>
  `;
}

function syncCard(title, description, ready, statusMessage, statusOk, buttonLabel, action) {
  return `
    <div class="sync-card">
      <div class="section-head">
        <div>
          <h4>${escapeHtml(title)}</h4>
          <p>${escapeHtml(description)}</p>
        </div>
        <span class="badge ${ready ? "active" : "inactive"}">${ready ? "Configurado" : "Pendiente"}</span>
      </div>
      <div class="info-box">${escapeHtml(statusMessage || "Sin actividad")}</div>
      <div class="button-row">
        <button class="btn ${ready ? "btn-primary" : "btn-secondary"}" ${ready ? "" : "disabled"} data-sync-action="${escapeHtml(action)}">${escapeHtml(buttonLabel)}</button>
      </div>
      ${statusOk === false ? `<div class="error-box">Último resultado con error.</div>` : statusOk === true ? `<div class="success-box">Última ejecución correcta.</div>` : ""}
    </div>
  `;
}

function renderDashboard() {
  const selected = findSelected();
  const config = state.config || { integrations: {}, syncState: {} };
  const syncState = config.syncState || {};

  return `
    <div class="dashboard-shell">
      <aside class="sidebar">
        <div class="brand-row">
          <div class="brand-badge">LV</div>
          <div class="brand-copy">
            <h2>${escapeHtml(config.businessName || "LV Inmobiliaria")}</h2>
            <p>Panel Admin</p>
          </div>
        </div>
        <div class="sidebar-nav">
          <div class="nav-chip active">Propiedades</div>
          <div class="nav-chip">Sincronización</div>
          <div class="nav-chip">Vista previa Meta</div>
        </div>
        <div class="info-box">
          <strong>Usuario:</strong> ${escapeHtml(state.username || "-")}
          <br />
          <strong>Bot env:</strong> ${escapeHtml(config.integrations?.renderBotEnvKey || "PROPERTY_CATALOG_JSON")}
        </div>
        <div class="button-row">
          <button class="btn btn-ghost" id="logout-btn">Cerrar sesión</button>
        </div>
      </aside>

      <main class="main">
        <section class="topbar">
          <div class="topbar-row">
            <div>
              <h1>Panel admin de propiedades</h1>
              <p>Gestiona, edita y sincroniza el catálogo del bot y el catálogo de Meta desde un panel limpio, oscuro, compacto y funcional.</p>
            </div>
            <div class="button-row">
              <button class="btn btn-secondary" id="import-btn">Importar JSON</button>
              <button class="btn btn-secondary" id="import-meta-btn" ${config.integrations?.metaImportReady ? "" : "disabled"}>Importar desde Meta</button>
              <a class="btn btn-secondary" href="${ADMIN_BASE}/api/properties/export">Exportar JSON</a>
              <button class="btn btn-primary" id="new-btn">Nueva propiedad</button>
            </div>
          </div>
          <div class="stats-grid">
            ${statCard("Total", state.stats.total)}
            ${statCard("Activas", state.stats.active)}
            ${statCard("Alquiler", state.stats.alquiler)}
            ${statCard("Venta", state.stats.venta)}
          </div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Listado principal</h3>
              <p class="muted">Busca rápido, filtra por tipo y mantén los IDs alineados entre bot y Meta.</p>
            </div>
          </div>
          <div class="toolbar">
            <input class="input" id="filter-q" placeholder="Buscar por nombre, id, code, ubicación, agente..." value="${escapeHtml(state.filters.q)}" />
            <select class="select" id="filter-category">
              <option value="">Todas las categorías</option>
              ${["apartamentos","casas","solares","proyectos","locales_comerciales","venta","alquiler"].map((opt) => `<option value="${opt}" ${state.filters.category === opt ? "selected" : ""}>${opt}</option>`).join("")}
            </select>
            <select class="select" id="filter-operation">
              <option value="">Todas las operaciones</option>
              <option value="venta" ${state.filters.operation === "venta" ? "selected" : ""}>venta</option>
              <option value="alquiler" ${state.filters.operation === "alquiler" ? "selected" : ""}>alquiler</option>
            </select>
            <select class="select" id="filter-active">
              <option value="">Todos los estados</option>
              <option value="true" ${state.filters.active === "true" ? "selected" : ""}>Activas</option>
              <option value="false" ${state.filters.active === "false" ? "selected" : ""}>Inactivas</option>
            </select>
          </div>
          <div class="layout-2">
            <div class="table-wrap">
              <table class="table">
                <thead>
                  <tr>
                    <th>Propiedad</th>
                    <th>Operación</th>
                    <th>Categoría</th>
                    <th>Ubicación</th>
                    <th>Precio</th>
                    <th>Agente</th>
                    <th>Estado</th>
                    <th>Acciones</th>
                  </tr>
                </thead>
                <tbody>${renderRows()}</tbody>
              </table>
            </div>
            ${renderPreviewPanel()}
          </div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Sincronización centralizada</h3>
              <p class="muted">Aplica cambios al bot y al catálogo de Meta desde este mismo panel.</p>
            </div>
            <div class="button-row">
              <button class="btn btn-primary" ${config.integrations?.renderReady && config.integrations?.metaReady ? "" : "disabled"} data-sync-action="all">Sincronizar todo</button>
            </div>
          </div>
          <div class="sync-grid">
            ${syncCard(
              "Bot / Render",
              "Actualiza PROPERTY_CATALOG_JSON y dispara deploy del bot.",
              !!config.integrations?.renderReady,
              syncState.lastBotSyncMessage,
              syncState.lastBotSyncOk,
              "Sincronizar bot",
              "bot"
            )}
            ${syncCard(
              "Meta → Panel",
              "Lee las propiedades ya existentes en Meta y las carga aquí sin duplicarlas por retailer_id.",
              !!config.integrations?.metaImportReady,
              syncState.lastMetaImportMessage,
              syncState.lastMetaImportOk,
              "Importar desde Meta",
              "meta-import"
            )}
            ${syncCard(
              "Meta Catalog",
              "Empuja los productos del panel al catálogo usando catalog_management.",
              !!config.integrations?.metaReady,
              syncState.lastMetaSyncMessage,
              syncState.lastMetaSyncOk,
              "Sincronizar Meta",
              "meta"
            )}
          </div>
        </section>

        <section class="section-card">
          <div class="section-head">
            <div>
              <h3>Vista previa técnica</h3>
              <p class="muted">Ideal para revisar payloads antes de publicar.</p>
            </div>
            ${selected ? `<button class="btn btn-secondary" data-preview-id="${escapeHtml(selected.id)}">Actualizar preview Meta</button>` : ""}
          </div>
          <div id="meta-preview-slot">${selected ? `<div class="preview-panel"><pre>${escapeHtml(JSON.stringify(selected, null, 2))}</pre></div>` : `<div class="empty-state">No hay propiedad seleccionada.</div>`}</div>
        </section>
      </main>

      ${renderPropertyModal()}
      ${renderImportModal()}
    </div>
  `;
}

function renderField(name, label, value, type = "text", className = "") {
  return `
    <label class="label ${className}">${escapeHtml(label)}
      <input class="input" name="${escapeHtml(name)}" type="${escapeHtml(type)}" value="${escapeHtml(value ?? "")}" />
    </label>
  `;
}

function renderTextArea(name, label, value, className = "") {
  return `
    <label class="label ${className}">${escapeHtml(label)}
      <textarea class="textarea" name="${escapeHtml(name)}">${escapeHtml(value ?? "")}</textarea>
    </label>
  `;
}

function renderSelect(name, label, value, options, className = "") {
  return `
    <label class="label ${className}">${escapeHtml(label)}
      <select class="select" name="${escapeHtml(name)}">
        ${options.map((opt) => `<option value="${escapeHtml(opt.value)}" ${String(value) === String(opt.value) ? "selected" : ""}>${escapeHtml(opt.label)}</option>`).join("")}
      </select>
    </label>
  `;
}

function renderPropertyModal() {
  if (!state.editing) return `<div class="modal-backdrop" id="property-modal"></div>`;
  const form = state.editing;
  return `
    <div class="modal-backdrop open" id="property-modal">
      <div class="modal">
        <div class="modal-head">
          <div class="modal-title">
            <div>
              <h2>${form.__mode === "create" ? "Nueva propiedad" : "Editar propiedad"}</h2>
              <p class="muted">No se toca el bot. Este panel administra el catálogo aparte y lo sincroniza cuando tú lo decidas.</p>
            </div>
            <button class="btn btn-ghost" id="close-modal-btn">Cerrar</button>
          </div>
        </div>
        <form id="property-form">
          <div class="modal-body">
            <section class="form-sections">
              <div class="section-card">
                <div class="section-head"><h3>Base</h3></div>
                <div class="field-grid">
                  ${renderField("id", "ID interno", form.id)}
                  ${renderField("retailer_id", "retailer_id / Meta ID", form.retailer_id)}
                  ${renderField("code", "Código", form.code)}
                  ${renderField("title", "Título", form.title, "text", "span-2")}
                  ${renderSelect("category", "Categoría", form.category, [
                    { value: "apartamentos", label: "apartamentos" },
                    { value: "casas", label: "casas" },
                    { value: "solares", label: "solares" },
                    { value: "proyectos", label: "proyectos" },
                    { value: "locales_comerciales", label: "locales_comerciales" },
                  ])}
                  ${renderSelect("operation", "Operación", form.operation, [
                    { value: "venta", label: "venta" },
                    { value: "alquiler", label: "alquiler" },
                  ])}
                  ${renderField("price", "Precio", form.price)}
                  ${renderField("currency", "Moneda", form.currency)}
                  ${renderField("location", "Ubicación", form.location, "text", "span-2")}
                  ${renderField("exact_address", "Dirección exacta", form.exact_address, "text", "span-2")}
                  ${renderField("exact_location_reference", "Referencia de ubicación", form.exact_location_reference, "text", "span-2")}
                </div>
              </div>

              <div class="section-card">
                <div class="section-head"><h3>Características</h3></div>
                <div class="field-grid">
                  ${renderField("bedrooms", "Habitaciones", form.bedrooms, "number")}
                  ${renderField("bathrooms", "Baños", form.bathrooms, "number")}
                  ${renderField("parking", "Parqueos", form.parking, "number")}
                  ${renderField("floor_level", "Nivel / piso", form.floor_level)}
                  ${renderField("area_m2", "Área m²", form.area_m2, "number")}
                  ${renderField("lot_m2", "Solar m²", form.lot_m2, "number")}
                  ${renderField("construction_m2", "Construcción m²", form.construction_m2, "number")}
                  ${renderField("duration_min", "Duración visita (min)", form.duration_min, "number")}
                  ${renderTextArea("short_description", "Descripción corta", form.short_description, "span-4")}
                  ${renderTextArea("features", "Features / amenidades (una por línea)", form.features, "span-2")}
                  ${renderTextArea("nearby_places", "Lugares cercanos (uno por línea)", form.nearby_places, "span-2")}
                </div>
              </div>

              <div class="section-card">
                <div class="section-head"><h3>Legal y servicios</h3></div>
                <div class="field-grid">
                  ${renderField("year_built", "Año construcción", form.year_built)}
                  ${renderField("condition", "Condición", form.condition)}
                  ${renderField("legal_status", "Estado legal", form.legal_status)}
                  ${renderField("safety", "Seguridad", form.safety)}
                  ${renderField("transport_access", "Acceso / transporte", form.transport_access, "text", "span-2")}
                  ${renderField("bank_financing_note", "Nota financiamiento", form.bank_financing_note, "text", "span-2")}
                  ${renderField("down_payment", "Inicial / separación", form.down_payment)}
                  ${renderField("payment_facilities", "Facilidades de pago", form.payment_facilities)}
                  ${renderField("estimated_monthly_fee", "Cuota mensual aprox.", form.estimated_monthly_fee)}
                  ${renderField("transfer_cost", "Costo traspaso", form.transfer_cost)}
                  ${renderSelect("title_deed", "Título deslindado", String(form.title_deed), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("documents_up_to_date", "Documentos al día", String(form.documents_up_to_date), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("bank_financing", "Financiamiento bancario", String(form.bank_financing), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("water_service", "Servicio de agua", String(form.water_service), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("electric_service", "Servicio eléctrico", String(form.electric_service), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("paved_street", "Calle asfaltada", String(form.paved_street), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                  ${renderSelect("sewer", "Cloaca", String(form.sewer), [
                    { value: "", label: "No especificado" },
                    { value: "true", label: "Sí" },
                    { value: "false", label: "No" },
                  ])}
                </div>
              </div>

              <div class="section-card">
                <div class="section-head"><h3>Agente y Meta</h3></div>
                <div class="field-grid">
                  ${renderField("agent_name", "Nombre del agente", form.agent_name, "text", "span-2")}
                  ${renderField("agent_phone", "Teléfono del agente", form.agent_phone)}
                  ${renderField("status", "Status", form.status)}
                  ${renderField("meta_url", "URL pública para Meta", form.meta_url, "text", "span-2")}
                  ${renderField("meta_image_url", "URL imagen principal Meta", form.meta_image_url, "text", "span-2")}
                  ${renderField("meta_availability", "Availability Meta", form.meta_availability)}
                  <label class="label switch-row span-2">
                    <input type="checkbox" name="active" ${form.active ? "checked" : ""} />
                    <span>Propiedad activa</span>
                  </label>
                </div>
              </div>
            </section>
          </div>
          <div class="modal-foot">
            <div class="button-row">
              <button class="btn btn-secondary" type="button" id="cancel-modal-btn">Cancelar</button>
              <button class="btn btn-primary" type="submit">Guardar propiedad</button>
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
      <div class="modal" style="width:min(900px,100%)">
        <div class="modal-head">
          <div class="modal-title">
            <div>
              <h2>Importar PROPERTY_CATALOG_JSON</h2>
              <p class="muted">Pega aquí el array completo. Esto reemplaza el contenido actual del panel.</p>
            </div>
            <button class="btn btn-ghost" id="close-import-btn">Cerrar</button>
          </div>
        </div>
        <div class="modal-body">
          <textarea class="textarea" id="import-json" style="min-height:420px">${escapeHtml(state.importText)}</textarea>
        </div>
        <div class="modal-foot">
          <div class="button-row">
            <button class="btn btn-secondary" type="button" id="cancel-import-btn">Cancelar</button>
            <button class="btn btn-primary" type="button" id="confirm-import-btn">Importar catálogo</button>
          </div>
        </div>
      </div>
    </div>
  `;
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

async function handleLogin(event) {
  event.preventDefault();
  const formData = new FormData(event.currentTarget);
  try {
    await api("/api/auth/login", {
      method: "POST",
      body: JSON.stringify({
        username: formData.get("username"),
        password: formData.get("password"),
      }),
    });
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

async function handleSaveProperty(event) {
  event.preventDefault();
  const form = event.currentTarget;
  const formData = new FormData(form);
  const payload = Object.fromEntries(formData.entries());
  payload.active = formData.get("active") === "on";
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
  const endpoint = action === "all"
    ? "/api/sync/all"
    : action === "meta"
    ? "/api/sync/meta"
    : action === "meta-import"
    ? "/api/meta/import"
    : "/api/sync/bot";
  try {
    const result = await api(endpoint, { method: "POST" });
    await refreshData();
    render();
    showToast(result.message || "Sincronización completada", "success");
  } catch (error) {
    await refreshData().catch(() => {});
    render();
    showToast(error.message, "error");
  }
}

async function handleMetaPreview(id) {
  try {
    const result = await api(`/api/meta-preview/${encodeURIComponent(id)}`);
    const slot = document.getElementById("meta-preview-slot");
    if (slot) {
      slot.innerHTML = `<div class="preview-panel"><pre>${escapeHtml(JSON.stringify(result.metaPayload, null, 2))}</pre></div>`;
    }
  } catch (error) {
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

async function applyFilters() {
  await refreshData();
  render();
}

function debounce(fn, delay = 350) {
  let timer;
  return (...args) => {
    clearTimeout(timer);
    timer = setTimeout(() => fn(...args), delay);
  };
}

const debouncedFilter = debounce(applyFilters, 320);

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

  const closeModalBtn = document.getElementById("close-modal-btn");
  if (closeModalBtn) closeModalBtn.addEventListener("click", closePropertyModal);
  const cancelModalBtn = document.getElementById("cancel-modal-btn");
  if (cancelModalBtn) cancelModalBtn.addEventListener("click", closePropertyModal);
  const propertyForm = document.getElementById("property-form");
  if (propertyForm) propertyForm.addEventListener("submit", handleSaveProperty);

  const closeImportBtn = document.getElementById("close-import-btn");
  if (closeImportBtn) closeImportBtn.addEventListener("click", closeImportModal);
  const cancelImportBtn = document.getElementById("cancel-import-btn");
  if (cancelImportBtn) cancelImportBtn.addEventListener("click", closeImportModal);
  const confirmImportBtn = document.getElementById("confirm-import-btn");
  if (confirmImportBtn) confirmImportBtn.addEventListener("click", handleImportConfirm);

  const filterQ = document.getElementById("filter-q");
  if (filterQ) filterQ.addEventListener("input", (event) => {
    state.filters.q = event.target.value;
    debouncedFilter();
  });
  const filterCategory = document.getElementById("filter-category");
  if (filterCategory) filterCategory.addEventListener("change", (event) => {
    state.filters.category = event.target.value;
    applyFilters();
  });
  const filterOperation = document.getElementById("filter-operation");
  if (filterOperation) filterOperation.addEventListener("change", (event) => {
    state.filters.operation = event.target.value;
    applyFilters();
  });
  const filterActive = document.getElementById("filter-active");
  if (filterActive) filterActive.addEventListener("change", (event) => {
    state.filters.active = event.target.value;
    applyFilters();
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

  document.querySelectorAll("[data-preview-id]").forEach((btn) => {
    btn.addEventListener("click", () => handleMetaPreview(btn.dataset.previewId));
  });
}

boot();
