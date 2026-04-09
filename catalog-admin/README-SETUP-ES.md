# Panel Admin de Catálogo - LV Inmobiliaria

Este panel se agregó **sin tocar el bot actual**.  
El bot permanece igual y este servicio funciona aparte para:

- agregar propiedades
- editar propiedades
- eliminar propiedades
- exportar/importar `PROPERTY_CATALOG_JSON`
- sincronizar el catálogo del bot en Render
- sincronizar el catálogo de Meta con los mismos IDs

## Carpeta a desplegar

Despliega **solo esta carpeta** como servicio aparte:

`catalog-admin`

## Cómo usarlo en Render

### Opción recomendada
Crea **un servicio web nuevo** y en `Root Directory` selecciona:

`catalog-admin`

### Build / Start
No necesitas build especial.

- Build Command: dejar vacío o `npm install`
- Start Command: `npm start`

## Variables de entorno mínimas

### Acceso al panel
- `ADMIN_PANEL_USERNAME`
- `ADMIN_PANEL_PASSWORD`
- `ADMIN_PANEL_SESSION_SECRET`

### Nombre negocio
- `BUSINESS_NAME=LV Inmobiliaria`

## Variables para sincronizar el bot en Render

- `RENDER_API_KEY`
- `RENDER_BOT_SERVICE_ID`
- `RENDER_BOT_ENV_KEY=PROPERTY_CATALOG_JSON`
- `RENDER_BOT_DEPLOY_HOOK_URL` (opcional pero recomendado)

## Variables para sincronizar Meta

- `META_ACCESS_TOKEN`
- `META_CATALOG_ID`
- `META_GRAPH_VERSION=v23.0`
- `META_DEFAULT_URL`
- `META_DEFAULT_IMAGE_URL`
- `META_DEFAULT_AVAILABILITY=in stock`

## Seed inicial

Puedes arrancar de 3 formas:

### 1. Pegar el catálogo actual como env
Usa:

`INITIAL_PROPERTY_CATALOG_JSON`

### 2. Colocarlo en archivo seed
Archivo:

`data/properties.seed.json`

### 3. Importarlo manualmente desde el panel
Usando el botón **Importar JSON**.

## Importante sobre Meta

Para que una propiedad se sincronice bien a Meta desde el panel, conviene llenar:

- `retailer_id`
- `title`
- `price`
- `currency`
- `meta_url`
- `meta_image_url`

## Importante sobre Render

Este panel intenta actualizar `PROPERTY_CATALOG_JSON` en el servicio del bot y luego lanzar deploy.

Si tu cuenta / endpoint de Render cambia comportamiento, el panel te mostrará el error en pantalla para que sepas exactamente dónde falló.

## Seguridad

Cambia de inmediato:

- usuario
- contraseña
- session secret
- token de Meta
- token de Render

Y si mostraste tokens en capturas, lo recomendable es regenerarlos.
