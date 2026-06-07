# Langtaosha Frontend

Independent frontend for the Langtaosha search API.

## Local Development

Start the backend first. The backend API defaults to port `5173`:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD
FRONTEND_ALLOWED_ORIGINS=http://localhost:5004 \
PD_BACKEND_CONFIG=src/config/config_tecent_backend_server_mimic.yaml \
python app/main.py
```

Start the frontend. The frontend defaults to port `5004`:

```bash
cd /home/wnlab/langtaosha/Langtaosha_PD/frontend
npm install
npm run dev
```

If the backend is started with `API_AUTH_TOKEN` or `API_AUTH_TOKENS`, keep the
same token available to the Vite proxy:

```bash
VITE_BACKEND_PROXY_TARGET=http://127.0.0.1:5173 \
VITE_BACKEND_API_TOKEN=<TEST_API_TOKEN> \
npm run dev
```

Open:

```text
http://localhost:5004
```

During the migration window, the public pages on port `5004` intentionally keep
the previous Flask-rendered experience by proxying these routes to the backend:

```text
/
/search
/study
/future
/show_page
/span-matcher
/feedback-review
/static
```

Port `5173` is the backend API port. Direct browser access to legacy page routes
on `5173`, such as `/search` or `/study`, returns 404 by default. The Vite proxy
adds an internal legacy-page header when `5004` needs to fetch those pages during
the migration window.

By default the frontend calls same-origin `/api`, and Vite proxies it to:

```text
http://127.0.0.1:5173
```

Override the proxy target with:

```text
VITE_BACKEND_PROXY_TARGET=http://127.0.0.1:5173
```

You can bypass the same-origin proxy and call a backend URL directly with:

```text
VITE_API_BASE_URL=http://localhost:5173
```

The React MVP remains in `src/`, but it is not the default public page while
the old UI must stay visually unchanged.

## Build

```bash
npm run build
```

The static build output is written to `dist/`.
