# Redeploy Ubuntu

## 1. Restaurar codigo y dependencias

```bash
git clone <repo> /opt/dashboard
cd /opt/dashboard/back
cp .env.production.example .env
uv sync
cd /opt/dashboard/front
npm install
npm run build
```

## 2. Restaurar estado local

- Base SQLite: copiar `back/storage/dashboard.db`
- Reportes: copiar `back/storage/uploads`
- Verificar permisos de escritura sobre `back/storage/`

## 3. Publicar backend

```bash
sudo cp /opt/dashboard/deploy/dashboard-api.service.example /etc/systemd/system/dashboard-api.service
sudo systemctl daemon-reload
sudo systemctl enable --now dashboard-api
sudo systemctl status dashboard-api
```

## 4. Publicar frontend con nginx

```bash
sudo cp /opt/dashboard/deploy/nginx.dashboard.monitoreocotaba.conf.example /etc/nginx/sites-available/dashboard.monitoreocotaba.com
sudo ln -s /etc/nginx/sites-available/dashboard.monitoreocotaba.conf.example /etc/nginx/sites-enabled/dashboard.monitoreocotaba.com
sudo nginx -t
sudo systemctl reload nginx
```

## 5. Smoke test antes de DNS

```bash
cd /opt/dashboard
python3 scripts/smoke_test.py --base-url http://127.0.0.1:8000/api --username admin --password 'Admin123!' --company ismocol
```

## 6. Checklist de corte

- `GET /api/healthz` responde `200`
- `GET /api/readyz` responde `200`
- login exitoso
- `GET /api/auth/me` con cookie de sesion
- `GET /api/dashboard?company=ismocol`
- `GET /api/feed?company=ismocol`
- despues de eso, cambiar DNS al nuevo destino
