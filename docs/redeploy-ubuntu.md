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

## 3. Publicar API y worker

```bash
sudo cp /opt/dashboard/deploy/dashboard-api.service.example /etc/systemd/system/dashboard-api.service
sudo cp /opt/dashboard/deploy/dashboard-worker.service.example /etc/systemd/system/dashboard-worker.service
sudo systemctl daemon-reload
sudo systemctl enable --now dashboard-api dashboard-worker
sudo systemctl status dashboard-api
sudo systemctl status dashboard-worker
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
- `dashboard-api.service` y `dashboard-worker.service` estan activos con PID y memoria independientes
- `GET /api/admin/jobs` muestra heartbeat vigente para cualquier job `running`
- login exitoso
- `GET /api/auth/me` con cookie de sesion
- `GET /api/dashboard?company=ismocol`
- `GET /api/feed?company=ismocol`
- despues de eso, cambiar DNS al nuevo destino
