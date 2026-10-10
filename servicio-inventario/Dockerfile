# Se construye desde la raíz del kit para incluir contract/schemas:
#   docker compose -f servicio-inventario/docker-compose.yml up -d --build
FROM python:3.12-slim

ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    CONTRACT_DIR=/app/contract/schemas

WORKDIR /app/servicio-inventario
COPY servicio-inventario/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY contract/schemas /app/contract/schemas
COPY servicio-inventario/sql ./sql
COPY servicio-inventario/guardian ./guardian

EXPOSE 8000
ENTRYPOINT ["python", "-m", "guardian"]
CMD ["serve"]
