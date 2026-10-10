-- =====================================================================
-- 002 — Ajustes del servicio sobre el esquema base (001_schema.sql)
--
-- El esquema base se aplica sin cambios; aquí solo se agrega lo que el
-- servicio necesita para operar.
-- =====================================================================

-- La bandeja de salida también publica a inventory.dlq, con encabezados
-- (motivo del error, tópico de origen).
ALTER TABLE outbox
    ADD COLUMN topic   text  NOT NULL DEFAULT 'inventory.events',
    ADD COLUMN headers jsonb;

-- `code` deja de ser único: las pruebas de contrato crean ubicaciones con
-- code = 'U-' || (location_id % 1000), que se repite entre corridas. El
-- escaneo por código resuelve la ambigüedad prefiriendo la ubicación activa.
ALTER TABLE locations DROP CONSTRAINT locations_code_key;
CREATE INDEX locations_code_idx ON locations (code);

-- Ubicación de recepción (regla 11). El catálogo puede sobrescribirla.
INSERT INTO locations (location_id, code, name, is_workbench, active)
VALUES (100, 'U-100', 'Recepción de material', false, true)
ON CONFLICT (location_id) DO NOTHING;

-- SKU compactado (sin espacios ni signos) para relacionar compras cuyo
-- número de parte viene escrito de otra forma: "MN70 - 2 - 88" ↔ "MN70-2-88".
-- Lo calcula el servicio al recibir part.upserted.
ALTER TABLE parts ADD COLUMN sku_compact text;
CREATE INDEX parts_sku_compact_idx ON parts (sku_compact);

-- Cómo se relacionó una recepción pendiente y qué movimiento generó.
ALTER TABLE unmatched_receipts
    ADD COLUMN resolution   text CHECK (resolution IN ('manual', 'automatic')),
    ADD COLUMN movement_id  bigint;

-- Posición de lectura por tópico y partición. Se guarda en la misma
-- transacción que el efecto del evento: borrar la base reinicia la lectura
-- desde el principio (regla 23).
CREATE TABLE consumer_offsets (
    topic        text        NOT NULL,
    partition    int         NOT NULL,
    next_offset  bigint      NOT NULL,
    updated_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (topic, partition)
);

-- Orden por llave entre eventos encolados (un voided espera a su approved).
CREATE INDEX pending_events_key_idx ON pending_events (topic, event_key, id);

-- Búsquedas frecuentes del dominio.
CREATE INDEX needs_inspection_idx ON needs (work_order_id, inspection_id) WHERE active;
CREATE INDEX reservations_wo_part_idx ON reservations (work_order_id, part_id) WHERE status = 'active';
CREATE INDEX shortages_wo_idx ON shortages (work_order_id) WHERE status = 'open';
