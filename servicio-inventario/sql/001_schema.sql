-- =====================================================================
-- Servicio de inventario del taller — esquema PostgreSQL
--
-- Principios:
--   * El stock NUNCA se guarda como número editable: es la suma de `movements`.
--   * `movements` es inmutable (un trigger impide UPDATE/DELETE).
--   * Cada evento se procesa en UNA transacción que incluye el INSERT en
--     `processed_events`; si el event_id ya existe, la transacción no hace nada.
--   * Los eventos de salida se escriben en `outbox` dentro de la misma
--     transacción y un proceso aparte los publica a `inventory.events`.
-- =====================================================================

-- ---------------------------------------------------------------------
-- 1. Control de eventos (idempotencia, desorden, salida)
-- ---------------------------------------------------------------------

-- event_id ya aplicados. Procesar dos veces el mismo evento = no hacer nada.
CREATE TABLE processed_events (
    event_id      uuid        PRIMARY KEY,
    event_type    text        NOT NULL,
    topic         text        NOT NULL,
    processed_at  timestamptz NOT NULL DEFAULT now()
);

-- Eventos que llegaron antes que su pieza, orden o línea de BOM.
-- Se reintentan; después de N intentos se mandan a inventory.dlq.
CREATE TABLE pending_events (
    id               bigserial   PRIMARY KEY,
    event_id         uuid        NOT NULL UNIQUE,
    topic            text        NOT NULL,
    event_type       text        NOT NULL,
    event_key        text        NOT NULL,
    payload          jsonb       NOT NULL,          -- el sobre completo
    waiting_for      text,                          -- ej. 'part:412', 'work_order:1203'
    attempts         int         NOT NULL DEFAULT 0,
    last_error       text,
    next_attempt_at  timestamptz NOT NULL DEFAULT now(),
    created_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX pending_events_next_idx ON pending_events (next_attempt_at);

-- Eventos de salida (stock.*) pendientes de publicar en inventory.events.
-- Se insertan en la misma transacción que el cambio: nunca se pierde ni se duplica.
CREATE TABLE outbox (
    id            bigserial   PRIMARY KEY,
    event_id      uuid        NOT NULL UNIQUE,
    event_type    text        NOT NULL,
    event_key     text        NOT NULL,
    payload       jsonb       NOT NULL,             -- el sobre completo, source = 'inventory'
    created_at    timestamptz NOT NULL DEFAULT now(),
    published_at  timestamptz
);
CREATE INDEX outbox_unpublished_idx ON outbox (id) WHERE published_at IS NULL;

-- ---------------------------------------------------------------------
-- 2. Catálogo (llega por shop.catalog)
-- ---------------------------------------------------------------------

CREATE TABLE parts (
    part_id     int         PRIMARY KEY,
    sku         text,                               -- puede ser null; puede repetirse
    sku_norm    text GENERATED ALWAYS AS (upper(btrim(sku))) STORED,
    name        text        NOT NULL,
    family      text,                               -- ej. REFACCIÓN, SERVICIO
    "group"     text,
    subgroup    text,
    unit_id     int,
    unit_name   text,
    image_url   text,
    active      boolean     NOT NULL DEFAULT true,
    updated_at  timestamptz NOT NULL DEFAULT now()
);
-- NO es unique: el catálogo real trae piezas distintas con el mismo sku.
CREATE INDEX parts_sku_norm_idx ON parts (sku_norm);

CREATE TABLE locations (
    location_id   int         PRIMARY KEY,
    code          text        NOT NULL UNIQUE,      -- lo que se escanea: U-101
    name          text        NOT NULL,
    is_workbench  boolean     NOT NULL DEFAULT false,
    active        boolean     NOT NULL DEFAULT true,
    updated_at    timestamptz NOT NULL DEFAULT now()
);

-- Modelos de equipo (los 5 que tienen lista de partes).
CREATE TABLE models (
    model_id    int         PRIMARY KEY,
    model_name  text        NOT NULL,
    brand       text,
    updated_at  timestamptz NOT NULL DEFAULT now()
);

-- Líneas de la lista de partes. bom.upserted trae la lista completa:
-- las líneas que ya no vienen se marcan active = false (no se borran,
-- porque necesidades viejas pueden apuntar a ellas).
CREATE TABLE bom_lines (
    bom_line_id   int         PRIMARY KEY,
    model_id      int         NOT NULL REFERENCES models,
    part_id       int,                              -- null = pieza sin identificar
    name          text        NOT NULL,
    group_name    text,                             -- kit; si es null, el kit es por name
    unit          text,
    qty_per_unit  int,                              -- se usa cuando la inspección trae quantity null
    sort_order    int         NOT NULL DEFAULT 0,
    active        boolean     NOT NULL DEFAULT true
);
CREATE INDEX bom_lines_model_idx ON bom_lines (model_id);

-- Política de reabasto por pieza (nivel intermedio del reto).
CREATE TABLE stock_policies (
    part_id         int  PRIMARY KEY REFERENCES parts,
    min_qty         int  NOT NULL DEFAULT 0 CHECK (min_qty >= 0),
    max_qty         int  CHECK (max_qty IS NULL OR max_qty >= min_qty),
    reorder_qty     int  CHECK (reorder_qty IS NULL OR reorder_qty > 0),
    lead_time_days  int  CHECK (lead_time_days IS NULL OR lead_time_days >= 0)
);

-- ---------------------------------------------------------------------
-- 3. Taller (llega por shop.work_orders y shop.inspections)
-- ---------------------------------------------------------------------

CREATE TABLE work_orders (
    work_order_id  int         PRIMARY KEY,
    code           text        NOT NULL UNIQUE,     -- R061026012
    model_id       int,
    department     text CHECK (department IN ('spindles','motors','electronics','torque_center')),
    stage          text,
    received_at    timestamptz,
    deleted_at     timestamptz                      -- work_order.deleted; null = viva
);

CREATE TABLE inspections (
    inspection_id  int         PRIMARY KEY,
    work_order_id  int         NOT NULL,
    kind           text        NOT NULL CHECK (kind IN ('quick','full')),
    status         text        NOT NULL CHECK (status IN ('submitted','approved','rejected','discarded','voided')),
    approved_by    int,
    approved_at    timestamptz,
    reason         text,                            -- de rejected / voided
    items          jsonb       NOT NULL DEFAULT '[]'
);
CREATE INDEX inspections_wo_idx ON inspections (work_order_id);

-- Necesidad vigente por (orden, línea de BOM). Una aprobación posterior que
-- trae la misma línea REEMPLAZA required_qty (regla 2 del contrato).
CREATE TABLE needs (
    work_order_id       int         NOT NULL,
    bom_line_id         int         NOT NULL,
    inspection_id       int         NOT NULL,       -- de qué inspección viene (para voided)
    inspection_item_id  int         NOT NULL,
    part_id             int,                        -- null = faltante sin identificar
    name                text        NOT NULL,
    required_qty        int,                        -- null = sin cantidad ni qty_per_unit
    active              boolean     NOT NULL DEFAULT true,
    updated_at          timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (work_order_id, bom_line_id)
);

-- ---------------------------------------------------------------------
-- 4. Libro mayor
-- ---------------------------------------------------------------------

CREATE TABLE movements (
    movement_id       bigserial   PRIMARY KEY,
    part_id           int         NOT NULL REFERENCES parts,
    location_id       int         NOT NULL REFERENCES locations,
    quantity          int         NOT NULL CHECK (quantity <> 0),   -- con signo
    type              text        NOT NULL CHECK (type IN
                        ('receipt','issue','transfer_out','transfer_in','adjustment')),
    reference         text,                         -- orden, línea de compra o motivo
    work_order_id     int,
    purchase_line_id  int,
    unit_cost         numeric(14,2),
    currency          text CHECK (currency IN ('MXN','USD')),
    issued_by         text,
    transfer_id       uuid,                         -- une transfer_out con transfer_in
    source_event_id   uuid,                         -- evento de entrada que lo originó
    occurred_at       timestamptz NOT NULL,
    recorded_at       timestamptz NOT NULL DEFAULT now()
);
CREATE INDEX movements_part_loc_idx ON movements (part_id, location_id);
CREATE INDEX movements_part_time_idx ON movements (part_id, occurred_at DESC, movement_id DESC);
-- Una línea de compra solo puede entrar una vez (segundo candado de idempotencia).
CREATE UNIQUE INDEX movements_one_receipt_per_line
    ON movements (purchase_line_id) WHERE type = 'receipt';

-- Inmutable: un error se corrige con otro movimiento, nunca editando.
CREATE FUNCTION movements_immutable() RETURNS trigger LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'movements es inmutable: registra un ajuste en su lugar';
END $$;
CREATE TRIGGER movements_no_update BEFORE UPDATE OR DELETE ON movements
    FOR EACH ROW EXECUTE FUNCTION movements_immutable();

-- ---------------------------------------------------------------------
-- 5. Reservas, faltantes, recepciones sin relacionar, sugerencias
-- ---------------------------------------------------------------------

-- Reserva a nivel pieza (sin ubicación); la API reporta reserved = 0 por ubicación.
CREATE TABLE reservations (
    reservation_id      bigserial   PRIMARY KEY,
    work_order_id       int         NOT NULL,
    bom_line_id         int         NOT NULL,
    inspection_item_id  int         NOT NULL,
    part_id             int         NOT NULL REFERENCES parts,
    quantity            int         NOT NULL CHECK (quantity > 0),
    issued_qty          int         NOT NULL DEFAULT 0 CHECK (issued_qty >= 0),
    status              text        NOT NULL DEFAULT 'active'
                          CHECK (status IN ('active','fulfilled','cancelled')),
    created_at          timestamptz NOT NULL DEFAULT now(),
    updated_at          timestamptz NOT NULL DEFAULT now(),
    CHECK (issued_qty <= quantity)
);
CREATE INDEX reservations_active_part_idx ON reservations (part_id) WHERE status = 'active';
CREATE INDEX reservations_wo_idx ON reservations (work_order_id, bom_line_id);

CREATE TABLE shortages (
    shortage_id         bigserial   PRIMARY KEY,
    work_order_id       int         NOT NULL,
    bom_line_id         int         NOT NULL,
    inspection_item_id  int         NOT NULL,
    part_id             int,                        -- null = sin identificar
    name                text        NOT NULL,
    missing_quantity    int,                        -- null = cantidad desconocida
    status              text        NOT NULL DEFAULT 'open'
                          CHECK (status IN ('open','resolved','cancelled')),
    opened_at           timestamptz NOT NULL DEFAULT now(),
    closed_at           timestamptz
);
-- Una sola fila abierta por (orden, línea): si cambia, se actualiza.
CREATE UNIQUE INDEX shortages_one_open_per_line
    ON shortages (work_order_id, bom_line_id) WHERE status = 'open';
-- Para surtir del más antiguo al más nuevo (regla 12).
CREATE INDEX shortages_open_part_idx ON shortages (part_id, opened_at) WHERE status = 'open';

CREATE TABLE unmatched_receipts (
    id                   bigserial   PRIMARY KEY,
    purchase_line_id     int         NOT NULL UNIQUE,
    purchase_request_id  int         NOT NULL,
    request_number       text        NOT NULL,
    part_number          text        NOT NULL,
    description          text        NOT NULL,
    quantity             int         NOT NULL,
    unit_price           numeric(14,2),
    currency             text CHECK (currency IN ('MXN','USD')),
    work_order_code      text,
    received_at          timestamptz NOT NULL,
    status               text        NOT NULL DEFAULT 'pending'
                           CHECK (status IN ('pending','resolved')),
    candidate_part_ids   int[],                     -- si coincidió con varias piezas
    resolved_part_id     int REFERENCES parts,
    resolved_at          timestamptz
);

-- Sugerencia vigente por pieza; se recalcula cuando cambian faltantes o saldo.
CREATE TABLE reorder_suggestions (
    part_id             int         PRIMARY KEY REFERENCES parts,
    suggested_quantity  int         NOT NULL CHECK (suggested_quantity >= 1),
    work_order_ids      int[]       NOT NULL DEFAULT '{}',
    updated_at          timestamptz NOT NULL DEFAULT now()
);

-- ---------------------------------------------------------------------
-- 6. Vistas: el saldo se CALCULA, no se guarda
-- ---------------------------------------------------------------------

-- on_hand por pieza y ubicación.
CREATE VIEW v_stock_by_location AS
SELECT m.part_id, m.location_id, l.code, SUM(m.quantity)::int AS on_hand
FROM movements m JOIN locations l USING (location_id)
GROUP BY m.part_id, m.location_id, l.code;

-- Totales por pieza: on_hand, reserved, available.
CREATE VIEW v_availability AS
SELECT p.part_id,
       COALESCE(s.on_hand, 0)                          AS on_hand,
       COALESCE(r.reserved, 0)                         AS reserved,
       COALESCE(s.on_hand, 0) - COALESCE(r.reserved, 0) AS available
FROM parts p
LEFT JOIN (SELECT part_id, SUM(on_hand)::int AS on_hand
           FROM v_stock_by_location GROUP BY part_id) s USING (part_id)
LEFT JOIN (SELECT part_id, SUM(quantity - issued_qty)::int AS reserved
           FROM reservations WHERE status = 'active' GROUP BY part_id) r USING (part_id);

-- Kardex con saldo corrido (todas las ubicaciones), del más reciente al más antiguo.
CREATE VIEW v_ledger AS
SELECT m.*,
       SUM(m.quantity) OVER (PARTITION BY m.part_id
                             ORDER BY m.occurred_at, m.movement_id)::int AS balance
FROM movements m;
