"""Guardian: servicio de inventario del taller.

Consume los eventos del taller (catálogo, órdenes, inspecciones y compras), lleva el libro
mayor de existencias en PostgreSQL, reserva material para las órdenes, detecta faltantes,
sugiere compras y publica sus propios eventos en `inventory.events`.
"""

__version__ = "1.0.0"
