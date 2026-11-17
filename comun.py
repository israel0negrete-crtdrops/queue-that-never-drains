"""Lo que comparten el productor, el consumidor y el muestreo.

La topología del laboratorio, a la vista:

    tele (direct) --posiciones--> posiciones ----(rechazo sin reencolar)----> tele.muertos --> posiciones.dlq
                                      ^
                                      | (al vencer 1 s)
    tele.espera (direct) ------> posiciones.espera   (reintentos con espera)

- Colas clásicas, durables; mensajes persistentes (delivery_mode=2).
- En este laboratorio, a posiciones.dlq sólo llega lo que el consumidor
  rechaza sin reencolar: posiciones no tiene tiempo de vida ni largo máximo.
  En general, RabbitMQ también manda a la DLQ por tiempo de vida vencido o por
  largo máximo, si la cola lo configura; así regresa posiciones.espera.
- Todo es sintético: la forma de una posición, nunca datos reales.
"""
from __future__ import annotations

import base64
import json
import time
import urllib.request

import pika

HOST, PUERTO = "127.0.0.1", 5672
API = "http://127.0.0.1:15672/api"
COLA, DLQ, ESPERA = "posiciones", "posiciones.dlq", "posiciones.espera"
ESPERA_MS = 1000


def ahora_ms() -> int:
    return time.time_ns() // 1_000_000


def conectar() -> pika.BlockingConnection:
    return pika.BlockingConnection(pika.ConnectionParameters(HOST, PUERTO, heartbeat=30, blocked_connection_timeout=600))


def declarar(canal) -> None:
    canal.exchange_declare("tele", "direct", durable=True)
    canal.exchange_declare("tele.muertos", "direct", durable=True)
    canal.exchange_declare("tele.espera", "direct", durable=True)
    canal.queue_declare(COLA, durable=True, arguments={
        "x-dead-letter-exchange": "tele.muertos", "x-dead-letter-routing-key": DLQ})
    canal.queue_declare(DLQ, durable=True)
    canal.queue_declare(ESPERA, durable=True, arguments={
        "x-message-ttl": ESPERA_MS, "x-dead-letter-exchange": "tele", "x-dead-letter-routing-key": COLA})
    canal.queue_bind(COLA, "tele", COLA)
    canal.queue_bind(DLQ, "tele.muertos", DLQ)
    canal.queue_bind(ESPERA, "tele.espera", ESPERA)


def vaciar() -> None:
    con = conectar()
    c = con.channel()
    declarar(c)
    for q in (COLA, DLQ, ESPERA):
        c.queue_purge(q)
    con.close()


def mensaje(seq: int, t_pub: int, tipo: str = "ok") -> bytes:
    """Una posición sintética de ~150 bytes. tipo: ok, invalido, transitoria
    (falla dos veces y a la tercera se guarda) o transitoriaN (falla N veces)."""
    if tipo == "invalido":
        return b'{"seq": ' + str(seq).encode() + b', "lat": "no es un numero"'   # JSON roto
    d = {"seq": seq, "equipo": f"EQ{seq % 500:04d}", "t_evento": t_pub - 2000, "t_pub": t_pub,
         "lat": 19.4326 + (seq % 1000) * 1e-5, "lon": -99.1332 - (seq % 1000) * 1e-5,
         "vel": seq % 90, "rumbo": seq % 360}
    if tipo.startswith("transitoria"):
        d["fallar"] = int(tipo[len("transitoria"):] or 2)
    return json.dumps(d, separators=(",", ":")).encode()


def api(ruta: str) -> dict:
    req = urllib.request.Request(API + ruta)
    # Usuario de fábrica del broker de laboratorio: sólo acepta conexiones de 127.0.0.1.
    req.add_header("Authorization", "Basic " + base64.b64encode(b"guest:guest").decode())
    with urllib.request.urlopen(req, timeout=2) as r:
        return json.load(r)


def listos(cola: str = COLA) -> int:
    """Mensajes listos según el broker, al momento (sin el retraso de la API)."""
    con = conectar()
    try:
        return con.channel().queue_declare(cola, passive=True).method.message_count
    finally:
        con.close()
