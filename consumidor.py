#!/usr/bin/env python3
"""Consume posiciones, las guarda en SQLite y sólo entonces confirma.

    python3 consumidor.py --prefetch 10 --db prueba.db --registro c1.csv

La regla de Israel: el ACK va después de completar la persistencia. SQLite en
WAL con synchronous=FULL, un commit por mensaje: cuando el consumidor
confirma, la posición ya está en disco.

Qué hace con un mensaje que falla (--fallas):
  requeue     lo devuelve a la cola (nack con requeue): el ingenuo.
  dlq         lo rechaza sin reencolar; la cola lo manda a posiciones.dlq.
  reintentos  inválido -> a la DLQ de inmediato; falla transitoria -> espera
              1 s en posiciones.espera y vuelve, hasta --max-intentos; agotado
              -> a la DLQ.

--tabla ingenua guarda cada entrega como fila nueva; idempotente usa la clave
(equipo, t_evento) y una reentrega no duplica. --caer-tras-guardar "500,900"
termina el proceso justo después del commit y antes del ACK, la primera vez
que ve esos mensajes: la falla del escenario 4.

--lote 10 confirma por lote: guarda hasta 10 posiciones en una sola
transacción (o lo que haya juntado en 50 ms), hace un commit y sólo entonces
confirma todas con un ACK múltiple. La regla se respeta: nada se confirma
antes de estar en disco. Con lote, el prefetch debe ser mayor que el lote.
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import signal
import sqlite3

import pika

from comun import COLA, ESPERA, ahora_ms, conectar, declarar


class Transitoria(Exception):
    pass


class Invalido(Exception):
    pass


def abrir_db(ruta: str) -> sqlite3.Connection:
    db = sqlite3.connect(ruta, timeout=30, isolation_level=None)
    db.execute("PRAGMA journal_mode=WAL")
    db.execute("PRAGMA synchronous=FULL")
    db.execute("""CREATE TABLE IF NOT EXISTS posiciones (
        equipo TEXT NOT NULL, t_evento INTEGER NOT NULL, seq INTEGER, lat REAL, lon REAL, vel INTEGER,
        rumbo INTEGER, PRIMARY KEY (equipo, t_evento))""")
    db.execute("""CREATE TABLE IF NOT EXISTS posiciones_ingenua (
        id INTEGER PRIMARY KEY AUTOINCREMENT, equipo TEXT, t_evento INTEGER, seq INTEGER, lat REAL,
        lon REAL, vel INTEGER, rumbo INTEGER)""")
    return db


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--prefetch", type=int, default=10)
    ap.add_argument("--db", required=True)
    ap.add_argument("--tabla", choices=["idempotente", "ingenua"], default="idempotente")
    ap.add_argument("--fallas", choices=["requeue", "dlq", "reintentos"], default="reintentos")
    ap.add_argument("--max-intentos", type=int, default=3)
    ap.add_argument("--caer-tras-guardar", default="")
    ap.add_argument("--registro", required=True)
    ap.add_argument("--lote", type=int, default=1)
    a = ap.parse_args()
    if a.lote > 1 and a.prefetch <= a.lote:
        ap.error("con --lote, el prefetch debe ser mayor que el lote")
    caer = {int(x) for x in a.caer_tras_guardar.split(",") if x}

    db = abrir_db(a.db)
    if a.tabla == "idempotente":
        sql = "INSERT INTO posiciones VALUES (?,?,?,?,?,?,?) ON CONFLICT DO NOTHING"
    else:
        sql = "INSERT INTO posiciones_ingenua (equipo,t_evento,seq,lat,lon,vel,rumbo) VALUES (?,?,?,?,?,?,?)"
    vistos: dict[int, int] = {}      # intentos por mensaje en este proceso (para simular la falla transitoria)

    f = open(a.registro, "a", newline="")
    reg = csv.writer(f)
    if f.tell() == 0:
        reg.writerow(["pid", "t_entrega", "t_fin", "seq", "reentregado", "intentos", "t_pub", "resultado"])

    def anotar(t_ent, seq, red, intentos, t_pub, res):
        reg.writerow([os.getpid(), t_ent, ahora_ms(), seq, int(red), intentos, t_pub, res])

    con = conectar()
    canal = con.channel()
    declarar(canal)
    canal.basic_qos(prefetch_count=a.prefetch)

    pendientes: list[tuple] = []     # (delivery_tag, renglón del registro) dentro de la transacción abierta

    def cerrar_lote():
        if not pendientes:
            return
        db.execute("COMMIT")
        canal.basic_ack(pendientes[-1][0], multiple=True)
        for _, r in pendientes:
            anotar(*r)
        pendientes.clear()

    def procesar(ch, met, props, cuerpo):
        t_ent = ahora_ms()
        if a.lote > 1:
            try:
                d = json.loads(cuerpo)
                fila = (d["equipo"], int(d["t_evento"]), int(d["seq"]), float(d["lat"]), float(d["lon"]),
                        int(d["vel"]), int(d["rumbo"]))
            except (ValueError, KeyError, TypeError):
                cerrar_lote()
                ch.basic_reject(met.delivery_tag, requeue=False)
                anotar(t_ent, -1, met.redelivered, 0, -1, "dlq-invalido")
                return
            if not pendientes:
                db.execute("BEGIN")
                con.call_later(0.05, cerrar_lote)
            cur = db.execute(sql, fila)
            pendientes.append((met.delivery_tag, (t_ent, fila[2], met.redelivered, 0, int(d["t_pub"]),
                                                  "ok" if cur.rowcount == 1 else "duplicado")))
            if len(pendientes) >= a.lote:
                cerrar_lote()
            return
        intentos = int((props.headers or {}).get("x-intentos", 0))
        seq, t_pub = -1, -1
        try:
            try:
                d = json.loads(cuerpo)
                seq, t_pub = int(d["seq"]), int(d["t_pub"])
                fila = (d["equipo"], int(d["t_evento"]), seq, float(d["lat"]), float(d["lon"]),
                        int(d["vel"]), int(d["rumbo"]))
            except (ValueError, KeyError, TypeError) as e:
                raise Invalido(str(e))
            if d.get("fallar"):
                vistos[seq] = vistos.get(seq, 0) + 1
                previos = max(intentos, vistos[seq] - 1)
                if previos < d["fallar"]:
                    raise Transitoria("la base no respondió (simulado)")
            cur = db.execute(sql, fila)
            res = "ok" if cur.rowcount == 1 else "duplicado"
            if seq in caer and not met.redelivered:
                anotar(t_ent, seq, met.redelivered, intentos, t_pub, "guardado-sin-ack")
                f.flush()
                os._exit(3)          # se guardó y el proceso muere antes del ACK
            ch.basic_ack(met.delivery_tag)
            anotar(t_ent, seq, met.redelivered, intentos, t_pub, res)
        except Invalido:
            if a.fallas == "requeue":
                ch.basic_nack(met.delivery_tag, requeue=True)
                anotar(t_ent, seq, met.redelivered, intentos, t_pub, "invalido-reencolado")
            else:
                ch.basic_reject(met.delivery_tag, requeue=False)
                anotar(t_ent, seq, met.redelivered, intentos, t_pub, "dlq-invalido")
        except Transitoria:
            if a.fallas == "requeue":
                ch.basic_nack(met.delivery_tag, requeue=True)
                anotar(t_ent, seq, met.redelivered, intentos, t_pub, "transitoria-reencolada")
            elif a.fallas == "dlq" or intentos + 1 >= a.max_intentos:
                ch.basic_reject(met.delivery_tag, requeue=False)
                anotar(t_ent, seq, met.redelivered, intentos, t_pub,
                       "dlq-transitoria" if a.fallas == "dlq" else "dlq-agotado")
            else:
                h = dict(props.headers or {})
                h["x-intentos"] = intentos + 1
                ch.basic_publish("tele.espera", ESPERA, cuerpo,
                                 pika.BasicProperties(delivery_mode=2, headers=h, content_type="application/json"))
                ch.basic_ack(met.delivery_tag)
                anotar(t_ent, seq, met.redelivered, intentos, t_pub, "a-espera")

    def terminar(*_):
        # Desde la señal no se toca el canal: se pide al ciclo de pika que se detenga
        # entre mensajes, para no cortar un ACK a la mitad.
        con.add_callback_threadsafe(canal.stop_consuming)

    signal.signal(signal.SIGTERM, terminar)
    signal.signal(signal.SIGINT, terminar)
    canal.basic_consume(COLA, procesar)
    canal.start_consuming()
    if pendientes:
        cerrar_lote()
    f.flush()
    try:
        con.close()
    except Exception:
        pass


if __name__ == "__main__":
    main()
