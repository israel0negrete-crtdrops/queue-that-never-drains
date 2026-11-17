#!/usr/bin/env python3
"""Publica posiciones sintéticas a una tasa fija.

    python3 productor.py --tasa 800 --duracion 120 --salida pub.csv
    python3 productor.py --tasa 0 --cuantos 20000          # tan rápido como pueda: precarga

La tasa se sostiene contra el reloj (el mensaje i sale en t0 + i/tasa), así
que si el broker frena al productor, el atraso queda en pub.csv y no se
esconde. --especiales "120:invalido,300:transitoria" marca mensajes por número.
"""
from __future__ import annotations

import argparse
import csv
import time

import pika

from comun import COLA, ahora_ms, conectar, declarar, mensaje


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--tasa", type=float, default=0, help="mensajes por segundo; 0 = sin límite")
    ap.add_argument("--duracion", type=float, default=0)
    ap.add_argument("--cuantos", type=int, default=0)
    ap.add_argument("--inicio", type=int, default=0, help="primer número de secuencia")
    ap.add_argument("--especiales", default="")
    ap.add_argument("--salida")
    a = ap.parse_args()
    especiales = {int(k): v for k, v in (x.split(":") for x in a.especiales.split(",") if x)}

    con = conectar()
    canal = con.channel()
    declarar(canal)

    t0 = time.monotonic()
    por_segundo: dict[int, int] = {}
    i = 0
    while True:
        if a.cuantos and i >= a.cuantos:
            break
        transcurrido = time.monotonic() - t0
        if a.duracion and transcurrido >= a.duracion:
            break
        if a.tasa:
            adelanto = t0 + i / a.tasa - time.monotonic()
            if adelanto > 0.002:
                con.sleep(adelanto)
        seq = a.inicio + i
        t = ahora_ms()
        # timestamp (segundos) deja que el broker reporte la edad del mensaje más viejo.
        props = pika.BasicProperties(delivery_mode=2, content_type="application/json", timestamp=t // 1000)
        canal.basic_publish("tele", COLA, mensaje(seq, t, especiales.get(seq, "ok")), props)
        s = int(time.monotonic() - t0)
        por_segundo[s] = por_segundo.get(s, 0) + 1
        i += 1
    con.close()

    if a.salida:
        with open(a.salida, "w", newline="") as f:
            w = csv.writer(f)
            w.writerow(["segundo", "publicados"])
            for s in sorted(por_segundo):
                w.writerow([s, por_segundo[s]])
    dur = time.monotonic() - t0
    print(f"publicados {i} en {dur:.1f} s ({i / dur:.0f}/s)")


if __name__ == "__main__":
    main()
