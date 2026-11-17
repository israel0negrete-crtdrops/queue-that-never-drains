#!/usr/bin/env python3
"""Tercera corrida: dónde se quiebra la capacidad sostenida, y si el lote aguanta.

    python3 tercera.py --raiz ../resultados/20261029T1457/tercera

La segunda corrida mostró que a 293/s (uno por commit) la cola no crece en
10 min, aunque vaciando a toda velocidad el disco se frena a ~266/s. En la
primera, a 767/s y 986/s sí se desplomó. Fijado antes de correr:

1. quiebre  productor a 400, 550 y 700 por segundo, 10 min cada uno, un
            consumidor con un commit por mensaje (prefetch 10).
2. lote     productor a 3,000 por segundo, 10 min, un consumidor que confirma
            por lote (10 por commit, prefetch 20).
"""
from __future__ import annotations

import argparse
import json
from pathlib import Path

import escenarios as e


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--raiz", required=True)
    a = ap.parse_args()
    raiz = Path(a.raiz)
    for tasa in (400, 550, 700):
        e.hora_permitida()
        d = e.preparar(raiz / "quiebre" / f"t{tasa}")
        ev = e.correr_con_productor(d, tasa, 600, {}, cola_extra=10)
        ev["tasa"] = tasa
        (d / "eventos.json").write_text(json.dumps(ev, indent=1))
        print("quiebre", tasa, ev, flush=True)
    e.hora_permitida()
    d = e.preparar(raiz / "lote" / "t3000")
    ev = e.correr_con_productor(d, 3000, 600, {"lote": 10}, cola_extra=10, prefetch=20)
    ev["tasa"] = 3000
    (d / "eventos.json").write_text(json.dumps(ev, indent=1))
    print("lote", ev, flush=True)
    e.vaciar()
    print("TERMINADO", flush=True)


if __name__ == "__main__":
    main()
