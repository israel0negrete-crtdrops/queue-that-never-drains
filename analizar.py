#!/usr/bin/env python3
"""Resume una corrida de escenarios.py: series por segundo y cifras de cada escenario.

    python3 analizar.py ../resultados/20261029T1457

Deja resumen.json y series/<escenario>-<caso>.csv en la carpeta de la corrida.
Todo sale de los registros del consumidor (un renglón por entrega, con la hora
de publicación y la de fin) y de las muestras del broker; nada se estima.

"Edad" de un mensaje = cuándo terminó de guardarse menos cuándo se publicó:
lo que tardó en volverse dato utilizable.
"""
from __future__ import annotations

import csv
import json
import statistics
import sys
from pathlib import Path

OK = {"ok", "duplicado"}


def leer(d: Path) -> list[dict]:
    out = []
    for f in sorted(d.glob("consumidor*.csv")):
        out += list(csv.DictReader(f.open()))
    return out


def muestras(d: Path) -> list[dict]:
    f = d / "muestras.csv"
    return [r for r in csv.DictReader(f.open()) if r["listos"] != ""] if f.exists() else []


def serie(d: Path, t0: int) -> list[dict]:
    """Por segundo desde t0: publicados, guardados, edad máx. y mediana de lo guardado, listos y sin confirmar."""
    pub = {int(r["segundo"]): int(r["publicados"]) for r in csv.DictReader((d / "pub.csv").open())}
    regs = leer(d)
    por: dict[int, list[int]] = {}
    for r in regs:
        if r["resultado"] in OK:
            s = (int(r["t_fin"]) - t0) // 1000
            por.setdefault(s, []).append(int(r["t_fin"]) - int(r["t_pub"]))
    bro = {}
    for m in muestras(d):
        bro[(int(m["t"]) - t0) // 1000] = m
    fin = max([*pub, *por, *bro, 0])
    out = []
    for s in range(0, fin + 1):
        e = por.get(s, [])
        b = bro.get(s, {})
        out.append({"s": s, "publicados": pub.get(s, 0), "guardados": len(e),
                    "edad_max_ms": max(e) if e else "", "edad_mediana_ms": int(statistics.median(e)) if e else "",
                    "listos": b.get("listos", ""), "sin_confirmar": b.get("sin_confirmar", "")})
    return out


def escribir(ruta: Path, filas: list[dict]) -> None:
    ruta.parent.mkdir(exist_ok=True)
    with ruta.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=list(filas[0]))
        w.writeheader()
        w.writerows(filas)


def pendiente(xs: list[float], ys: list[float]) -> float:
    mx, my = statistics.fmean(xs), statistics.fmean(ys)
    den = sum((x - mx) ** 2 for x in xs)
    return sum((x - mx) * (y - my) for x, y in zip(xs, ys)) / den if den else 0.0


def main() -> None:
    raiz = Path(sys.argv[1])
    res: dict = {"capacidad": json.loads((raiz / "capacidad" / "capacidad.json").read_text())}
    mu = next(x["por_segundo"] for x in res["capacidad"] if x["prefetch"] == 10 and x["consumidores"] == 1)
    res["capacidad_base"] = mu

    # 1. tasa contra capacidad
    res["tasa"] = []
    for d in sorted((raiz / "tasa").glob("x*")):
        ev = json.loads((d / "eventos.json").read_text())
        s = serie(d, ev["inicio_ms"])
        escribir(raiz / "series" / f"tasa-{d.name}.csv", s)
        # Segundos completos del productor: el último segundo con publicados es parcial.
        dur = max(x["s"] for x in s if x["publicados"] > 0)
        # Llegadas y salidas en la segunda mitad, ya sin el arranque, en la misma ventana.
        lam = statistics.fmean(x["publicados"] for x in s[dur // 2:dur])
        # La pendiente del atraso, en la segunda mitad del productor (ya sin el arranque).
        mitad = [x for x in s[dur // 2:dur] if x["listos"] != ""]
        crece = pendiente([x["s"] for x in mitad], [int(x["listos"]) + int(x["sin_confirmar"]) for x in mitad])
        guard = [x["guardados"] for x in s[dur // 2:dur]]
        edades = [x["edad_max_ms"] for x in s[:dur] if x["edad_max_ms"] != ""]
        ultimo = max((i for i, x in enumerate(s) if x["guardados"]), default=0)
        res["tasa"].append({
            "factor": float(d.name[1:]), "llegan_por_s": round(lam, 1),
            "salen_por_s": round(statistics.fmean(guard), 1),
            "atraso_crece_por_s": round(crece, 1),
            "atraso_max": max((int(x["listos"]) + int(x["sin_confirmar"]) for x in s if x["listos"] != ""), default=0),
            "edad_max_al_final_s": round(edades[-1] / 1000, 2) if edades else None,
            "edad_mediana_primer_minuto_ms": int(statistics.median(
                [x["edad_mediana_ms"] for x in s[5:60] if x["edad_mediana_ms"] != ""])),
            "vacio_s_despues_del_productor": max(0, ultimo + 1 - dur)})

    # 2. caída del consumidor
    res["caida"] = []
    for d in sorted((raiz / "caida").glob("x*")):
        ev = json.loads((d / "eventos.json").read_text())
        t0 = ev["inicio_ms"]
        s = serie(d, t0)
        escribir(raiz / "series" / f"caida-{d.name}.csv", s)
        vuelve = (ev["regreso_ms"] - t0) // 1000
        cae = (ev["caida_ms"] - t0) // 1000
        antes = [x["edad_max_ms"] for x in s[10:cae] if x["edad_max_ms"] != ""]
        umbral = max(1000, 2 * max(antes))       # "al día": la edad vuelve al nivel de antes de la caída
        rec = None
        for i in range(vuelve, len(s) - 5):
            v = [x["edad_max_ms"] for x in s[i:i + 5]]
            if all(e != "" and e <= umbral for e in v):
                rec = i - vuelve
                break
        lam = statistics.fmean(x["publicados"] for x in s[5:cae])
        pico = max((int(x["listos"]) + int(x["sin_confirmar"]) for x in s if x["listos"] != ""), default=0)
        rapido = [x["guardados"] for x in s[vuelve + 2:vuelve + 2 + max(5, (rec or 10) // 2)]]
        res["caida"].append({
            "factor": float(d.name[1:]), "llegan_por_s": round(lam, 1), "caida_s": vuelve - cae,
            "atraso_al_volver": pico, "salen_por_s_vaciando": round(statistics.fmean(rapido), 1) if rapido else None,
            "edad_max_s": round(max(x["edad_max_ms"] for x in s if x["edad_max_ms"] != "") / 1000, 1),
            "recuperacion_s": rec, "recuperacion_prevista_s": ev.get("recuperacion_prevista_s"),
            "umbral_al_dia_ms": umbral})

    # 3. mensajes que fallan
    res["veneno"] = []
    for d in sorted((raiz / "veneno").glob("*")):
        ev = json.loads((d / "eventos.json").read_text())
        r = leer(d)
        cuenta: dict[str, int] = {}
        for x in r:
            cuenta[x["resultado"]] = cuenta.get(x["resultado"], 0) + 1
        s = serie(d, ev["inicio_ms"])
        escribir(raiz / "series" / f"veneno-{d.name}.csv", s)
        inval = [int(x["t_entrega"]) for x in r if x["resultado"] == "invalido-reencolado"]
        res["veneno"].append({
            "politica": d.name, "resultados": cuenta,
            "reentregas_del_invalido_por_s": round(len(inval) / ((max(inval) - min(inval)) / 1000), 0) if len(inval) > 1 else 0,
            "listos_al_final": ev["listos_al_final"], "listos_tras_detener": ev.get("listos_tras_detener"),
            "dlq": ev["dlq_al_final"],
            "edad_mediana_ms": int(statistics.median(int(x["t_fin"]) - int(x["t_pub"]) for x in r if x["resultado"] in OK))})

    # 4. guardado sin ACK
    res["sin_ack"] = []
    for d in sorted((raiz / "sin_ack").glob("*")):
        ev = json.loads((d / "eventos.json").read_text())
        r = leer(d)
        res["sin_ack"].append({
            "tabla": d.name, "caidas": ev["reinicios"], "filas": ev["filas"], "mensajes_distintos": ev["seq_distintos"],
            "filas_de_mas": ev["filas"] - ev["seq_distintos"],
            "reentregados": sum(1 for x in r if x["reentregado"] == "1"),
            "detectados_como_duplicado": sum(1 for x in r if x["resultado"] == "duplicado")})

    (raiz / "resumen.json").write_text(json.dumps(res, indent=1, ensure_ascii=False))
    print(json.dumps(res, indent=1, ensure_ascii=False))


if __name__ == "__main__":
    main()
