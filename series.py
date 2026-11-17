#!/usr/bin/env python3
"""Series por segundo de cada caso con productor, para publicar sin los registros crudos.

    python3 series.py ../resultados/20261029T1457

Para cada carpeta con pub.csv y eventos.json deja <caso>/serie.csv: publicados,
guardados, edad máxima y mediana de lo guardado, listos y sin confirmar.
"""
import json
import sys
from pathlib import Path

import analizar as a

for d in sorted(Path(sys.argv[1]).rglob("pub.csv")):
    d = d.parent
    ev = json.loads((d / "eventos.json").read_text())
    a.escribir(d / "serie.csv", a.serie(d, ev["inicio_ms"]))
    print(d)
