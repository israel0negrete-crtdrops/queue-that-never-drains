# Corridas del 29 de octubre de 2026

Tres corridas, en orden. Cada una se diseñó después de ver la anterior, y lo
que cambió está escrito en su script antes de correrla.

## 1. Primera corrida (14:57–15:58)

- **Capacidad en 10,000 mensajes (~9 s):** prefetch 10 y un consumidor,
  1,095/s. Con 2 y 4 consumidores, 1,001 y 925/s: la base escribe de uno en
  uno. Prefetch 1: 723/s.
- **Tasa, 2 min cada una (tasas del segundo minuto):** a 0.95 de esa
  capacidad, entran y salen 1,041/s y no crece; a 1.05, entran 1,150/s, salen
  1,122/s y crece 28/s; a 1.20, entran 1,314/s, salen 1,103/s y crece 211/s.
- **Caída de 60 s:** a 0.5, 57 s para volver a estar al día (prevista 60); a
  0.7, 134 s (prevista 140). A 0.9 nunca se recuperó: 470,000 mensajes y 8
  min de edad al final. Ver la segunda corrida.
- **Veneno:** con requeue, el inválido se reentregó miles de veces por
  segundo y el broker marcaba 0 listos. Con DLQ directa, el de falla
  transitoria se perdió en la DLQ. Con reintentos: inválido a la DLQ al
  primer intento, transitoria guardada al tercero, la agotada a la DLQ.
- **Guardado sin ACK (5 caídas):** tabla ingenua, 5 filas de más; clave
  (equipo, t_evento), 0.

## Diagnóstico (16:03)

Vaciando 400,000 sin productor: ~1,150/s los primeros 3 min, luego ~300/s.
Su CSV no se incluye; la prueba de 15 min de la segunda corrida
(`segunda/sostenida/fila.csv`) registra las mismas columnas y muestra lo mismo:
temperatura de unos 46 °C de media en las dos fases, frecuencia media de 1,727
a 1,441 MHz (menos trabajo), y el NVMe de ~3,200 escrituras/s a 0.5 ms de
mediana a ~835/s a 1.75 ms, con picos de 11.5 ms. La causa dentro del disco
(caché, recolección de basura) **no está comprobada**.

## 2. Segunda corrida (16:19–17:20)

- **Sostenida, 15 min:** 1,159, 1,169, 1,130 y después 261–275/s. Media de
  los últimos 5 min: 266/s.
- **Descanso:** tras 30, 120 y 300 s de pausa, 1,043, 1,153 y 1,151/s en
  los primeros 20 s.
- **Tasa, 10 min:** a 213, 253 y 293/s, sin atraso.
- **Lote de 10 (prefetch 20):** vació 1,000,000 en 129 s, ~7,750/s (corrida corta).
- Un consumidor terminó con un error de pika al detenerlo a media
  confirmación (lo no confirmado volvió a la cola); corregido para la tercera.

## 3. Tercera corrida (18:11–18:53)

Sin descanso entre casos: cada uno hereda el estado del disco del anterior.

- **400/s:** al día 8 min; en el min 9 empezó a atrasarse, en el 10 salían ~286/s y la edad
  era de 22 s al apagar el productor (31 s ya vaciando, después).
- **550/s:** ~300/s desde el min 2; 143,000 en cola y 263 s de edad al final.
- **700/s:** 680/s el primer minuto, luego ~295/s; 217,000 y 312 s.
- **3,000/s con lote de 10:** 2,995/s el primer minuto, luego bajando de
  2,512 a ~1,950/s; 448,000 en cola y 150 s de edad al final.

## Lo que se puede decir y lo que no

- La capacidad de una prueba corta (1,100/s) no es la sostenida (~270–300/s
  en este equipo). Cuánto dura la fase rápida depende de la tasa y de cuánto
  descansó el disco antes.
- 293/s aguantó 10 min; no se probó más tiempo. No afirmar que aguanta
  indefinidamente.
- Confirmar por lote subió la capacidad sostenida unas 7 veces (~2,000/s
  contra ~290/s), pero también se degradó con el tiempo.
- Las cifras son de este equipo: un disco sin DRAM de 128 GB con broker y
  base en el mismo disco. Otro servidor tendrá otros números; lo que se
  repite es que hay que medir la capacidad sostenida, no la de un minuto.
