#!/bin/bash
# Arranca el broker de laboratorio en segundo plano. Se niega de 21:30 a 7:00.
h=$(date +%H%M); if [ "$h" -ge 2130 ] || [ "$h" -lt 700 ]; then echo "fuera de horario (7:00-21:30)"; exit 1; fi
. "$(dirname "$0")/entorno.sh"
rabbitmq-server -detached
for i in $(seq 30); do rabbitmqctl ping >/dev/null 2>&1 && break; sleep 1; done
rabbitmqctl await_startup --timeout 60 >/dev/null && echo arriba
