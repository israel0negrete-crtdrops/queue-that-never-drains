#!/bin/bash
. "$(dirname "$0")/entorno.sh"
rabbitmqctl stop && sleep 2 && epmd -kill >/dev/null 2>&1; echo detenido
