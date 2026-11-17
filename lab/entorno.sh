# Entorno del RabbitMQ de laboratorio: todo dentro de /home/edgelab/labs/06-cola, sólo 127.0.0.1.
export L=/home/edgelab/labs/06-cola
export PATH=/home/edgelab/labs/06-cola/root/usr/lib/erlang/bin:/home/edgelab/labs/06-cola/root/usr/lib/rabbitmq/bin:$PATH
export HOME=/home/edgelab/labs/06-cola/estado/home
export ERL_EPMD_ADDRESS=127.0.0.1
export RABBITMQ_NODENAME=cola@localhost
export RABBITMQ_NODE_IP_ADDRESS=127.0.0.1
export RABBITMQ_DIST_PORT=25672
export RABBITMQ_CONFIG_FILE=/home/edgelab/labs/06-cola/etc/rabbitmq.conf
export RABBITMQ_CONF_ENV_FILE=/home/edgelab/labs/06-cola/etc/rabbitmq-env.conf
export RABBITMQ_ENABLED_PLUGINS_FILE=/home/edgelab/labs/06-cola/etc/enabled_plugins
export RABBITMQ_MNESIA_BASE=/home/edgelab/labs/06-cola/estado/mnesia
export RABBITMQ_LOG_BASE=/home/edgelab/labs/06-cola/estado/log
export RABBITMQ_ADVANCED_CONFIG_FILE=/home/edgelab/labs/06-cola/etc/advanced.config
export PYTHONPATH=/home/edgelab/labs/06-cola/root/usr/lib/python3/dist-packages
