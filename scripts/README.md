# Scripts auxiliares

En este directorio se encuentran 3 scripts auxiliares. Todos utilizan únicamente la librería estándar de python 3.12.

Todos están pensados para ser ejecutados desde el directorio raíz invocando a `python3 scripts/<script>.py`

## Generador de docker-compose

El script `generate_compose.py` se encarga de generar automáticamente un archivo docker-compose para la correcta ejecución del proyecto. Se recomienda no editar dicho archivo manualmente dado a que la configuración incorrecta de los parámetros puede causar fallas irrecuperables en el sistema. Se puede invocar como `python3 scripts/generar_compose.py -h` para imprimir detalles sobre el uso correcto del mismo.

Este script hace uso extensivo del archivo `compose-spec.json`. El mismo contiene todos los parámetros configurables para el sistema. En el nivel superior del objeto json, las únicas claves son `name` que corresponde al nombre del proyecto de docker compose, y `services` que contiene las configuraciones para todos los servicios. Las configuraciones son:

- `name`: Nombre del servicio. Se recomienda no alterar
- `output_nodes` y `*_routing_key`: Parámetros para la entrada y salida de los servicios. Se recomienda no alterar
- `replicas`: Cantidad de réplicas para cada servicio. Salvo `client-handler` y los `master-group-by-*`, el resto de los servicios soportan cualquier cantidad arbitraria de réplicas.
- `log_level`: Nivel de log del servicio (para ver con `docker compose logs`)
- `heartbeat_port`: Puerto a utilizar para monitoreo.
- `storage`: Ruta a un directorio local para montar el almacenamiento de los servicios que lo utilizan.
- `messages_per_commit`: Cantidad de mensajes a recibir antes de bajar el estado al almacenamiento en los nodos que guardan estado.
- `ping_interval`, `grace_period`, `max_retries` y `max_cncurrent_healthchecks`: Configuraciones para controlar la frecuencia del servicio de monitoreo 

## Chaos monkey

El script `chancho_loco.py` es un _chaos monkey_ que permite probar la tolerancia a fallos del sistema. Tiene varias funciones para detener contenedores del sistema. Al ejecutar el script, se muestran por consola los comandos disponibles:

- `auto X Y`: Matar cada X segundos Y contenedores
- `stop`: Detener el auto kill
- `bomba`: Matar todos los contenedores excepto uno con nombre hearth-beat
- `matar <nombre>`: Matar un contenedor especifico
- `matar_varios <prefijo>`: Matar todos los contenedores con un prefijo especifico
- `mostrar`: Mostrar contenedores corriendo

## Comparador de resultados

Por conveniencia se incluye el script `compare_results.py` que verifica que los resultados obtenidos sean correctos, frente a una corrida verificada que se encuentra en `client/known-good-results/`. Simplemente ejecutar el script llamando `python3 scripts/compare_results.py` desde el directorio raíz del repositorio y se imprimirán por pantalla las coincidencias y discrepancias para todos los clientes encontrados en `client/results/`
