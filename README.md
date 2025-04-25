# distribuidos-tp1

## Cómo correr el proyecto

1. Para ejecutar el proyecto se deberá ubicar en la raíz del mismo y ejecutar el siguiente comando:
```bash
make generate_compose
```

Este comando generará un archivo `docker-compose.yml` con la configuración necesaria para levantar el proyecto. Si bien está configurado para levantar varias réplicas para aquellos nodos escalables, puede configurarse dentro de `./scripts/compose-spec.json` la cantidad de réplicas deseadas para cada nodo.

2. Una vez generado el archivo `docker-compose.yml`, se deberá ejecutar el siguiente comando:
```bash
make run_docker
```

Este comando levantará el proyecto en contenedores Docker y mostrará los logs de cada uno de ellos. Para detener el proyecto, se deberá ejecutar el siguiente comando:
```bash
make stop_docker
```
