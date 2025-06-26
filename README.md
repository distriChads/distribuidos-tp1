# TP Sistemas Distribuidos - Movies Analysis

Este repositorio consiste en un sistema de análisis de películas realizado en el contexto de la materia Sistemas Distribuidos I (75.74) de la Facultad de Ingeniería de la Universidad de Buenos Aires.

Consta de un sistema distribuído de múltiples programas que en conjunto procesan determinadas queries sobre 3 datasets de películas de IMDB.

Las queries ejecutadas sobre el dataset son:
1. Películas y sus géneros de los años 00' con producción Argentina y Española.
2. Top 5 de países que más dinero han invertido en producciones sin colaborar con otros países.
3. Película de producción Argentina estrenada a partir del 2000, con mayor y con menor promedio de rating.
4. Top 10 de actores con mayor participación en películas de producción Argentina con fecha de estreno posterior al 2000. 
5. Average de la tasa ingreso/presupuesto de películas con overview de sentimiento positivo vs. sentimiento negativo.

Los datasets utilizados para el desarrollo no vienen incluídos en el repositorio. Pueden ser obtenidos en la publicación original de [Kaggle](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset). El sistema sólo usa los datasets `movies_metadata`, `credits` y `ratings`.

## Dependencias de desarrollo

1. Python v3.12 o superior
2. Go v1.24 o superior
3. Docker compose v2.32 o superior

## Cómo correr el proyecto

Para ejecutar el proyecto utilizando la configuración por defecto:

1. Descargar y descomprimir los 3 datasets y ubicarlos en el directiorio `datasets` bajo los nombres `datasets/movies_metadata.csv`, `datasets/credits.csv` y `datasets.ratings.csv` respectivamente.

2. Crear un archivo docker-compose ubicándose en la raíz del proyecto y ejecutando
```bash
make docker-compose
```

3. Construir todos los contenedores ejecutando desde la raíz del proyecto
```bash
docker compose build
```

4. Ejecutar todos los contenedores
```bash
docker compose up -d
```

5. Desde otra terminal, pararse en el directorio `client`. Asegurarse de instalar todas las dependencias del cliente (se recomienda utilizar un entorno virtual)

6. Ejecutar el cliente con el comando
```bash
python3 main.py
```

7. Una vez terminada la ejecución de la query, los resultados estarán disponibles en el directorio `client/results/{uuid}/`, el cliente muestra la uuid correspondiente a su consulta por pantalla durante la ejecución.

8. Comparar resultados ejecutando desde el directorio raíz
```bash
python3 scripts/compare_results.py
```

9. Detener la ejecución de los contenedores ejecutando desde el directorio raíz
```bash
docker compose stop
```

## Editar la configuración

Lo más recomendable para alterar las variables de configuración es editar el entorno de los contenedores desde el `docker-compose.yaml`. Ver [`scripts/README.md`](scripts/README.md) para más información sobre el uso del script de generación del docker-compose.
