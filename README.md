# TP Sistemas Distribuidos - Movies Analysis

Este repositorio contiene un sistema de análisis de películas desarrollado como trabajo práctico para la materia **Sistemas Distribuidos I (75.74)** de la Facultad de Ingeniería de la Universidad de Buenos Aires.

El sistema está compuesto por múltiples programas distribuidos que, en conjunto, procesan queries específicas sobre tres datasets de películas provistos por IMDB.

## Queries implementadas

Las consultas ejecutadas sobre el dataset son:

1. Películas y sus géneros producidas en los años 2000 en Argentina y España.
2. Top 5 de países que más dinero invirtieron en producciones **sin colaboración internacional**.
3. Película argentina estrenada a partir del año 2000 con mayor y menor promedio de rating.
4. Top 10 de actores con mayor participación en películas argentinas estrenadas desde el año 2000.
5. Promedio de la relación ingreso/presupuesto para películas con sentimiento positivo vs. negativo (según su descripción).

## Datasets utilizados

Los datasets no están incluidos en este repositorio y deben descargarse manualmente desde [Kaggle - The Movies Dataset](https://www.kaggle.com/datasets/rounakbanik/the-movies-dataset).
El sistema requiere únicamente los siguientes archivos:

* `movies_metadata.csv`
* `credits.csv`
* `ratings.csv`

Deben colocarse en un directorio `datasets/` en la raíz del proyecto, con los siguientes nombres exactos:

```
datasets/movies_metadata.csv  
datasets/credits.csv  
datasets/ratings.csv  
```

## Instrucciones de ejecución

1. **Preparar los datasets**
   Descargar y ubicar los tres archivos mencionados anteriormente en el directorio `datasets/`.

2. **Generar el archivo de configuración `docker-compose.yaml`**
   Desde la raíz del proyecto:

   ```bash
   make generate_compose
   ```

3. **Construir los contenedores**

   ```bash
   make build_docker
   ```

4. **Iniciar la ejecución distribuida**

   ```bash
   make run_docker
   ```

5. **Ejecutar el cliente**
   En otra terminal, ingresar al directorio `client/`, instalar las dependencias necesarias (se recomienda el uso de un entorno virtual), y correr el cliente:

   ```bash
   python3 main.py
   ```

   El cliente imprimirá una `UUID` que identifica los resultados de la consulta.

6. **Ver resultados**
   Los resultados estarán disponibles en el directorio:

   ```
   client/results/{uuid}/
   ```

7. **Verificar resultados**
   Desde la raíz del proyecto:

   ```bash
   make verify_results
   ```

8. **Detener la ejecución**

   ```bash
   make stop_docker
   ```

## Configuración avanzada

Para modificar parámetros de configuración (como rutas, nombres de servicios, variables de entorno, etc.), lo más recomendable es editar directamente el archivo `compose-spec.json`.
Este archivo puede generarse automáticamente usando `make generate_compose`.

Para más información sobre la configuración y generación de este archivo, consultar el archivo [`scripts/README.md`](scripts/README.md).
