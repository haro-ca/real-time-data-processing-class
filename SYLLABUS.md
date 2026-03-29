# Procesamiento de Datos en Tiempo Real

Este curso empieza con una sola base de datos y termina con un pipeline completo de streaming en produccion. Cada clase agrega una pieza, y cada pieza existe porque la anterior no fue suficiente. Al final, tendras un sistema corriendo de punta a punta, y sabras exactamente por que cada componente esta ahi.

```
Postgres ─→ CDC ─→ Kafka ─→ Spark/Flink ─→ ClickHouse ─→ FastAPI
   (OLTP)              (streaming)            (OLAP)        (API)
                                                  │
                                               Grafana
                                             (monitoreo)
```

Formato: 12 clases de 3 horas. ~1 hora de teoria, ~2 horas construyendo, midiendo y rompiendo cosas.

---

## Temario

### Bloque 1: Los Limites de una Base de Datos Transaccional (Semanas 1-2)

> Antes de distribuir, escalar o complicar, hay que saber exactamente cuanto aguanta un solo nodo y que se paga por agregar mas.

- Internals de PostgreSQL: WAL, MVCC, buffer pool, vacuum. Los cinco puntos donde un nodo se satura.
- Benchmarking real: construir tu propio generador de carga, no usar herramientas prehechas. La instrumentacion es el punto.
- Distribucion de OLTP: teorema CAP (formalizado), consenso (Raft), transacciones distribuidas (2PC), relojes logicos.
- El costo concreto: medir cuanta latencia pagas por cada garantia de consistencia.

### Bloque 2: Datos Analiticos y el Problema de Moverlos (Semanas 3-4)

> OLTP y OLAP optimizan para patrones de acceso opuestos. Eso significa que los datos tienen que moverse de uno al otro, y eso es mas dificil de lo que parece.

- Row stores vs. column stores: por que el mismo hardware puede escanear mil millones de filas en analitica pero no supera 10k TPS en transaccional.
- Ejecucion vectorizada, zone maps, compresion columnar. Las optimizaciones especificas que generan diferencias de 50-100x.
- Batch ETL en la practica: idempotencia, slowly changing dimensions, evolucion de esquemas, manejo de fallas.
- Orquestadores (Airflow, Dagster): por que existen, que resuelven, en que se diferencian.

### Bloque 3: De Batch a Streaming (Semanas 5-6)

> En batch preguntas "¿que cambio?" cada cierto tiempo. Con CDC y Kafka, recibes "que cambio" en el momento que sucede. Ese cambio de paradigma es el puente al procesamiento en tiempo real.

- Change Data Capture: decodificacion de WAL, replicacion logica en Postgres, patron outbox.
- Apache Kafka desde cero: almacenamiento log-structured, particiones, consumer groups, exactly-once semantics.
- De consumidores manuales a frameworks: el trabajo manual con Kafka prepara el terreno para Spark.

### Bloque 4: Stream Processing (Semanas 7-8)

> Los datos ya fluyen. Ahora hay que procesarlos, agregar, enriquecer, deduplicar, sin perder eventos y sin duplicarlos.

- Transformaciones stateless: map, filter, windowing (tumbling, sliding, session).
- Event time vs. processing time. Watermarks: el tradeoff entre completitud y latencia.
- Operaciones stateful: joins, sesionizacion, deduplicacion. Checkpointing y recuperacion.
- Exactly-once end-to-end: por que el problema de los dos generales hace esto teoricamente imposible, y como se resuelve en la practica con sinks idempotentes.

### Bloque 5: Streaming Real y OLAP en Tiempo Real (Semanas 9-10)

> "Tiempo real" puede significar 2 segundos o 2 milisegundos. La diferencia entre micro-batch y true streaming determina cual puedes cumplir, y tiene implicaciones de arquitectura, costo y complejidad.

- Spark (micro-batch) vs. Flink (true streaming): medicion real, no teoria. CDFs de latencia p99 en workloads identicos.
- PyFlink DataStream API y Flink SQL: cuando usar cada uno.
- OLAP en tiempo real: ClickHouse ingiriendo de Kafka y sirviendo queries sub-segundo simultaneamente.
- De base de datos a API: construir un endpoint analitico sobre datos que se actualizan continuamente.

### Bloque 6: Todo Junto y Todo Roto (Semanas 11-12)

> Cada pieza funciona en aislamiento. Conectarlas todas es donde la ingenieria real sucede, y donde todo se rompe de formas que no esperabas.

- Pipeline completo: Postgres → CDC → Kafka → Spark → ClickHouse → FastAPI → Grafana.
- Schema registries, evolucion de contratos, backpressure, observabilidad.
- Chaos engineering: diagnosticar y reparar fallas reales a traves de todas las capas del stack, bajo presion de tiempo.

---

## Clases

### Clase 1: Cuanto Aguanta un Solo Nodo OLTP

> Al terminar esta clase, sabras exactamente cuantas transacciones por segundo aguanta tu hardware, y por qué.

**Teoria:** Internals de PostgreSQL: WAL, MVCC, shared buffers, vacuum, locks. Cinco puntos de saturacion y como identificar cual se satura primero. Ley de Amdahl aplicada a workloads de bases de datos.

**Practico:** Construir un generador de carga con `asyncpg`. Benchmarkear Postgres con recursos limitados (Docker, 2 CPUs, 4GB RAM). Escalar concurrencia de 10 a 500 conexiones y observar como el throughput sube, se estanca y colapsa.

**Entregable:** Analisis de cuello de botella con flame graphs (`py-spy`) y `pg_stat_statements`, explicando que recurso se saturo y a que TPS.

---

### Clase 2: Que Pasa Cuando Distribuyes OLTP

> Mas nodos no significa mas rendimiento. Significa mas garantias, a un costo que debes medir.

**Teoria:** Teorema CAP formalizado (Gilbert/Lynch). Consenso con Raft: eleccion de lider, replicacion de log. Transacciones distribuidas (2PC) y su problema de bloqueo. Relojes: Lamport, hybrid logical clocks, TrueTime.

**Practico:** Desplegar CockroachDB (3 nodos, Docker Compose). Correr el mismo workload de Clase 1. Resultados: 2-5x menos TPS, mayor latencia p99. Matar un nodo mid-workload, el cluster sobrevive. Matar dos, el cluster se detiene (CP en accion).

**Entregable:** Analisis comparativo Postgres vs. CockroachDB con histogramas de latencia (p50/p95/p99) y documentacion del comportamiento ante fallas.

---

### Clase 3: Por Que OLAP es un Problema Fundamentalmente Diferente

> El mismo query, los mismos datos, el mismo hardware, 67x mas rapido. La diferencia no es magia, es layout de datos.

**Teoria:** Row stores vs. column stores: layout fisico, reduccion de I/O, compresion (dictionary, RLE, delta). Ejecucion vectorizada vs. tuple-at-a-time. Zone maps y segment elimination. Late materialization. Por que los column stores son malos para OLTP.

**Practico:** DuckDB vs. Postgres head-to-head con datos de NYC taxi (~100M filas). Cuatro queries diseñadas para exponer diferentes ventajas: I/O, zone maps, hash aggregation, window functions. Deep dive en `EXPLAIN ANALYZE` de ambos motores.

**Entregable:** Query plans anotados explicando que optimizaciones especificas causan la diferencia en cada query, con evidencia de bytes leidos, filas escaneadas y tiempos por operador.

---

### Clase 4: ETL Clasico y Por Que "Solo Mueve los Datos" es Dificil

> Suena trivial: SELECT de un lado, INSERT del otro. Hasta que falla a mitad de camino y tienes que decidir si tus datos estan duplicados o incompletos.

**Teoria:** ETL vs. ELT: donde ocurre el compute, quien es dueño de la transformacion. Idempotencia y por que es el requisito mas importante de cualquier pipeline. Slowly changing dimensions (tipos 1, 2, 3). Evolucion de esquemas. Por que existen los orquestadores.

**Practico:** Construir pipeline batch en Python puro: extraer de Postgres, transformar con DuckDB, cargar en target analitico. Inyectar falla mid-pipeline y hacer que recupere sin duplicados. Luego ver la misma pipeline como DAG de Airflow y como assets de Dagster (walkthrough, no hands-on).

**Entregable:** Pipeline idempotente, correrla 3 veces para la misma fecha, probar que el resultado es identico cada vez.

---

### Clase 5: CDC, El Puente Entre OLTP y Todo lo Demas

> En batch preguntas "que cambio desde la ultima vez?" Con CDC, dejas de preguntar, los cambios llegan solos.

**Teoria:** Polling vs. CDC basado en log. Decodificacion de WAL en Postgres: slots de replicacion logica, plugin `pgoutput`, protocolo de streaming. Patron outbox. Arquitectura de Debezium.

**Practico:** Implementar consumidor CDC con `psycopg3` que lee el WAL de Postgres y mantiene una vista materializada en DuckDB sincronizada con la fuente. Insertar, actualizar y borrar filas en Postgres y verlas reflejadas. Expansion opcional (10 min): Debezium como solucion de produccion.

**Entregable:** CDC consumer funcional que mantiene DuckDB en sync con Postgres.

---

### Clase 6: Fundamentos de Event Streaming con Kafka

> Kafka no es una cola de mensajes. Es un commit log distribuido, y esa diferencia cambia como diseñas todo lo que viene despues.

**Teoria:** Almacenamiento log-structured. Particiones, offsets, consumer groups. Exactly-once: productores idempotentes, productores transaccionales, `read_committed`. Mecanismo ISR: replicas in-sync, acks, unclean leader election.

**Practico:** Desplegar Kafka (KRaft, sin ZooKeeper). Escribir productores y consumidores con `confluent-kafka-python`. Experimentar con asignacion de particiones, rebalanceo y consumer lag. Producir eventos fuera de orden. Cierre: mapeo entre el trabajo manual de esta clase y como Spark lo abstrae en Clase 7.

**Entregable:** Consumidor que maneja rebalanceo correctamente y reporta sus propias metricas de lag.

---

### Clase 7: Stream Processing I, Transformaciones y Windowing

> Los datos ya fluyen por Kafka. Ahora hay que hacer algo util con ellos, sin pausar el flujo.

**Teoria:** Topologia de stream processing: sources, operators, sinks. Transformaciones stateless (map, filter, flatMap). Windowing: tumbling, sliding, session. Event time vs. processing time. Watermarks y el tradeoff completitud/latencia.

**Practico:** PySpark Structured Streaming. Construir pipeline que consume de Kafka y computa revenue por ventana tumbling de 5 minutos. Inyectar eventos tardios con diferentes configuraciones de watermark y observar cuales se aceptan y cuales se descartan.

**Entregable:** Pipeline con lateness configurable, demostrando que pasa cuando eventos llegan despues de que la ventana cierra.

---

### Clase 8: Stream Processing II, Estado y Exactly-Once

> Esta es la clase mas dificil del curso. Hasta ahora todo era stateless, ahora el procesador tiene memoria, y esa memoria tiene que sobrevivir a fallas.

**Teoria:** Operaciones stateful: joins (stream-stream, stream-table), sesionizacion, deduplicacion. State backends y checkpointing. Exactly-once end-to-end: el gap entre procesamiento y entrega a un sink externo. El problema de los dos generales.

**Practico:** Join stateful en PySpark: enriquecer transacciones con datos de clientes desde un Kafka topic compactado. Escribir resultados a Postgres con upserts idempotentes. Matar el procesador con `kill -9`, reiniciar, y verificar cero duplicados y cero perdida de datos.

**Entregable:** Pipeline fault-tolerant con prueba documentada de exactly-once: conteo antes del kill, conteo despues del restart, evidencia de que los numeros cuadran.

---

### Clase 9: Micro-Batch vs. True Streaming, Spark vs. Flink

> "Tiempo real" no es un termino tecnico, es una promesa de latencia. Esta clase te enseña a medir si la estas cumpliendo.

**Teoria:** Modelo micro-batch de Spark: trigger intervals, limites de batch, latencia minima teorica. Modelo true streaming de Flink: procesamiento record-a-record, emision inmediata. PyFlink DataStream API y Flink SQL: cuando usar cada uno.

**Practico:** Re-implementar la pipeline de Clase 7 en PyFlink (DataStream API). Benchmarkear ambos motores en workloads identicos. Producir CDFs de latencia p99 que muestren la diferencia. Demostracion breve de Flink SQL (10-15 min).

**Entregable:** Reporte benchmark con CDFs de latencia para ambos motores y recomendacion arquitectonica para tres escenarios: dashboard que actualiza cada 30s, alertas en 5s, deteccion de fraude en 500ms.

---

### Clase 10: OLAP en Tiempo Real, Sirviendo Resultados

> Los datos estan procesados. Ahora alguien necesita consultarlos, en sub-segundo, mientras siguen llegando datos nuevos.

**Teoria:** Pre-agregacion vs. on-the-fly. Materialized views en ClickHouse. Motores real-time OLAP (ClickHouse, Pinot, Druid) vs. batch OLAP (DuckDB, Snowflake). LSM trees y MergeTree. Latencia de ingestion vs. latencia de query.

**Practico:** Desplegar ClickHouse (Docker). Configurar ingestion desde Kafka con Kafka engine tables. Implementar queries analiticas en un skeleton de FastAPI proporcionado. Comparar contra DuckDB con datos batch-loaded.

**Entregable:** Endpoint HTTP en tiempo real sirviendo agregaciones sub-segundo, con latencia medida bajo ingestion concurrente.

---

### Clase 11: Pipeline End-to-End

> Cada componente funciona en aislamiento. Conectarlos todos es un problema de ingenieria diferente, y es donde la observabilidad se vuelve obligatoria.

**Teoria:** Exactly-once a traves de fronteras de sistemas. Schema Registry (Confluent) con Avro: compatibilidad forward, backward, full. Backpressure: como se propaga y como detectarla. Monitoreo: consumer lag, latencia de procesamiento, duracion de checkpoints.

**Practico:** Conectar todo de Clases 1-10 en un solo pipeline (Docker Compose proporcionado). Introducir un cambio de esquema en Postgres y propagarlo a traves de CDC → Kafka → Spark → ClickHouse → API sin downtime. Configurar Grafana para monitorear cada etapa.

**Entregable:** Pipeline end-to-end corriendo con dashboard de Grafana mostrando lag, throughput y latencia en cada etapa.

---

### Clase 12: Capstone, Rompe Todo, Arregla Todo

> En produccion, las cosas se rompen de formas que no ensayaste. Esta clase simula eso.

**Teoria:** Taxonomia de fallas: particiones de red, poison pills, hotspots de particion, corrupcion de esquema, disco lleno. Chaos engineering: metodologia, no destruccion aleatoria. Degradacion controlada. Capacity planning.

**Practico:** CTF. Pipeline pre-construida (implementacion de referencia) con 6 escenarios de falla inyectados a traves de todas las capas del stack. Diagnosticar y reparar bajo presion de tiempo. Las fallas son independientes, se atacan en cualquier orden.

**Entregable:** Post-mortem por cada falla (causa raiz, metodo de deteccion, fix, estrategia de prevencion) y los code fixes correspondientes.

---

## Entregables

Cada clase tiene como entregable un repositorio de GitHub con:

- Codigo funcional
- `README.md` explicando el enfoque y hallazgos
- `AGENTS.md` o `CLAUDE.md` documentando contexto del proyecto para asistentes de IA

Entrega via pull request (preferido) o zip al drive del curso.

## Herramientas

| Componente | Herramienta | Se introduce en |
|---|---|---|
| OLTP | PostgreSQL | Clase 1 |
| OLTP distribuido | CockroachDB | Clase 2 |
| OLAP batch | DuckDB | Clase 3 |
| Event streaming | Apache Kafka (KRaft) | Clase 6 |
| Stream processing | PySpark Structured Streaming | Clase 7 |
| True streaming | Apache Flink (PyFlink) | Clase 9 |
| OLAP tiempo real | ClickHouse | Clase 10 |
| Capa API | FastAPI | Clase 10 |
| Schema registry | Confluent Schema Registry | Clase 11 |
| Monitoreo | Grafana | Clase 11 |
| Orquestacion (mostrado, no usado) | Airflow, Dagster | Clase 4 |

Herramientas gratuitas. Recomendado: Python 3.13+, Docker, un agente de IA ([opencode](https://github.com/nicepkg/opencode) sin tarjeta de credito, o Claude Code, GitHub Copilot, Windsurf Cascade, Codex).

## Prerequisitos

- Python 3.13+
- Docker y Docker Compose
- Familiaridad con SQL y async basico en Python (`asyncio`)
