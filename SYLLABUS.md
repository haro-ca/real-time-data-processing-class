# Procesamiento de Datos en Tiempo Real

Este curso empieza con una sola base de datos y termina con un pipeline completo de streaming en producción. Cada clase agrega una pieza, y cada pieza existe porque la anterior no fue suficiente. Al final, tendrás un sistema corriendo de punta a punta, y sabrás exactamente por qué cada componente está ahí.

```
Postgres ─→ CDC ─→ Kafka ─→ Spark/Flink ─→ ClickHouse ─→ FastAPI
   (OLTP)              (streaming)            (OLAP)        (API)
                                                  │
                                               Grafana
                                             (monitoreo)
```

Formato: 12 clases de 3 horas. ~1 hora de teoría, ~2 horas construyendo, midiendo y rompiendo cosas.

---

## Temario

### Bloque 1: Los límites de una Base de Datos Transaccional (Semanas 1-2)

> Antes de distribuir, escalar o complicar, hay que saber exactamente cuánto aguanta un solo nodo y que se paga por agregar más.

- Internals de PostgreSQL: WAL, MVCC, buffer pool, vacuum. Los cinco puntos donde un nodo se satura.
- Benchmarking real: construir tu propio generador de carga, no usar herramientas prehechas. La instrumentación es el punto.
- Distribución de OLTP: teorema CAP (formalizado), consenso (Raft), transacciones distribuidas (2PC), relojes lógicos.
- El costo concreto: medir cuánta latencia pagas por cada garantia de consistencia.

### Bloque 2: El problema del transporte de datos: de transaccional a analítico (Semanas 3-4)

> OLTP y OLAP optimizan para patrones de acceso opuestos. Eso significa que los datos tienen que moverse de uno al otro, y eso es más difícil de lo que parece.

- Row stores vs. column stores: por qué el mismo hardware puede escanear mil millones de filas en analítica pero no supera 10k TPS en transaccional.

### Bloque 3: De batch a streaming (Semanas 5-6)

> En batch preguntas "¿qué cambio?" cada cierto tiempo. Con CDC y Kafka, decides "qué cambio" en el momento que sucede. Ese cambio de paradigma es el puente al procesamiento en tiempo real.

- Change Data Capture: decodificación de WAL, replicación lógica en Postgres, patrón outbox.
- Apache Kafka desde cero: almacenamiento log-structured, particiones, consumer groups, exactly-once semantics.
- De consumidores manuales a frameworks: el trabajo manual con Kafka prepara el terreno para Spark.

### Bloque 4: Fundamentos del Stream Processing (Semanas 7-8)

> Los datos ya fluyen. Ahora hay que procesarlos, agregar, enriquecer, deduplicar, sin perder eventos y sin duplicarlos.

- Transformaciones stateless: map, filter, windowing (tumbling, sliding, session).
- Event time vs. processing time. Watermarks: el tradeoff entre completitud y latencia.
- Operaciones stateful: joins, sesionización, deduplicación. Checkpointing y recuperación.
- Exactly-once end-to-end: por qué el problema de los dos generales hace esto teóricamente imposible, y cómo se resuelven en la práctica con sinks idempotentes.

### Bloque 5: Streaming en tiempo real (Semanas 9-10)

> "Tiempo real" puede significar 2 segundos o 2 milisegundos. La diferencia entre micro-batch y true streaming determina cuál puedes cumplir, y tiene implicaciones de arquitectura, costo y complejidad.

- Spark (micro-batch) vs. Flink (true streaming): medición real, no teoría. CDFs de latencia p99 en workloads idénticos.
- PyFlink DataStream API y Flink SQL: cuando usar cada uno.
- OLAP en tiempo real: ClickHouse ingiriendo de Kafka y sirviendo queries sub-segundo simultáneamente.
- De base de datos a API: construir un endpoint analítico sobre datos que se actualizan continuamente.

### Bloque 6: Todo junto y todo roto: los problemas de la integración (Semanas 11-12)

> Cada pieza funciona en aislamiento. Conectarlas todas es donde la ingeniería real sucede, y donde todo se rompe de formas que no esperabas.

- Pipeline completo: Postgres → CDC → Kafka → Spark → ClickHouse → FastAPI → Grafana.
- Schema registries, evolución de contratos, backpressure, observabilidad.
- Chaos engineering: diagnosticar y reparar fallas reales a través de todas las capas del stack, bajo presión de tiempo.

---

## Clases

### Clase 1: Cuánto Aguanta un Solo Nodo OLTP

> Al terminar está clase, sabrás exactamente cuántas transacciones por segundo aguanta tu hardware, y por qué.

**Teoría:** Internals de PostgreSQL: WAL, MVCC, shared buffers, vacuum, locks. Cinco puntos de saturación y cómo identificar cuál se satura primero. Ley de Amdahl aplicada a workloads de bases de datos.

**Práctico:** Construir un generador de carga con `asyncpg`. Benchmarkear Postgres con recursos limitados (Docker, 2 CPUs, 4GB RAM). Escalar concurrencia de 10 a 500 conexiones y observar cómo el throughput sube, se estanca y colapsa.

**Entregable:** Análisis de cuello de botella con flame graphs (`py-spy`) y `pg_stat_statements`, explicando qué recurso se saturó y a qué TPS.

---

### Clase 2: Qué Pasa Cuando Distribuyes OLTP

> Más nodos no significa más rendimiento. Significa más garantías, a un costo que debes medir.

**Teoría:** Teorema CAP formalizado (Gilbert/Lynch). Consenso con Raft: elección de líder, replicación de log. Transacciones distribuidas (2PC) y su problema de bloqueo. Relojes: Lamport, hybrid logical clocks, TrueTime.

**Práctico:** Desplegar CockroachDB (3 nodos, Docker Compose). Correr el mismo workload de Clase 1. Resultados: 2-5x menos TPS, mayor latencia p99. Matar un nodo mid-workload, el clúster sobrevive. Matar dos, el clúster se detiene (CP en acción).

**Entregable:** Análisis comparativo Postgres vs. CockroachDB con histogramas de latencia (p50/p95/p99) y documentación del comportamiento ante fallas.

---

### Clase 3: Por Qué OLAP es un Problema Fundamentalmente Diferente

> El mismo query, los mismos datos, el mismo hardware, 67x más rápido. La diferencia no es magia, es layout de datos.

**Teoría:** Row stores vs. column stores: layout físico, reducción de I/O, compresión (dictionary, RLE, delta). Ejecución vectorizada vs. tuple-at-a-time. Zone maps y segment elimination. Late materialización. Por qué los column stores son malos para OLTP.

**Práctico:** DuckDB vs. Postgres head-to-head con datos de NYC taxi (~100M filas). Cuatro queries diseñadas para exponer diferentes ventajas: I/O, zone maps, hash aggregation, window functions. Deep dive en `EXPLAIN ANALYZE` de ambos motores.

**Entregable:** Query plans anotados explicando qué optimizaciones específicas causan la diferencia en cada query, con evidencia de bytes leídos, filas escaneadas y tiempos por operador.

---

### Clase 4: ETL Clásico y Por Qué "Solo Mueve los Datos" es Difícil

> Suena trivial: SELECT de un lado, INSERT del otro. Hasta que falla a mitad de camino y tienes que decidir si tus datos están duplicados o incompletos.

**Teoría:** ETL vs. ELT: dónde ocurre el compute, quién es dueño de la transformación. Idempotencia y por qué es el requisito más importante de cualquier pipeline. Slowly changing dimensions (tipos 1, 2, 3). Evolución de esquemas. Por qué existen los orquestadores.

**Práctico:** Construir pipeline batch en Python puro: extraer de Postgres, transformar con DuckDB, cargar en target analítico. Inyectar falla mid-pipeline y hacer que recupere sin duplicados. Luego ver la misma pipeline como DAG de Airflow y como assets de Dagster (walkthrough, no hands-on).

**Entregable:** Pipeline idempotente, correrla 3 veces para la misma fecha, probar que el resultado es idéntico cada vez.

---

### Clase 5: CDC, El Puente Entre OLTP y Todo lo Demás

> En batch preguntas "¿qué cambio desde la última vez?" Con CDC, dejas de preguntar, los cambios llegan solos.

**Teoría:** Polling vs. CDC basado en log. Decodificación de WAL en Postgres: slots de replicación lógica, plugin `pgoutput`, protocolo de streaming. Patrón outbox. Arquitectura de Debezium.

**Práctico:** Implementar consumidor CDC con `psycopg3` que lee el WAL de Postgres y mantiene una vista materializada en DuckDB sincronizada con la fuente. Insertar, actualizar y borrar filas en Postgres y verlas reflejadas. Expansión opcional (10 min): Debezium como solución de producción.

**Entregable:** CDC consumer funcional que mantiene DuckDB en sync con Postgres.

---

### Clase 6: Fundamentos de Event Streaming con Kafka

> Kafka no es una cola de mensajes. Es un commit log distribuido, y esa diferencia cambia cómo diseñas todo lo que viene después.

**Teoría:** Almacenamiento log-structured. Particiones, offsets, consumer groups. Exactly-once: productores idempotentes, productores transaccionales, `read_committed`. Mecanismo ISR: réplicas in-sync, acks, unclean leader election.

**Práctico:** Desplegar Kafka (KRaft, sin ZooKeeper). Escribir productores y consumidores con `confluent-kafka-python`. Experimentar con asignación de particiones, rebalanceo y consumer lag. Producir eventos fuera de orden. Cierre: mapeo entre el trabajo manual de está clase y cómo Spark lo abstrae en Clase 7.

**Entregable:** Consumidor que maneja rebalanceo correctamente y reporta sus propias métricas de lag.

---

### Clase 7: Stream Processing I, Transformaciones y Windowing

> Los datos ya fluyen por Kafka. Ahora hay que hacer algo útil con ellos, sin pausar el flujo.

**Teoría:** Topología de stream processing: sources, operators, sinks. Transformaciones stateless (map, filter, flatMap). Windowing: tumbling, sliding, session. Event time vs. processing time. Watermarks y el tradeoff completitud/latencia.

**Práctico:** PySpark Structured Streaming. Construir pipeline que consume de Kafka y computa revenue por ventana tumbling de 5 minutos. Inyectar eventos tardíos con diferentes configuraciones de watermark y observar cuáles se aceptan y cuáles se descartan.

**Entregable:** Pipeline con lateness configurable, demostrando qué pasa cuando eventos llegan después de que la ventana cierra.

---

### Clase 8: Stream Processing II, Estado y Exactly-Once

> Esta es la clase más difícil del curso. Hasta ahora todo era stateless, ahora el procesador tiene memoria, y esa memoria tiene que sobrevivir a fallas.

**Teoría:** Operaciones stateful: joins (stream-stream, stream-table), sesionización, deduplicación. State backends y checkpointing. Exactly-once end-to-end: el gap entre procesamiento y entrega a un sink externo. El problema de los dos generales.

**Práctico:** Join stateful en PySpark: enriquecer transacciones con datos de clientes desde un Kafka topic compactado. Escribir resultados a Postgres con upserts idempotentes. Matar el procesador con `kill -9`, reiniciar, y verificar cero duplicados y cero pérdida de datos.

**Entregable:** Pipeline fault-tolerant con prueba documentada de exactly-once: conteo antes del kill, conteo después del restárt, evidencia de que los números cuadran.

---

### Clase 9: Micro-Batch vs. True Streaming, Spark vs. Flink

> "Tiempo real" no es un término técnico, es una promesa de latencia. Esta clase te enseña a medir si la estás cumpliendo.

**Teoría:** Modelo micro-batch de Spark: trigger intervals, límites de batch, latencia mínima teórica. Modelo true streaming de Flink: procesamiento record-a-record, emisión inmediata. PyFlink DataStream API y Flink SQL: cuando usar cada uno.

**Práctico:** Re-implementar la pipeline de Clase 7 en PyFlink (DataStream API). Benchmarkear ambos motores en workloads idénticos. Producir CDFs de latencia p99 que muestren la diferencia. Demostración breve de Flink SQL (10-15 min).

**Entregable:** Reporte benchmark con CDFs de latencia para ambos motores y recomendación arquitectónica para tres escenarios: dashboard que actualiza cada 30s, alertas en 5s, detección de fraude en 500ms.

---

### Clase 10: OLAP en Tiempo Real, Sirviendo Resultados

> Los datos están procesados. Ahora alguien necesita consultarlos, en sub-segundo, mientras siguen llegando datos nuevos.

**Teoría:** Pre-agregación vs. on-the-fly. Materialized views en ClickHouse. Motores real-time OLAP (ClickHouse, Pinot, Druid) vs. batch OLAP (DuckDB, Snowflake). LSM trees y MergeTree. Latencia de ingestión vs. latencia de query.

**Práctico:** Desplegar ClickHouse (Docker). Configurar ingestión desde Kafka con Kafka engine tables. Implementar queries analíticas en un skeleton de FastAPI proporcionado. Comparar contra DuckDB con datos batch-loaded.

**Entregable:** Endpoint HTTP en tiempo real sirviendo agregaciones sub-segundo, con latencia medida bajo ingestión concurrente.

---

### Clase 11: Pipeline End-to-End

> Cada componente funciona en aislamiento. Conectarlos todos es un problema de ingeniería diferente, y es donde la observabilidad se vuelve obligatoria.

**Teoría:** Exactly-once a través de fronteras de sistemas. Schema Registry (Confluent) con Avro: compatibilidad forward, backward, full. Backpressure: cómo se propaga y cómo detectarla. Monitoreo: consumer lag, latencia de procesamiento, duración de checkpoints.

**Práctico:** Conectar todo de Clases 1-10 en un solo pipeline (Docker Compose proporcionado). Introducir un cambio de esquema en Postgres y propagarlo a través de CDC → Kafka → Spark → ClickHouse → API sin downtime. Configurar Grafana para monitorear cada etapa.

**Entregable:** Pipeline end-to-end corriendo con dashboard de Grafana mostrando lag, throughput y latencia en cada etapa.

---

### Clase 12: Capstone, Rompe Todo, Arregla Todo

> En producción, las cosas se rompen de formas que no ensayaste. Esta clase simula eso.

**Teoría:** Taxonomía de fallas: particiones de red, poison pills, hotspots de partición, corrupción de esquema, disco lleno. Chaos engineering: metodología, no destrucción aleatoria. Degradación controlada. Capacity planning.

**Práctico:** CTF. Pipeline pre-construida (implementación de referencia) con 6 escenarios de falla inyectados a través de todas las capas del stack. Diagnosticar y reparar bajo presión de tiempo. Las fallas son independientes, se atacan en cualquier orden.

**Entregable:** Post-mortem por cada falla (causa raíz, método de detección, fix, estrategia de prevención) y los code fixes correspondientes.

---

## Entregables

Cada clase tiene como entregable un repositorio de GitHub con:

- Código funcional
- `README.md` explicando el enfoque y hallazgos
- `AGENTS.md` o `CLAUDE.md` documentando contexto del proyecto para asistentes de IA

Entrega vía pull request (preferido) o zip al drive del curso.

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
| Orquestación (mostrado, no usado) | Airflow, Dagster | Clase 4 |

Herramientas gratuitas. Recomendado: Python 3.13+, Docker, un agente de IA ([opencode](https://github.com/nicepkg/opencode) sin tarjeta de crédito, o Claude Code, GitHub Copilot, Windsurf Cascade, Codex).

## Prerrequisitos

- Python 3.13+
- Docker y Docker Compose
- Familiaridad con SQL y async básico en Python (`asyncio`)
