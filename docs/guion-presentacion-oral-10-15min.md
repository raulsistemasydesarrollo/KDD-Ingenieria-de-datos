# Guia de Exposicion Oral (10-15 min)

## Portada

- Proyecto: `Proyecto Big Data KDD - Logistica`
- Documento: `Guion de presentacion oral ante profesor`
- Version: `v1.0`
- Fecha: `02/04/2026`

## 1. Objetivo de la exposicion

Demostrar en 10-15 minutos que el proyecto:

1. Cumple el ciclo KDD extremo a extremo.
2. Es tecnicamente solido en arquitectura y operacion.
3. Aporta valor practico con analitica logistica y dashboard operativo.

## 2. Estructura recomendada (cronometro real)

### Opcion A: 10-11 minutos (compacta)

1. `00:00 - 01:00` Introduccion y problema.
2. `01:00 - 03:00` Arquitectura E2E (NiFi, Kafka, Spark, Hive, Cassandra, Airflow, Dashboard).
3. `03:00 - 06:30` Demo dashboard + rutas + insights + reentreno IA.
4. `06:30 - 08:30` Operacion y resiliencia (healthchecks, fallback, scripts).
5. `08:30 - 10:00` Cierre: resultados, limites y siguientes pasos.

### Opcion B: 13-15 minutos (completa)

1. `00:00 - 01:30` Introduccion y objetivos.
2. `01:30 - 04:00` Arquitectura y flujo de datos KDD.
3. `04:00 - 09:30` Demo funcional guiada (caso de uso completo).
4. `09:30 - 12:00` Robustez operativa y validaciones.
5. `12:00 - 14:00` Decisiones tecnicas clave + trade-offs.
6. `14:00 - 15:00` Conclusiones y preguntas.

## 3. Guion hablado (texto base)

## 3.1 Apertura (1 minuto)

Texto sugerido (natural):

> "Buenos dias. En esta presentacion os voy a enseñar un sistema Big Data logistico completo, de extremo a extremo.  
> Partimos de eventos GPS y clima, los procesamos en batch y en streaming, y terminamos en un dashboard operativo que sirve para tomar decisiones reales de ruta."

Mensaje clave:

- No es solo una demo visual: es una plataforma operable de extremo a extremo.

## 3.2 Arquitectura (2-3 minutos, tono conversacional)

Apoyate en `docs/architecture-diagram.png`.

Puntos que debes mencionar:

1. `NiFi` separa ingestas en `gps_ingestion` y `weather_ingestion`.
2. `Kafka` desacopla productores/consumidores (`raw` y `filtered`).
3. `Spark`:
   - `batch` para historico, grafos y ML,
   - `streaming` para estado vivo.
4. `Hive` para capa analitica SQL y reporting.
5. `Cassandra` para baja latencia del dashboard.
6. `Airflow` para healthchecks y orquestacion.
7. `Dashboard` como capa de explotacion operativa.

Frase de impacto (si quieres remarcar):

> "La clave fue separar capa operativa en tiempo real (Cassandra) de capa analitica historica (Hive), manteniendo trazabilidad raw en HDFS."

Frase alternativa mas sencilla:

> "Para resumirlo: Cassandra nos da reflejo rapido de lo que pasa ahora, y Hive nos da analitica completa para estudiar y justificar decisiones."

## 3.3 Demo funcional (3-6 minutos)

### Preparacion previa (antes de entrar a clase)

Ejecuta:

```bash
./scripts/start_kdd.sh
docker compose ps
curl -s http://localhost:8501/health
```

Abre:

- Dashboard: `http://localhost:8501`
- (Opcional) Airflow: `http://localhost:8080`
- (Opcional) NiFi: `https://localhost:8443/nifi`

### Secuencia demo recomendada

1. **Vista Tiempo Real**
   - Enseña flota viva, filtros `Origen RT`/`Destino RT`.
   - Demuestra que los filtros de esta vista no rompen la vista de red.
   - Frase sugerida:
     > "Aqui estoy filtrando solo la operacion en tiempo real. Fijaos que no estoy tocando el analisis de red de la derecha."

2. **Vista Red Logistica**
   - Selecciona `Origen` y `Destino`.
   - Cambia `Perfil` entre `balanced`, `fastest`, `resilient`, `eco`, `low_risk`, `reliable`.
   - Muestra cambio en ruta y metrica de tiempo/riesgo/eco.
   - Frase sugerida:
     > "El mismo origen-destino puede dar rutas diferentes segun objetivo: si priorizo rapidez, riesgo o fiabilidad de ETA."

3. **Multiobjetivo + temporal**
   - Ajusta sliders `Peso tiempo/riesgo/eco`.
   - Cambia `Patron horario` (`peak` y `night`) para comparar.
   - Marca `Evitar nodos` y recalcula.
   - Frase sugerida:
     > "Con esto simulo restricciones reales de operacion: por ejemplo evitar un nodo conflictivo o una hora punta."

4. **Insights de red (live)**
   - Explica `Cuellos de botella` y `Nodos criticos`.
   - Muestra historico en Cassandra/Hive (`insights-sync`).
   - Frase sugerida:
     > "No solo vemos una foto puntual; tambien guardamos historico para comparar si la red mejora o empeora."

5. **Reentreno IA**
   - Enseña boton `Reentrenar IA`.
   - Explica status + recomendacion por score de deriva.
   - Comenta endpoints: `POST /api/ml/retrain`, `GET /api/ml/retrain/status`.
   - Frase sugerida:
     > "Este boton no reentrena a ciegas: primero tenemos una recomendacion con score de deriva y reglas de cooldown."

## 3.4 Robustez y operacion (2-3 minutos)

Mensajes clave:

1. Healthchecks diario y horario en Airflow.
2. Auto-recuperacion meteo cuando vista Madrid queda vacia.
3. Compatibilidad de objetos streaming Hive (`ensure_hive_streaming_compat.sh`).
4. Fallbacks:
   - Dashboard: Cassandra primario + archivos NiFi.
   - Spark streaming: Hive + fallback Parquet.

Como contarlo de forma clara:

> "Si una pieza falla, no nos quedamos bloqueados. El sistema degrada de forma controlada y tenemos scripts para recuperar estado rapido."

Comandos que puedes citar:

```bash
./scripts/validate_hive_pipeline.sh
./scripts/ensure_hive_streaming_compat.sh
curl -s http://localhost:8501/api/debug/sources
```

## 3.5 Cierre (1 minuto)

Texto sugerido (natural):

> "En resumen: el proyecto no se queda en teoria. Hemos montado un pipeline completo, reproducible y operable, con analitica de rutas que realmente ayuda a decidir.  
> Y como extra, añadimos control de deriva y reentreno asistido para mantener el modelo util en el tiempo."

## 3.6 Mini chuleta de transiciones (para sonar fluido)

Usa estas frases puente entre bloques:

1. Arquitectura -> Demo:
   > "Una vez visto como esta montado, os enseño que hace en tiempo real."
2. Demo -> Operacion:
   > "Y ahora la parte importante: como mantenemos esto estable cuando hay incidencias."
3. Operacion -> Cierre:
   > "Con todo esto, cierro con los resultados y por que creemos que cumple los objetivos del proyecto."

## 3.7 Consejos para no sonar leido

1. No memorices parrafos: memoriza ideas por bloque.
2. Habla mirando la pantalla cuando demuestres, y al profesor cuando concluyas cada bloque.
3. Si te atascas, vuelve a esta estructura corta:
   - que problema resolvemos,
   - como lo resolvemos,
   - evidencia en demo,
   - por que es robusto.
4. Termina cada bloque con una frase de conclusion breve.
5. Lleva una velocidad algo mas lenta de la habitual (mejor claridad que correr).

## 4. Plan B si algo falla en directo

## 4.1 Si el dashboard no carga

```bash
docker compose ps
docker compose restart dashboard
curl -s http://localhost:8501/health
```

## 4.2 Si faltan datos de clima en Hive

```bash
./scripts/repopulate_hive_weather_from_cassandra.sh
```

## 4.3 Si hay errores de vistas streaming

```bash
./scripts/ensure_hive_streaming_compat.sh
```

## 4.4 Si no puedes hacer demo viva

Usa esta narrativa:

1. arquitectura + flujo (diagrama),
2. captura dashboard actualizada (`docs/dashboard.png`),
3. evidencias en docs:
   - `docs/operations.md`
   - `docs/memoria-tecnica-sistema.md`
   - `docs/release-notes-2026-04-02.md`

## 5. Preguntas tipicas del profesor (y respuesta corta)

1. **"Que aporta Cassandra frente a Hive?"**  
   Cassandra sirve baja latencia para estado vivo por vehiculo; Hive para analitica historica y reporting.

2. **"Como gestionas consistencia temporal?"**  
   Parsing robusto de timestamps UTC, vistas Madrid en Hive y control de frescura de eventos.

3. **"Que pasa si se cae una parte del pipeline?"**  
   Hay degradacion controlada: healthchecks, fallback de fuentes y scripts de recuperacion.

4. **"Por que varios perfiles de ruta?"**  
   Porque negocio real tiene objetivos distintos: velocidad, robustez, coste eco y estabilidad de ETA.

5. **"Como justificas el boton de reentreno?"**  
   Permite operacion asistida del modelo con recomendacion por deriva (score + umbral + cooldown).

## 6. Checklist final (30 segundos antes de exponer)

- [ ] `docker compose ps` sin servicios clave caidos.
- [ ] Dashboard abierto y refrescado (`Ctrl+F5`).
- [ ] Caso de ruta preparado (origen/destino ya pensados).
- [ ] Frase de apertura y cierre memorizadas.
- [ ] Plan B listo por si falla internet/servicio.
