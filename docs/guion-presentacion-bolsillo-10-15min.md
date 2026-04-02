# Guion De Bolsillo (10-15 min)

Repositorio GitHub:

- `https://github.com/raulsistemasydesarrollo/KDD-Ingenieria-de-datos`

## 0) Apertura (20-30s)

> "Voy a presentar una plataforma Big Data logistica completa: ingesta GPS/clima, procesamiento batch+streaming, analitica en Hive/Cassandra y dashboard operativo para decision de rutas."

## 1) Estructura rapida (di esto al principio)

1. Problema y objetivo.
2. Arquitectura end-to-end.
3. Demo funcional.
4. Robustez operativa.
5. Cierre.

## 2) Mensaje por bloque (frases cortas)

### Problema y objetivo (1 min)

- Problema: decisiones de ruta con incertidumbre (trafico/clima/delay).
- Objetivo: pasar de datos crudos a decision operativa en tiempo real.

### Arquitectura (2-3 min)

- NiFi separa `gps_ingestion` y `weather_ingestion`.
- Kafka desacopla con topics raw/filtered.
- Spark batch + streaming.
- Hive = analitica historica; Cassandra = baja latencia operativa.
- Airflow = healthchecks y orquestacion.
- Dashboard = explotacion final.

Frase:

> "Cassandra me da el ahora; Hive me da el analisis completo."

### Demo (4-6 min)

1. **Tiempo Real**: filtros `Origen RT/Destino RT`.
2. **Red Logistica**: calcular ruta por perfil.
3. **Perfiles**: `balanced`, `fastest`, `resilient`, `eco`, `low_risk`, `reliable`.
4. **Multiobjetivo**: pesos tiempo/riesgo/eco.
5. **Patron horario**: `peak` vs `night`.
6. **Evitar nodos**: recalculo con restriccion.
7. **Insights live**: cuellos de botella + nodos criticos + historico.
8. **Reentreno IA**: boton + estado + recomendacion + paneles de candidatos (izq: modelo en uso, der: descripciones).

Frase:

> "Mismo origen-destino, rutas distintas segun objetivo operativo."

### Robustez (2 min)

- Healthchecks diario/horario en Airflow.
- Auto-recuperacion meteo en Hive.
- Fallbacks controlados (dashboard y Spark).
- Scripts de recuperacion rapida.

Frase:

> "Si falla una pieza, el sistema degrada de forma controlada."

### Cierre (45-60s)

> "Resultado: pipeline KDD completo, reproducible y operable, con optimizacion de rutas multiobjetivo y soporte de decision en tiempo real."

## 3) Transiciones (usa una por bloque)

- Arquitectura -> Demo:
  > "Ahora os enseño que esto funciona en vivo."
- Demo -> Robustez:
  > "Y lo importante: como se mantiene estable cuando hay incidencias."
- Robustez -> Cierre:
  > "Con esto cierro con el valor final del proyecto."

## 4) Preguntas tipicas (respuesta en 1 linea)

1. Cassandra vs Hive:
   - Cassandra para consulta operativa inmediata; Hive para analitica historica.
2. Si cae algo:
   - Hay healthchecks, fallback y scripts de recuperacion.
3. Por que varios perfiles:
   - Porque negocio real optimiza objetivos distintos (tiempo, riesgo, eco, fiabilidad).
4. Reentreno IA para que:
   - Para mantener el modelo util ante deriva, con recomendacion y cooldown.

## 5) Plan B tecnico (comandos minimos)

```bash
docker compose ps
curl -s http://localhost:8501/health
docker compose restart dashboard
./scripts/ensure_hive_streaming_compat.sh
./scripts/repopulate_hive_weather_from_cassandra.sh
```

## 6) Recordatorio final (10s antes de empezar)

- Habla por ideas, no leyendo.
- Una conclusion clara por bloque.
- Si te atascas: problema -> solucion -> evidencia -> robustez.
