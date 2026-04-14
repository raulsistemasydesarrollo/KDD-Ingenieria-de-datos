# Guion Hablado (3 minutos)
## Comité de Dirección - Trazabilidad del Dato

Fecha: 14/04/2026
Duración estimada: 3 minutos
Objetivo: abrir la reunión con mensaje claro de valor, control y decisiones

---

## 0) Apertura (20 segundos)
Hoy venimos a validar que el dato logístico está completamente trazado desde origen hasta decisión operativa, y que esa trazabilidad ya está produciendo valor en operación, analítica de red e inteligencia predictiva.

---

## 1) Qué hemos conseguido (40 segundos)
La plataforma ya garantiza recorrido completo del dato en 12 puntos auditables:
1. Captura de GPS, meteorología y maestros.
2. Ingesta y enrutado técnico con NiFi y Kafka.
3. Procesamiento batch y streaming con Spark.
4. Persistencia analítica en Hive y operacional en Cassandra.
5. Consumo final en dashboard para mapas, KPIs, rutas y bloque IA.

Mensaje para dirección: no solo vemos datos, vemos el camino completo del dato y podemos demostrarlo con evidencia técnica reproducible.

---

## 2) Qué decisiones habilita (45 segundos)
Con esta trazabilidad se habilitan cuatro decisiones de negocio:
1. Operación diaria en tiempo casi real de flota y clima.
2. Priorización de cuellos de botella y nodos críticos de red.
3. Optimización de rutas con criterios multiobjetivo.
4. Anticipación de incidencias con scoring de riesgo de retraso.

En términos simples: pasamos de reaccionar tarde a decidir antes.

---

## 3) Qué control tenemos sobre el dato (45 segundos)
Hay controles técnicos que reducen riesgo operativo:
1. Dedupe de eventos para evitar duplicidades.
2. Watermark en streaming para eventos tardíos.
3. Ventanas temporales homogéneas para comparabilidad.
4. Fallbacks activos si Hive o Cassandra tienen indisponibilidad puntual.

Traducción ejecutiva: incluso con incidencia técnica, el sistema mantiene continuidad informativa.

---

## 4) Riesgos abiertos y mitigación (30 segundos)
Riesgos relevantes:
1. Caída temporal de Cassandra.
   Mitigación: fallback de lectura desde fuentes NiFi.
2. Escritura Hive streaming no disponible puntualmente.
   Mitigación: persistencia alternativa en Parquet.
3. Reintentos de ingesta con riesgo de duplicados.
   Mitigación: deduplicación en Spark antes de consolidar.

---

## 5) Cierre y decisiones propuestas al comité (40 segundos)
Proponemos aprobar cuatro decisiones:
1. Formalizar esta trazabilidad como estándar de gobierno del dato logístico.
2. Fijar SLA de freshness operativo, por ejemplo menor de 15 minutos.
3. Adoptar cuadro de mando directivo con 6 KPIs: freshness, cobertura, delay, severidad meteo, cuellos de botella y rendimiento IA.
4. Establecer revisión mensual de modelo IA y deriva.

Cierre: la base tecnológica ya está preparada; ahora toca institucionalizarla como práctica de gestión.

---

## Anexo breve para el portavoz
Si hay una sola frase para cerrar:
"Tenemos datos trazables, decisiones accionables y continuidad operativa incluso ante incidencias técnicas."
