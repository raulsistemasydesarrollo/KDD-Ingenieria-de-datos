package com.proyectobigdata;

import com.datastax.oss.driver.api.core.CqlSession;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import org.graphframes.GraphFrame;
import org.apache.spark.ml.Pipeline;
import org.apache.spark.ml.PipelineModel;
import org.apache.spark.ml.PipelineStage;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.OneHotEncoder;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.VectorAssembler;
import org.apache.spark.ml.regression.RandomForestRegressor;
import org.apache.spark.api.java.function.VoidFunction2;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.apache.spark.storage.StorageLevel;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.broadcast;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.coalesce;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_entries;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.date_trunc;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.sqrt;
import static org.apache.spark.sql.functions.pow;
import static org.apache.spark.sql.functions.hour;
import static org.apache.spark.sql.functions.dayofweek;
import static org.apache.spark.sql.functions.sin;
import static org.apache.spark.sql.functions.cos;

/**
 * Job principal del pipeline KDD logistico.
 *
 * <p>Modos de ejecucion:
 * <ul>
 *   <li>batch: procesa historico, genera metricas, grafo, shortest paths y scoring ML.</li>
 *   <li>streaming: consume GPS/Weather desde Kafka y persiste resultados en Hive/Cassandra.</li>
 *   <li>insights-sync: consolida snapshots de insights de Cassandra hacia tablas Hive.</li>
 * </ul>
 *
 * <p>Este programa prioriza resiliencia operativa:
 * si falla escritura en Hive de flujos streaming, usa fallback parquet en HDFS.
 */
public final class LogisticsAnalyticsJob {

    private static final String DATABASE = "transport_analytics";
    private static final String RAW_PATH = "hdfs://hadoop:9000/data/raw/gps_events.jsonl";
    private static final String WAREHOUSES_PATH = "hdfs://hadoop:9000/data/master/warehouses.csv";
    private static final String VEHICLES_PATH = "hdfs://hadoop:9000/data/master/vehicles.csv";
    private static final String GRAPH_VERTICES_PATH = "hdfs://hadoop:9000/data/graph/vertices.csv";
    private static final String GRAPH_EDGES_PATH = "hdfs://hadoop:9000/data/graph/edges.csv";
    private static final String MASTER_WAREHOUSES_TABLE = DATABASE + ".master_warehouses";
    private static final String MASTER_VEHICLES_TABLE = DATABASE + ".master_vehicles";
    private static final String STREAMING_METRICS_FALLBACK_PATH =
            "hdfs://hadoop:9000/data/curated/delay_metrics_streaming";
    private static final String STREAMING_WEATHER_FALLBACK_PATH =
            "hdfs://hadoop:9000/data/curated/weather_observations_streaming";
    private static final String STREAMING_ENRICHED_EVENTS_FALLBACK_PATH =
            "hdfs://hadoop:9000/data/curated/enriched_events_streaming";
    private static final String DELAY_RISK_MODEL_PATH = "hdfs://hadoop:9000/models/delay_risk_rf";
    private static final String CASSANDRA_INSIGHTS_TABLE = "network_insights_snapshots";
    private static final String HIVE_INSIGHTS_SNAPSHOTS_TABLE = DATABASE + ".network_insights_snapshots_hive";
    private static final String HIVE_INSIGHTS_HOURLY_TRENDS_TABLE = DATABASE + ".network_insights_hourly_trends";
    private static final String WEATHER_STREAMING_TABLE = DATABASE + ".weather_observations_streaming";
    private static final String DELAY_STREAMING_TABLE = DATABASE + ".delay_metrics_streaming";
    private static final String DELAY_BATCH_TABLE = DATABASE + ".delay_metrics_batch";
    private static final int SHUFFLE_PARTITIONS =
            Integer.parseInt(System.getenv().getOrDefault("SPARK_SQL_SHUFFLE_PARTITIONS", "4"));
    private static final String AUTO_BROADCAST_THRESHOLD =
            System.getenv().getOrDefault("SPARK_SQL_AUTO_BROADCAST_JOIN_THRESHOLD", "20971520");

    private LogisticsAnalyticsJob() {
    }

    public static void main(String[] args) throws Exception {
        // Inicializacion comun de Spark + Hive + Cassandra, y dispatch por modo.
        String mode = args.length == 0 ? "batch" : args[0].toLowerCase(Locale.ROOT);
        String sqlTimeZone = System.getenv().getOrDefault("SPARK_SQL_TIMEZONE", "Europe/Madrid");
        String sparkSerializer =
                System.getenv().getOrDefault("SPARK_SERIALIZER", "org.apache.spark.serializer.KryoSerializer");

        SparkSession spark = SparkSession.builder()
                .appName("LogisticsKddJob")
                .master(System.getenv().getOrDefault("SPARK_MASTER", "yarn"))
                .config("spark.sql.session.timeZone", sqlTimeZone)
                .config("spark.sql.shuffle.partitions", Integer.toString(SHUFFLE_PARTITIONS))
                .config("spark.sql.adaptive.enabled", "true")
                .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
                .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
                .config("spark.sql.adaptive.skewJoin.enabled", "true")
                .config("spark.sql.autoBroadcastJoinThreshold", AUTO_BROADCAST_THRESHOLD)
                .config("spark.serializer", sparkSerializer)
                .config("spark.rdd.compress", "true")
                .config("spark.shuffle.compress", "true")
                .config("spark.shuffle.spill.compress", "true")
                .config("spark.sql.warehouse.dir", "hdfs://hadoop:9000/user/hive/warehouse")
                .config("spark.cassandra.connection.host", "cassandra")
                .config("spark.cassandra.connection.port", "9042")
                .enableHiveSupport()
                .getOrCreate();

        spark.sparkContext().setLogLevel("WARN");
        spark.sparkContext().setCheckpointDir("hdfs://hadoop:9000/tmp/graphframes-checkpoints");
        ensureHiveDatabase(spark);
        ensureHiveMasterTables(spark);
        ensureCassandraSchema();

        switch (mode) {
            case "batch" -> runBatchAnalysis(spark);
            case "streaming" -> runStreamingAnalysis(spark);
            case "insights-sync" -> runInsightsSync(spark);
            default -> throw new IllegalArgumentException("Modo no soportado: " + mode);
        }

        spark.stop();
    }

    private static void runBatchAnalysis(SparkSession spark) {
        // Pipeline batch completo para capa analitica historica.
        Dataset<Row> rawEvents = spark.read()
                .schema(rawSchema())
                .json(RAW_PATH);

        Dataset<Row> cleanedEvents = cleanAndNormalizeEvents(rawEvents)
                .dropDuplicates("event_id");
        Dataset<Row> enrichedEvents = enrichEvents(spark, cleanedEvents)
                .persist(StorageLevel.MEMORY_AND_DISK());

        try {
            enrichedEvents.write()
                    .mode(SaveMode.Overwrite)
                    .format("parquet")
                    .save("hdfs://hadoop:9000/data/curated/enriched_events");

            enrichedEvents.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(DATABASE + ".enriched_events");

            Dataset<Row> delayMetrics = enrichedEvents
                    .groupBy(
                            window(col("event_timestamp"), "15 minutes"),
                            col("warehouse_id"),
                            col("warehouse_name"),
                            col("region"))
                    .agg(
                            avg("delay_minutes").alias("avg_delay_minutes"),
                            avg("speed_kmh").alias("avg_speed_kmh"),
                            count(lit(1)).alias("event_count"))
                    .select(
                            col("window.start").alias("window_start"),
                            col("window.end").alias("window_end"),
                            col("warehouse_id"),
                            col("warehouse_name"),
                            col("region"),
                            col("avg_delay_minutes"),
                            col("avg_speed_kmh"),
                            col("event_count"));

            delayMetrics.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(DATABASE + ".delay_metrics_batch");

            Dataset<Row> graphMetrics = computeGraphMetrics(spark);
            graphMetrics.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(DATABASE + ".route_graph_metrics");
            Dataset<Row> shortestPathMetrics = computeShortestPathMetrics(spark);
            shortestPathMetrics.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(DATABASE + ".route_shortest_paths");

            saveLatestVehicleState(enrichedEvents);
            trainAndScoreDelayRiskModel(spark, enrichedEvents);
        } finally {
            enrichedEvents.unpersist();
        }
    }

    private static void runStreamingAnalysis(SparkSession spark) throws Exception {
        // Pipeline continuo de GPS y clima para monitorizacion operativa.
        String startingOffsets = System.getenv().getOrDefault("STREAMING_STARTING_OFFSETS", "latest");
        String maxOffsetsPerTrigger = System.getenv().getOrDefault("STREAMING_MAX_OFFSETS_PER_TRIGGER", "8000");

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "transport.filtered")
                .option("startingOffsets", startingOffsets)
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .load();
        Dataset<Row> weatherKafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "transport.weather.filtered")
                .option("startingOffsets", startingOffsets)
                .option("maxOffsetsPerTrigger", maxOffsetsPerTrigger)
                .load();

        Dataset<Row> parsedEvents = kafkaStream
                .selectExpr("CAST(value AS STRING) AS raw_json")
                .select(from_json(col("raw_json"), rawSchema()).alias("payload"))
                .select("payload.*");
        Dataset<Row> parsedWeather = weatherKafkaStream
                .selectExpr("CAST(value AS STRING) AS raw_json")
                .select(
                        get_json_object(col("raw_json"), "$.weather_event_id").alias("weather_event_id"),
                        get_json_object(col("raw_json"), "$.warehouse_id").alias("warehouse_id"),
                        get_json_object(col("raw_json"), "$.temperature_c").alias("temperature_c"),
                        get_json_object(col("raw_json"), "$.precipitation_mm").alias("precipitation_mm"),
                        get_json_object(col("raw_json"), "$.wind_kmh").alias("wind_kmh"),
                        get_json_object(col("raw_json"), "$.weather_code").alias("weather_code"),
                        get_json_object(col("raw_json"), "$.source").alias("source"),
                        get_json_object(col("raw_json"), "$.observation_time").alias("observation_time"));

        Dataset<Row> cleanedStream = cleanAndNormalizeEvents(parsedEvents)
                .withWatermark("event_timestamp", "20 minutes")
                .dropDuplicates("event_id");
        Dataset<Row> enrichedStream = enrichEvents(spark, cleanedStream);
        Dataset<Row> weatherStream = cleanAndNormalizeWeather(parsedWeather)
                .withWatermark("weather_timestamp", "20 minutes")
                .dropDuplicates("weather_event_id");

        Dataset<Row> windowedMetrics = enrichedStream
                .groupBy(window(col("event_timestamp"), "15 minutes"), col("warehouse_id"))
                .agg(
                        avg("delay_minutes").alias("avg_delay_minutes"),
                        avg("speed_kmh").alias("avg_speed_kmh"),
                        max("event_timestamp").alias("last_event_timestamp"),
                        count(lit(1)).alias("event_count"))
                .select(
                        col("window.start").alias("window_start"),
                        col("window.end").alias("window_end"),
                        col("warehouse_id"),
                        col("avg_delay_minutes"),
                        col("avg_speed_kmh"),
                        col("last_event_timestamp"),
                        col("event_count"));

        VoidFunction2<Dataset<Row>, Long> metricsBatchWriter =
                (batch, batchId) -> appendToHiveTable(batch, DATABASE + ".delay_metrics_streaming");
        VoidFunction2<Dataset<Row>, Long> stateBatchWriter =
                (batch, batchId) -> {
                    appendToHiveTable(
                            batch,
                            DATABASE + ".enriched_events_streaming",
                            STREAMING_ENRICHED_EVENTS_FALLBACK_PATH);
                    saveLatestVehicleState(batch);
                };
        VoidFunction2<Dataset<Row>, Long> weatherBatchWriter =
                (batch, batchId) -> {
                    appendToHiveTable(
                            batch,
                            DATABASE + ".weather_observations_streaming",
                            STREAMING_WEATHER_FALLBACK_PATH);
                    saveWeatherObservationsToCassandra(batch);
                };

        windowedMetrics.writeStream()
                .outputMode("update")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/delay_metrics")
                .foreachBatch(metricsBatchWriter)
                .start();

        enrichedStream.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/latest_vehicle_state")
                .foreachBatch(stateBatchWriter)
                .start();
        weatherStream.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/weather_observations")
                .foreachBatch(weatherBatchWriter)
                .start();

        spark.streams().awaitAnyTermination();
    }

    private static void runInsightsSync(SparkSession spark) {
        // Consolida snapshots de insights (Cassandra) en tablas Hive para reporting.
        try {
            Dataset<Row> cassandraInsights = spark.read()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "transport")
                    .option("table", CASSANDRA_INSIGHTS_TABLE)
                    .load();

            if (cassandraInsights.isEmpty()) {
                System.err.println("WARN: no hay snapshots de insights en Cassandra para sincronizar.");
                return;
            }

            Dataset<Row> normalized = cassandraInsights
                    .filter(col("snapshot_time").isNotNull())
                    .select(
                            col("bucket"),
                            col("entity_type"),
                            col("profile"),
                            col("min_congestion"),
                            col("snapshot_time"),
                            col("rank"),
                            col("entity_id"),
                            col("impact_score"),
                            col("criticality_score"),
                            col("effective_avg_delay_minutes"),
                            col("total_minutes"),
                            col("congestion_level"),
                            col("live_sample_count"))
                    .withColumn("snapshot_hour", date_trunc("hour", col("snapshot_time")))
                    .persist(StorageLevel.MEMORY_AND_DISK());

            try {
                normalized.write()
                        .mode(SaveMode.Overwrite)
                        .saveAsTable(HIVE_INSIGHTS_SNAPSHOTS_TABLE);

            WindowSpec topEdgeWindow = Window
                    .partitionBy("snapshot_hour", "profile", "min_congestion")
                    .orderBy(col("impact_score").desc(), col("rank").asc(), col("entity_id").asc());
            Dataset<Row> topEdges = normalized
                    .filter(col("entity_type").equalTo("edge"))
                    .withColumn("row_num", row_number().over(topEdgeWindow))
                    .filter(col("row_num").equalTo(1))
                    .select(
                            col("snapshot_hour"),
                            col("profile"),
                            col("min_congestion"),
                            col("entity_id").alias("top_edge_id"),
                            col("impact_score").alias("top_edge_impact_score"),
                            col("effective_avg_delay_minutes").alias("top_edge_delay_minutes"),
                            col("total_minutes").alias("top_edge_total_minutes"),
                            col("congestion_level").alias("top_edge_congestion_level"),
                            col("live_sample_count").alias("top_edge_live_sample_count"));

            WindowSpec topNodeWindow = Window
                    .partitionBy("snapshot_hour", "profile", "min_congestion")
                    .orderBy(col("criticality_score").desc(), col("rank").asc(), col("entity_id").asc());
            Dataset<Row> topNodes = normalized
                    .filter(col("entity_type").equalTo("node"))
                    .withColumn("row_num", row_number().over(topNodeWindow))
                    .filter(col("row_num").equalTo(1))
                    .select(
                            col("snapshot_hour"),
                            col("profile"),
                            col("min_congestion"),
                            col("entity_id").alias("top_node_id"),
                            col("criticality_score").alias("top_node_criticality_score"),
                            col("effective_avg_delay_minutes").alias("top_node_avg_incident_delay_minutes"),
                            col("total_minutes").alias("top_node_avg_profile_minutes"));

                Dataset<Row> hourlyTrends = topEdges
                        .join(topNodes, new String[]{"snapshot_hour", "profile", "min_congestion"}, "full_outer")
                        .orderBy(col("snapshot_hour").desc(), col("profile").asc(), col("min_congestion").asc());

                hourlyTrends.write()
                        .mode(SaveMode.Overwrite)
                        .saveAsTable(HIVE_INSIGHTS_HOURLY_TRENDS_TABLE);
            } finally {
                normalized.unpersist();
            }

            System.out.println(
                    "INFO: insights-sync completado. Tablas Hive: "
                            + HIVE_INSIGHTS_SNAPSHOTS_TABLE + ", "
                            + HIVE_INSIGHTS_HOURLY_TRENDS_TABLE);
        } catch (Exception syncFailure) {
            System.err.println(
                    "WARN: fallo en insights-sync Cassandra->Hive. "
                            + "Se omite para no bloquear pipeline. Causa: "
                            + syncFailure.getMessage());
        }
    }

    private static Dataset<Row> enrichEvents(SparkSession spark, Dataset<Row> cleanedEvents) {
        // Join con maestros de almacenes y vehiculos para contexto analitico.
        Dataset<Row> warehouses = spark.table(MASTER_WAREHOUSES_TABLE)
                // Evita colision con coordenadas GPS del evento original.
                .withColumnRenamed("latitude", "warehouse_latitude")
                .withColumnRenamed("longitude", "warehouse_longitude");
        Dataset<Row> vehicles = spark.table(MASTER_VEHICLES_TABLE);

        return cleanedEvents
                .join(broadcast(warehouses), "warehouse_id", "left")
                .join(broadcast(vehicles), "vehicle_id", "left");
    }

    private static void ensureHiveMasterTables(SparkSession spark) {
        // Publica/actualiza tablas maestras en Hive desde CSV en HDFS.
        Dataset<Row> warehouses = spark.read()
                .option("header", "true")
                .csv(WAREHOUSES_PATH);
        Dataset<Row> vehicles = spark.read()
                .option("header", "true")
                .csv(VEHICLES_PATH);

        warehouses.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(MASTER_WAREHOUSES_TABLE);
        vehicles.write()
                .mode(SaveMode.Overwrite)
                .saveAsTable(MASTER_VEHICLES_TABLE);
    }

    private static Dataset<Row> computeGraphMetrics(SparkSession spark) {
        // Metricas de grafo: componentes conectados + pagerank sobre red logistica.
        Dataset<Row> vertices = spark.read()
                .option("header", "true")
                .csv(GRAPH_VERTICES_PATH);

        Dataset<Row> edges = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(GRAPH_EDGES_PATH);

        GraphFrame graph = GraphFrame.apply(vertices, edges);
        Dataset<Row> components = graph.connectedComponents().run()
                .withColumnRenamed("component", "community_id");
        Dataset<Row> rankedVertices = graph.pageRank()
                .resetProbability(0.15)
                .maxIter(10)
                .run()
                .vertices()
                .select("id", "pagerank");

        return components
                .join(rankedVertices, "id")
                .orderBy(col("pagerank").desc());
    }

    private static Dataset<Row> computeShortestPathMetrics(SparkSession spark) {
        // Shortest paths por saltos desde landmarks (almacenes criticos).
        Dataset<Row> vertices = spark.read()
                .option("header", "true")
                .csv(GRAPH_VERTICES_PATH);
        Dataset<Row> edges = spark.read()
                .option("header", "true")
                .option("inferSchema", "true")
                .csv(GRAPH_EDGES_PATH);

        ArrayList<Object> landmarks = new ArrayList<>();
        List<Row> highCriticalWarehouses = spark.table(MASTER_WAREHOUSES_TABLE)
                .filter(col("criticality").equalTo("high"))
                .select("warehouse_id")
                .collectAsList();
        for (Row row : highCriticalWarehouses) {
            landmarks.add(row.getString(0));
        }

        if (landmarks.isEmpty()) {
            List<Row> fallback = vertices.select("id").limit(1).collectAsList();
            for (Row row : fallback) {
                landmarks.add(row.getString(0));
            }
        }

        GraphFrame graph = GraphFrame.apply(vertices, edges);
        Dataset<Row> shortestPaths = graph.shortestPaths()
                .landmarks(landmarks)
                .run();

        return shortestPaths
                .select(
                        col("id").alias("source_warehouse_id"),
                        explode(map_entries(col("distances"))).alias("distance_entry"))
                .select(
                        col("source_warehouse_id"),
                        col("distance_entry.key").alias("target_warehouse_id"),
                        col("distance_entry.value").cast(DataTypes.IntegerType).alias("hop_distance"));
    }

    private static void trainAndScoreDelayRiskModel(SparkSession spark, Dataset<Row> enrichedEvents) {
        // Entrena y aplica modelo de riesgo de retraso a estado reciente por vehiculo.
        Dataset<Row> eventsWithMlContext = null;
        Dataset<Row> mlDataset = null;
        try {
            eventsWithMlContext = enrichEventsWithMlContext(spark, enrichedEvents)
                    .persist(StorageLevel.MEMORY_AND_DISK());

            mlDataset = eventsWithMlContext
                    .filter(col("delay_minutes").isNotNull())
                    .filter(col("speed_kmh").isNotNull())
                    .filter(col("warehouse_id").isNotNull())
                    .filter(col("vehicle_id").isNotNull())
                    .filter(col("vehicle_type").isNotNull())
                    .withColumn("route_id", coalesce(col("route_id"), lit("UNKNOWN")))
                    .withColumn("capacity_kg", coalesce(col("capacity_kg"), lit(0)).cast(DataTypes.DoubleType))
                    .withColumn(
                            "warehouse_criticality_score",
                            when(col("criticality").equalTo("high"), lit(1.0))
                                    .when(col("criticality").equalTo("medium"), lit(0.6))
                                    .when(col("criticality").equalTo("low"), lit(0.3))
                                    .otherwise(lit(0.5)))
                    .withColumn(
                            "vehicle_status_score",
                            when(col("status").equalTo("active"), lit(1.0))
                                    .when(col("status").equalTo("maintenance"), lit(0.25))
                                    .otherwise(lit(0.6)))
                    .withColumn(
                            "distance_to_warehouse_km",
                            sqrt(
                                    pow(col("latitude").minus(col("warehouse_latitude").cast(DataTypes.DoubleType)).multiply(lit(111.32)), 2.0)
                                            .plus(pow(col("longitude").minus(col("warehouse_longitude").cast(DataTypes.DoubleType)).multiply(lit(85.39)), 2.0))))
                    .withColumn("hour_of_day", hour(col("event_timestamp")).cast(DataTypes.DoubleType))
                    .withColumn("day_of_week", dayofweek(col("event_timestamp")))
                    .withColumn(
                            "is_weekend",
                            when(col("day_of_week").isin(1, 7), lit(1.0))
                                    .otherwise(lit(0.0)))
                    .withColumn(
                            "hour_sin",
                            sin(col("hour_of_day").multiply(lit((2.0 * Math.PI) / 24.0))))
                    .withColumn(
                            "hour_cos",
                            cos(col("hour_of_day").multiply(lit((2.0 * Math.PI) / 24.0))))
                    .withColumn("label", col("delay_minutes").cast(DataTypes.DoubleType));
            mlDataset = mlDataset.persist(StorageLevel.MEMORY_AND_DISK());

            long mlCount = mlDataset.count();

            StringIndexer warehouseIndexer = new StringIndexer()
                    .setInputCol("warehouse_id")
                    .setOutputCol("warehouse_idx")
                    .setHandleInvalid("keep");
            StringIndexer vehicleTypeIndexer = new StringIndexer()
                    .setInputCol("vehicle_type")
                    .setOutputCol("vehicle_type_idx")
                    .setHandleInvalid("keep");
            StringIndexer routeIndexer = new StringIndexer()
                    .setInputCol("route_id")
                    .setOutputCol("route_idx")
                    .setHandleInvalid("keep");
            StringIndexer vehicleIdIndexer = new StringIndexer()
                    .setInputCol("vehicle_id")
                    .setOutputCol("vehicle_id_idx")
                    .setHandleInvalid("keep");
            OneHotEncoder baselineEncoder = new OneHotEncoder()
                    .setInputCols(new String[]{"warehouse_idx", "vehicle_type_idx"})
                    .setOutputCols(new String[]{"warehouse_ohe", "vehicle_type_ohe"});
            OneHotEncoder enhancedEncoder = new OneHotEncoder()
                    .setInputCols(new String[]{"warehouse_idx", "vehicle_type_idx", "route_idx", "vehicle_id_idx"})
                    .setOutputCols(new String[]{"warehouse_ohe", "vehicle_type_ohe", "route_ohe", "vehicle_id_ohe"});
            VectorAssembler baselineAssembler = new VectorAssembler()
                    .setInputCols(new String[]{"speed_kmh", "warehouse_ohe", "vehicle_type_ohe"})
                    .setOutputCol("features");
            VectorAssembler enhancedAssembler = new VectorAssembler()
                    .setInputCols(new String[]{
                            "speed_kmh",
                            "capacity_kg",
                            "warehouse_criticality_score",
                            "vehicle_status_score",
                            "distance_to_warehouse_km",
                            "climate_temperature_c",
                            "climate_precipitation_mm",
                            "climate_wind_kmh",
                            "climate_severity_score",
                            "congestion_avg_delay_minutes",
                            "congestion_avg_speed_kmh",
                            "congestion_event_count",
                            "congestion_pressure_score",
                            "hour_sin",
                            "hour_cos",
                            "is_weekend",
                            "warehouse_ohe",
                            "vehicle_type_ohe",
                            "route_ohe",
                            "vehicle_id_ohe"
                    })
                    .setOutputCol("features");
            RandomForestRegressor baselineRf = new RandomForestRegressor()
                    .setFeaturesCol("features")
                    .setLabelCol("label")
                    .setPredictionCol("prediction")
                    .setNumTrees(30)
                    .setMaxDepth(8);
            RandomForestRegressor tunedBaselineRf = new RandomForestRegressor()
                    .setFeaturesCol("features")
                    .setLabelCol("label")
                    .setPredictionCol("prediction")
                    .setNumTrees(90)
                    .setMaxDepth(12);
            RandomForestRegressor enhancedRf = new RandomForestRegressor()
                    .setFeaturesCol("features")
                    .setLabelCol("label")
                    .setPredictionCol("prediction")
                    .setNumTrees(80)
                    .setMaxDepth(10);

            WindowSpec latestByVehicle = Window.partitionBy("vehicle_id").orderBy(col("event_timestamp").desc());
            Dataset<Row> latestState = eventsWithMlContext
                    .withColumn("row_num", row_number().over(latestByVehicle))
                    .filter(col("row_num").equalTo(1))
                    .withColumn("route_id", coalesce(col("route_id"), lit("UNKNOWN")))
                    .withColumn("capacity_kg", coalesce(col("capacity_kg"), lit(0)).cast(DataTypes.DoubleType))
                    .withColumn(
                            "warehouse_criticality_score",
                            when(col("criticality").equalTo("high"), lit(1.0))
                                    .when(col("criticality").equalTo("medium"), lit(0.6))
                                    .when(col("criticality").equalTo("low"), lit(0.3))
                                    .otherwise(lit(0.5)))
                    .withColumn(
                            "vehicle_status_score",
                            when(col("status").equalTo("active"), lit(1.0))
                                    .when(col("status").equalTo("maintenance"), lit(0.25))
                                    .otherwise(lit(0.6)))
                    .withColumn(
                            "distance_to_warehouse_km",
                            sqrt(
                                    pow(col("latitude").minus(col("warehouse_latitude").cast(DataTypes.DoubleType)).multiply(lit(111.32)), 2.0)
                                            .plus(pow(col("longitude").minus(col("warehouse_longitude").cast(DataTypes.DoubleType)).multiply(lit(85.39)), 2.0))))
                    .withColumn("hour_of_day", hour(col("event_timestamp")).cast(DataTypes.DoubleType))
                    .withColumn("day_of_week", dayofweek(col("event_timestamp")))
                    .withColumn(
                            "is_weekend",
                            when(col("day_of_week").isin(1, 7), lit(1.0))
                                    .otherwise(lit(0.0)))
                    .withColumn(
                            "hour_sin",
                            sin(col("hour_of_day").multiply(lit((2.0 * Math.PI) / 24.0))))
                    .withColumn(
                            "hour_cos",
                            cos(col("hour_of_day").multiply(lit((2.0 * Math.PI) / 24.0))))
                    .select(
                            col("vehicle_id"),
                            col("warehouse_id"),
                            col("route_id"),
                            col("vehicle_type"),
                            col("capacity_kg"),
                            col("warehouse_criticality_score"),
                            col("vehicle_status_score"),
                            col("distance_to_warehouse_km"),
                            col("climate_temperature_c"),
                            col("climate_precipitation_mm"),
                            col("climate_wind_kmh"),
                            col("climate_severity_score"),
                            col("congestion_avg_delay_minutes"),
                            col("congestion_avg_speed_kmh"),
                            col("congestion_event_count"),
                            col("congestion_pressure_score"),
                            col("hour_sin"),
                            col("hour_cos"),
                            col("is_weekend"),
                            col("speed_kmh"),
                            col("event_timestamp"));

            Dataset<Row> scoredLatest;
            if (mlCount >= 3) {
                Dataset<Row> train;
                Dataset<Row> test;
                if (mlCount >= 20) {
                    Dataset<Row>[] split = mlDataset.randomSplit(new double[]{0.8, 0.2}, 42L);
                    train = split[0];
                    test = split[1].isEmpty() ? split[0] : split[1];
                } else {
                    train = mlDataset;
                    test = mlDataset;
                }

                Pipeline baselinePipeline = new Pipeline().setStages(new PipelineStage[]{
                        warehouseIndexer, vehicleTypeIndexer, baselineEncoder, baselineAssembler, baselineRf});
                PipelineModel baselineModel = baselinePipeline.fit(train);
                Dataset<Row> baselineScoredTest = baselineModel.transform(test);
                Pipeline tunedBaselinePipeline = new Pipeline().setStages(new PipelineStage[]{
                        warehouseIndexer, vehicleTypeIndexer, baselineEncoder, baselineAssembler, tunedBaselineRf});
                PipelineModel tunedBaselineModel = tunedBaselinePipeline.fit(train);
                Dataset<Row> tunedBaselineScoredTest = tunedBaselineModel.transform(test);

                Pipeline enhancedPipeline = new Pipeline().setStages(new PipelineStage[]{
                        warehouseIndexer,
                        vehicleTypeIndexer,
                        routeIndexer,
                        vehicleIdIndexer,
                        enhancedEncoder,
                        enhancedAssembler,
                        enhancedRf
                });
                PipelineModel enhancedModel = enhancedPipeline.fit(train);
                Dataset<Row> enhancedScoredTest = enhancedModel.transform(test);
                RegressionEvaluator evaluator = new RegressionEvaluator()
                        .setLabelCol("label")
                        .setPredictionCol("prediction")
                        .setMetricName("rmse");
                double baselineRmse = evaluator.evaluate(baselineScoredTest);
                double tunedBaselineRmse = evaluator.evaluate(tunedBaselineScoredTest);
                double enhancedRmse = evaluator.evaluate(enhancedScoredTest);
                PipelineModel selectedModel = baselineModel;
                double selectedRmse = baselineRmse;
                String selectedName = "baseline_rf";
                if (tunedBaselineRmse < selectedRmse) {
                    selectedModel = tunedBaselineModel;
                    selectedRmse = tunedBaselineRmse;
                    selectedName = "tuned_baseline_rf";
                }
                if (enhancedRmse < selectedRmse) {
                    selectedModel = enhancedModel;
                    selectedRmse = enhancedRmse;
                    selectedName = "enhanced_rf";
                }
                System.out.println(
                        "INFO: ML A/B delay risk => baseline_rmse=" + baselineRmse
                                + " | tuned_baseline_rmse=" + tunedBaselineRmse
                                + " | enhanced_rmse=" + enhancedRmse
                                + " | selected=" + selectedName
                                + " | selected_rmse=" + selectedRmse);
                selectedModel.write().overwrite().save(DELAY_RISK_MODEL_PATH);

                scoredLatest = selectedModel.transform(latestState)
                        .withColumn("predicted_delay_minutes", col("prediction").cast(DataTypes.DoubleType))
                        .withColumn(
                                "risk_level",
                                when(col("prediction").geq(10.0), lit("high"))
                                        .when(col("prediction").geq(5.0), lit("medium"))
                                        .otherwise(lit("low")));
            } else {
                System.err.println(
                        "WARN: dataset muy pequeno para entrenamiento ML; se usa scoring heuristico.");
                scoredLatest = latestState
                        .withColumn(
                                "predicted_delay_minutes",
                                when(col("speed_kmh").isNull(), lit(8.0))
                                        .when(col("speed_kmh").lt(30.0), lit(14.0))
                                        .when(col("speed_kmh").lt(50.0), lit(8.0))
                                        .otherwise(lit(3.0)))
                        .withColumn(
                                "risk_level",
                                when(col("predicted_delay_minutes").geq(10.0), lit("high"))
                                        .when(col("predicted_delay_minutes").geq(5.0), lit("medium"))
                                        .otherwise(lit("low")));
            }

            Dataset<Row> mlScores = scoredLatest.select(
                    col("vehicle_id"),
                    col("warehouse_id"),
                    col("event_timestamp").alias("last_event_timestamp"),
                    col("predicted_delay_minutes"),
                    col("risk_level"));

            mlScores.write()
                    .mode(SaveMode.Overwrite)
                    .saveAsTable(DATABASE + ".ml_delay_risk_scores");
        } catch (Exception mlFailure) {
            System.err.println(
                    "WARN: fallo en entrenamiento/scoring ML de delay risk. "
                            + "Se continúa con el pipeline. Causa: "
                            + mlFailure.getMessage());
        } finally {
            if (mlDataset != null) {
                mlDataset.unpersist();
            }
            if (eventsWithMlContext != null) {
                eventsWithMlContext.unpersist();
            }
        }
    }

    private static Dataset<Row> enrichEventsWithMlContext(SparkSession spark, Dataset<Row> enrichedEvents) {
        Dataset<Row> weatherFeatures = buildWindowedWeatherFeatures(spark);
        Dataset<Row> congestionFeatures = buildWindowedCongestionFeatures(spark);
        Dataset<Row> eventsWithWindow = enrichedEvents
                .withColumn("feature_window", window(col("event_timestamp"), "15 minutes"))
                .withColumn("feature_window_start", col("feature_window.start"))
                .drop("feature_window");

        return eventsWithWindow
                .join(weatherFeatures, new String[]{"warehouse_id", "feature_window_start"}, "left")
                .join(congestionFeatures, new String[]{"warehouse_id", "feature_window_start"}, "left")
                .withColumn("climate_temperature_c", coalesce(col("climate_temperature_c"), lit(18.0)))
                .withColumn("climate_precipitation_mm", coalesce(col("climate_precipitation_mm"), lit(0.0)))
                .withColumn("climate_wind_kmh", coalesce(col("climate_wind_kmh"), lit(0.0)))
                .withColumn("climate_severity_score", coalesce(col("climate_severity_score"), lit(0.0)))
                .withColumn("congestion_avg_delay_minutes", coalesce(col("congestion_avg_delay_minutes"), lit(0.0)))
                .withColumn("congestion_avg_speed_kmh", coalesce(col("congestion_avg_speed_kmh"), lit(0.0)))
                .withColumn("congestion_event_count", coalesce(col("congestion_event_count"), lit(0.0)))
                .withColumn("congestion_pressure_score", coalesce(col("congestion_pressure_score"), lit(0.0)))
                .drop("feature_window_start");
    }

    private static Dataset<Row> buildWindowedWeatherFeatures(SparkSession spark) {
        if (!spark.catalog().tableExists(WEATHER_STREAMING_TABLE)) {
            return spark.table(MASTER_WAREHOUSES_TABLE)
                    .select(col("warehouse_id"))
                    .limit(0)
                    .withColumn("feature_window_start", lit(null).cast(DataTypes.TimestampType))
                    .withColumn("climate_temperature_c", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("climate_precipitation_mm", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("climate_wind_kmh", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("climate_severity_score", lit(null).cast(DataTypes.DoubleType));
        }

        return spark.table(WEATHER_STREAMING_TABLE)
                .filter(col("warehouse_id").isNotNull())
                .filter(col("weather_timestamp").isNotNull())
                .withColumn("feature_window", window(col("weather_timestamp"), "15 minutes"))
                .groupBy(col("warehouse_id"), col("feature_window.start").alias("feature_window_start"))
                .agg(
                        avg(col("temperature_c").cast(DataTypes.DoubleType)).alias("climate_temperature_c"),
                        avg(col("precipitation_mm").cast(DataTypes.DoubleType)).alias("climate_precipitation_mm"),
                        avg(col("wind_kmh").cast(DataTypes.DoubleType)).alias("climate_wind_kmh"))
                .select(
                        col("warehouse_id"),
                        col("feature_window_start"),
                        col("climate_temperature_c"),
                        col("climate_precipitation_mm"),
                        col("climate_wind_kmh"))
                .withColumn(
                        "climate_severity_score",
                        coalesce(col("climate_precipitation_mm"), lit(0.0)).multiply(lit(3.0))
                                .plus(coalesce(col("climate_wind_kmh"), lit(0.0)).multiply(lit(0.15))));
    }

    private static Dataset<Row> buildWindowedCongestionFeatures(SparkSession spark) {
        String sourceTable = null;
        if (spark.catalog().tableExists(DELAY_STREAMING_TABLE)) {
            sourceTable = DELAY_STREAMING_TABLE;
        } else if (spark.catalog().tableExists(DELAY_BATCH_TABLE)) {
            sourceTable = DELAY_BATCH_TABLE;
        }

        if (sourceTable == null) {
            return spark.table(MASTER_WAREHOUSES_TABLE)
                    .select(col("warehouse_id"))
                    .limit(0)
                    .withColumn("feature_window_start", lit(null).cast(DataTypes.TimestampType))
                    .withColumn("congestion_avg_delay_minutes", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("congestion_avg_speed_kmh", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("congestion_event_count", lit(null).cast(DataTypes.DoubleType))
                    .withColumn("congestion_pressure_score", lit(null).cast(DataTypes.DoubleType));
        }

        return spark.table(sourceTable)
                .filter(col("warehouse_id").isNotNull())
                .select(
                        col("warehouse_id"),
                        col("window_start").alias("feature_window_start"),
                        coalesce(col("avg_delay_minutes"), lit(0.0)).cast(DataTypes.DoubleType)
                                .alias("congestion_avg_delay_minutes"),
                        coalesce(col("avg_speed_kmh"), lit(0.0)).cast(DataTypes.DoubleType)
                                .alias("congestion_avg_speed_kmh"),
                        coalesce(col("event_count"), lit(0)).cast(DataTypes.DoubleType)
                                .alias("congestion_event_count"))
                .withColumn(
                        "congestion_pressure_score",
                        coalesce(col("congestion_avg_delay_minutes"), lit(0.0)).multiply(lit(1.5))
                                .plus(when(coalesce(col("congestion_avg_speed_kmh"), lit(0.0)).gt(0.0), lit(100.0).divide(col("congestion_avg_speed_kmh")))
                                        .otherwise(lit(0.0)))
                                .plus(when(coalesce(col("congestion_event_count"), lit(0.0)).gt(0.0), lit(10.0).divide(sqrt(col("congestion_event_count"))))
                                        .otherwise(lit(0.0))));
    }

    private static Dataset<Row> cleanAndNormalizeEvents(Dataset<Row> events) {
        // Estandarizacion de eventos GPS: cast tipos, timestamp y filtros de calidad.
        return events
                .withColumn(
                        "event_timestamp",
                        coalesce(
                                to_timestamp(col("event_time"), "yyyy-MM-dd'T'HH:mm:ssX"),
                                to_timestamp(col("event_time"))))
                .withColumn("delay_minutes", col("delay_minutes").cast(DataTypes.IntegerType))
                .withColumn("speed_kmh", col("speed_kmh").cast(DataTypes.DoubleType))
                .na().fill(0, new String[]{"delay_minutes"})
                .na().fill(0.0, new String[]{"speed_kmh"})
                .filter(col("event_timestamp").isNotNull())
                .filter(col("vehicle_id").isNotNull())
                .filter(col("warehouse_id").isNotNull());
    }

    private static void ensureHiveDatabase(SparkSession spark) {
        // Asegura existencia del namespace analitico.
        spark.sql("CREATE DATABASE IF NOT EXISTS " + DATABASE);
    }

    private static void appendToHiveTable(Dataset<Row> batch, String tableName) {
        appendToHiveTable(batch, tableName, STREAMING_METRICS_FALLBACK_PATH);
    }

    private static void appendToHiveTable(Dataset<Row> batch, String tableName, String fallbackPath) {
        // Escritura robusta en Hive; fallback a parquet si hay incompatibilidad runtime/metastore.
        if (batch == null || batch.isEmpty()) {
            return;
        }
        SparkSession spark = batch.sparkSession();
        try {
            if (!spark.catalog().tableExists(tableName)) {
                batch.limit(0).write().mode(SaveMode.Overwrite).saveAsTable(tableName);
            }
            batch.write().mode(SaveMode.Append).insertInto(tableName);
        } catch (Exception hiveFailure) {
            // Hive 4 metastore can be incompatible with Spark 3.5 client APIs.
            // Keep streaming alive by persisting metrics to HDFS parquet as fallback.
            System.err.println(
                    "WARN: no se pudo escribir en Hive table " + tableName
                            + ". Se usa fallback parquet en " + fallbackPath
                            + ". Causa: " + hiveFailure.getMessage());
            batch.write()
                    .mode(SaveMode.Append)
                    .format("parquet")
                    .save(fallbackPath);
        }
    }

    private static void saveLatestVehicleState(Dataset<Row> events) {
        // Mantiene solo ultimo evento por vehiculo para consulta de baja latencia.
        WindowSpec latestByVehicle = Window.partitionBy("vehicle_id").orderBy(col("event_timestamp").desc());

        Dataset<Row> latestState = events
                .withColumn("row_num", row_number().over(latestByVehicle))
                .filter(col("row_num").equalTo(1))
                .select(
                        col("vehicle_id"),
                        col("warehouse_id"),
                        col("route_id"),
                        col("event_timestamp").alias("last_event_timestamp"),
                        col("delay_minutes"),
                        col("speed_kmh"),
                        col("latitude"),
                        col("longitude"));

        try {
            latestState.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "transport")
                    .option("table", "vehicle_latest_state")
                    .mode(SaveMode.Append)
                    .save();
        } catch (Exception cassandraFailure) {
            System.err.println(
                    "WARN: no se pudo escribir latest vehicle state en Cassandra. "
                            + "Se continúa sin bloquear Hive. Causa: "
                            + cassandraFailure.getMessage());
        }
    }

    private static void ensureCassandraSchema() {
        // Bootstrap de keyspace/tablas Cassandra requeridas por dashboard.
        String cassandraLocalDc = System.getenv().getOrDefault("CASSANDRA_LOCAL_DC", "datacenter1");
        try (CqlSession session = CqlSession.builder()
                        .addContactPoint(new InetSocketAddress("cassandra", 9042))
                        .withLocalDatacenter(cassandraLocalDc)
                        .build()) {
            session.execute("""
                        CREATE KEYSPACE IF NOT EXISTS transport
                        WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
                        """);
            session.execute("""
                        CREATE TABLE IF NOT EXISTS transport.vehicle_latest_state (
                            vehicle_id text PRIMARY KEY,
                            warehouse_id text,
                            route_id text,
                            last_event_timestamp timestamp,
                            delay_minutes int,
                            speed_kmh double,
                            latitude double,
                            longitude double
                        )
                        """);
            session.execute("""
                        CREATE TABLE IF NOT EXISTS transport.weather_observations_recent (
                            bucket text,
                            weather_timestamp timestamp,
                            weather_event_id text,
                            warehouse_id text,
                            temperature_c double,
                            precipitation_mm double,
                            wind_kmh double,
                            weather_code text,
                            source text,
                            PRIMARY KEY (bucket, weather_timestamp, weather_event_id)
                        ) WITH CLUSTERING ORDER BY (weather_timestamp DESC, weather_event_id DESC)
                        """);
            session.execute("""
                        CREATE TABLE IF NOT EXISTS transport.network_insights_snapshots (
                            bucket text,
                            entity_type text,
                            profile text,
                            min_congestion text,
                            snapshot_time timestamp,
                            rank int,
                            entity_id text,
                            impact_score double,
                            criticality_score double,
                            effective_avg_delay_minutes double,
                            total_minutes double,
                            congestion_level text,
                            live_sample_count int,
                            PRIMARY KEY ((bucket, entity_type, profile, min_congestion), snapshot_time, rank, entity_id)
                        ) WITH CLUSTERING ORDER BY (snapshot_time DESC, rank ASC, entity_id ASC)
                        """);
        } catch (Exception cassandraFailure) {
            System.err.println(
                    "WARN: Cassandra no disponible para inicializar schema. "
                            + "Se continúa para no bloquear escritura en Hive. Causa: "
                            + cassandraFailure.getMessage());
        }
    }

    private static StructType rawSchema() {
        return new StructType()
                .add("event_id", DataTypes.StringType)
                .add("vehicle_id", DataTypes.StringType)
                .add("warehouse_id", DataTypes.StringType)
                .add("route_id", DataTypes.StringType)
                .add("event_type", DataTypes.StringType)
                .add("latitude", DataTypes.DoubleType)
                .add("longitude", DataTypes.DoubleType)
                .add("delay_minutes", DataTypes.IntegerType)
                .add("speed_kmh", DataTypes.DoubleType)
                .add("event_time", DataTypes.StringType);
    }

    private static Dataset<Row> cleanAndNormalizeWeather(Dataset<Row> weatherEvents) {
        // Estandarizacion de clima con cast tolerante (numeric/string numeric).
        return weatherEvents
                .withColumn(
                        "weather_timestamp",
                        coalesce(
                                to_timestamp(col("observation_time"), "yyyy-MM-dd'T'HH:mm:ssX"),
                                to_timestamp(col("observation_time"))))
                .withColumn("temperature_c", col("temperature_c").cast(DataTypes.DoubleType))
                .withColumn("precipitation_mm", col("precipitation_mm").cast(DataTypes.DoubleType))
                .withColumn("wind_kmh", col("wind_kmh").cast(DataTypes.DoubleType))
                .na().fill(0.0, new String[]{"temperature_c", "precipitation_mm", "wind_kmh"})
                .filter(col("weather_timestamp").isNotNull())
                .filter(col("weather_event_id").isNotNull())
                .filter(col("warehouse_id").isNotNull())
                .select(
                        col("weather_event_id"),
                        col("warehouse_id"),
                        col("temperature_c"),
                        col("precipitation_mm"),
                        col("wind_kmh"),
                        col("weather_code"),
                        col("source"),
                        col("weather_timestamp"));
    }

    private static void saveWeatherObservationsToCassandra(Dataset<Row> weatherBatch) {
        // Persiste observaciones meteo recientes usadas por el dashboard.
        if (weatherBatch == null || weatherBatch.isEmpty()) {
            return;
        }
        Dataset<Row> observations = weatherBatch
                .withColumn("bucket", lit("all"))
                .select(
                        col("bucket"),
                        col("weather_timestamp"),
                        col("weather_event_id"),
                        col("warehouse_id"),
                        col("temperature_c"),
                        col("precipitation_mm"),
                        col("wind_kmh"),
                        col("weather_code"),
                        col("source"));

        try {
            observations.write()
                    .format("org.apache.spark.sql.cassandra")
                    .option("keyspace", "transport")
                    .option("table", "weather_observations_recent")
                    .mode(SaveMode.Append)
                    .save();
        } catch (Exception cassandraFailure) {
            System.err.println(
                    "WARN: no se pudo escribir weather observations en Cassandra. "
                            + "Se continúa sin bloquear Hive. Causa: "
                            + cassandraFailure.getMessage());
        }
    }

    private static StructType weatherSchema() {
        return new StructType()
                .add("weather_event_id", DataTypes.StringType)
                .add("warehouse_id", DataTypes.StringType)
                .add("temperature_c", DataTypes.DoubleType)
                .add("precipitation_mm", DataTypes.DoubleType)
                .add("wind_kmh", DataTypes.DoubleType)
                .add("weather_code", DataTypes.StringType)
                .add("source", DataTypes.StringType)
                .add("observation_time", DataTypes.StringType);
    }

}
