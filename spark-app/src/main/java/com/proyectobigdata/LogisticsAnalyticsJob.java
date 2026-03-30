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
import org.apache.spark.sql.streaming.StreamingQuery;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;

import static org.apache.spark.sql.functions.avg;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.count;
import static org.apache.spark.sql.functions.from_json;
import static org.apache.spark.sql.functions.get_json_object;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.map_entries;
import static org.apache.spark.sql.functions.max;
import static org.apache.spark.sql.functions.row_number;
import static org.apache.spark.sql.functions.to_timestamp;
import static org.apache.spark.sql.functions.window;
import static org.apache.spark.sql.functions.when;
import static org.apache.spark.sql.functions.explode;

/**
 * Job principal del pipeline KDD logistico.
 *
 * <p>Modos de ejecucion:
 * <ul>
 *   <li>batch: procesa historico, genera metricas, grafo, shortest paths y scoring ML.</li>
 *   <li>streaming: consume GPS/Weather desde Kafka y persiste resultados en Hive/Cassandra.</li>
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
    private static final String DELAY_RISK_MODEL_PATH = "hdfs://hadoop:9000/models/delay_risk_rf";

    private LogisticsAnalyticsJob() {
    }

    public static void main(String[] args) throws Exception {
        // Inicializacion comun de Spark + Hive + Cassandra, y dispatch por modo.
        String mode = args.length == 0 ? "batch" : args[0].toLowerCase(Locale.ROOT);
        String sqlTimeZone = System.getenv().getOrDefault("SPARK_SQL_TIMEZONE", "Europe/Madrid");

        SparkSession spark = SparkSession.builder()
                .appName("LogisticsKddJob")
                .master(System.getenv().getOrDefault("SPARK_MASTER", "yarn"))
                .config("spark.sql.session.timeZone", sqlTimeZone)
                .config("spark.sql.shuffle.partitions", "4")
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
        Dataset<Row> enrichedEvents = enrichEvents(spark, cleanedEvents);

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
    }

    private static void runStreamingAnalysis(SparkSession spark) throws Exception {
        // Pipeline continuo de GPS y clima para monitorizacion operativa.
        String startingOffsets = System.getenv().getOrDefault("STREAMING_STARTING_OFFSETS", "latest");

        Dataset<Row> kafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "transport.filtered")
                .option("startingOffsets", startingOffsets)
                .load();
        Dataset<Row> weatherKafkaStream = spark.readStream()
                .format("kafka")
                .option("kafka.bootstrap.servers", "kafka:9092")
                .option("subscribe", "transport.weather.filtered")
                .option("startingOffsets", startingOffsets)
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
        VoidFunction2<Dataset<Row>, Long> stateBatchWriter = (batch, batchId) -> saveLatestVehicleState(batch);
        VoidFunction2<Dataset<Row>, Long> weatherBatchWriter =
                (batch, batchId) -> {
                    appendToHiveTable(
                            batch,
                            DATABASE + ".weather_observations_streaming",
                            STREAMING_WEATHER_FALLBACK_PATH);
                    saveWeatherObservationsToCassandra(batch);
                };

        StreamingQuery metricsQuery = windowedMetrics.writeStream()
                .outputMode("update")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/delay_metrics")
                .foreachBatch(metricsBatchWriter)
                .start();

        StreamingQuery stateQuery = enrichedStream.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/latest_vehicle_state")
                .foreachBatch(stateBatchWriter)
                .start();
        StreamingQuery weatherQuery = weatherStream.writeStream()
                .outputMode("append")
                .option("checkpointLocation", "hdfs://hadoop:9000/tmp/checkpoints/weather_observations")
                .foreachBatch(weatherBatchWriter)
                .start();

        spark.streams().awaitAnyTermination();
    }

    private static Dataset<Row> enrichEvents(SparkSession spark, Dataset<Row> cleanedEvents) {
        // Join con maestros de almacenes y vehiculos para contexto analitico.
        Dataset<Row> warehouses = spark.table(MASTER_WAREHOUSES_TABLE);
        Dataset<Row> vehicles = spark.table(MASTER_VEHICLES_TABLE);

        return cleanedEvents
                .join(warehouses, "warehouse_id", "left")
                .join(vehicles, "vehicle_id", "left");
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
        try {
            Dataset<Row> mlDataset = enrichedEvents
                    .filter(col("delay_minutes").isNotNull())
                    .filter(col("speed_kmh").isNotNull())
                    .filter(col("warehouse_id").isNotNull())
                    .filter(col("vehicle_type").isNotNull())
                    .withColumn("label", col("delay_minutes").cast(DataTypes.DoubleType));

            long mlCount = mlDataset.count();

            StringIndexer warehouseIndexer = new StringIndexer()
                    .setInputCol("warehouse_id")
                    .setOutputCol("warehouse_idx")
                    .setHandleInvalid("keep");
            StringIndexer vehicleTypeIndexer = new StringIndexer()
                    .setInputCol("vehicle_type")
                    .setOutputCol("vehicle_type_idx")
                    .setHandleInvalid("keep");
            OneHotEncoder encoder = new OneHotEncoder()
                    .setInputCols(new String[]{"warehouse_idx", "vehicle_type_idx"})
                    .setOutputCols(new String[]{"warehouse_ohe", "vehicle_type_ohe"});
            VectorAssembler assembler = new VectorAssembler()
                    .setInputCols(new String[]{"speed_kmh", "warehouse_ohe", "vehicle_type_ohe"})
                    .setOutputCol("features");
            RandomForestRegressor rf = new RandomForestRegressor()
                    .setFeaturesCol("features")
                    .setLabelCol("label")
                    .setPredictionCol("prediction")
                    .setNumTrees(30)
                    .setMaxDepth(8);

            WindowSpec latestByVehicle = Window.partitionBy("vehicle_id").orderBy(col("event_timestamp").desc());
            Dataset<Row> latestState = enrichedEvents
                    .withColumn("row_num", row_number().over(latestByVehicle))
                    .filter(col("row_num").equalTo(1))
                    .select(
                            col("vehicle_id"),
                            col("warehouse_id"),
                            col("vehicle_type"),
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

                Pipeline pipeline = new Pipeline().setStages(new PipelineStage[]{
                        warehouseIndexer, vehicleTypeIndexer, encoder, assembler, rf});
                PipelineModel model = pipeline.fit(train);

                Dataset<Row> scoredTest = model.transform(test);
                RegressionEvaluator evaluator = new RegressionEvaluator()
                        .setLabelCol("label")
                        .setPredictionCol("prediction")
                        .setMetricName("rmse");
                double rmse = evaluator.evaluate(scoredTest);
                System.out.println("INFO: Modelo ML delay risk entrenado. RMSE test = " + rmse);
                model.write().overwrite().save(DELAY_RISK_MODEL_PATH);

                scoredLatest = model.transform(latestState)
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
        }
    }

    private static Dataset<Row> cleanAndNormalizeEvents(Dataset<Row> events) {
        // Estandarizacion de eventos GPS: cast tipos, timestamp y filtros de calidad.
        return events
                .withColumn("event_timestamp", to_timestamp(col("event_time")))
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
                .withColumn("weather_timestamp", to_timestamp(col("observation_time")))
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
