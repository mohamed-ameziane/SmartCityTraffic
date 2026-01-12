package ma.enset.trafficsparkprocessor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TrafficAnalyticsJob {
        public static void main(String[] args) {

                System.out.println("Demarrage de l'analyse Spark...");

                System.setProperty("hadoop.home.dir", "/");

                SparkSession spark = SparkSession.builder()
                                .appName("SmartCityTrafficAnalytics")
                                .master("local[*]")
                                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                                .getOrCreate();

                spark.sparkContext().setLogLevel("WARN");

                String inputPath = "hdfs://localhost:9000/data/raw/traffic/*.json";

                try {
                        Dataset<Row> rawData = spark.read().json(inputPath);

                        System.out.println("=== 1. Apercu des donnees brutes ===");
                        rawData.printSchema();
                        rawData.show(5);

                        Dataset<Row> avgSpeedRoad = rawData.groupBy("roadId")
                                        .agg(
                                                        avg("averageSpeed").alias("avg_speed"),
                                                        count("roadId").alias("event_count"));

                        Dataset<Row> trafficZone = rawData.groupBy("zone")
                                        .agg(
                                                        sum("vehicleCount").alias("total_vehicles"),
                                                        avg("occupancyRate").alias("avg_occupancy"));

                        Dataset<Row> congestions = rawData.filter(
                                        col("averageSpeed").lt(30).and(col("occupancyRate").gt(50)));

                        System.out.println("=== 2. Vitesse Moyenne par Route ===");
                        avgSpeedRoad.show();

                        System.out.println("=== 3. Zones Congestionnees ===");
                        congestions.select("eventTime", "roadId", "zone", "averageSpeed", "occupancyRate").show();

                        String outputPath = "hdfs://localhost:9000/data/analytics/traffic_zone_stats";

                        trafficZone.write()
                                        .mode(SaveMode.Overwrite)
                                        .parquet(outputPath);

                        System.out.println("Succes ! Resultats sauvegardes dans : " + outputPath);

                } catch (Exception e) {
                        System.err.println("Erreur lors du traitement Spark : " + e.getMessage());
                        System.err.println(
                                        "Verifiez que Kafka a bien envoye des donnees et que le Consumer les a ecrites dans HDFS.");
                }

                spark.stop();
        }
}