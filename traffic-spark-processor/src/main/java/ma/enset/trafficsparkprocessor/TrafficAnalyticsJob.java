package ma.enset.trafficsparkprocessor;

import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import static org.apache.spark.sql.functions.*;

public class TrafficAnalyticsJob {
    public static void main(String[] args) {

        System.out.println("Démarrage de l'analyse Spark...");

        // --- HACK WINDOWS OBLIGATOIRE ---
        // Permet d'éviter l'erreur "HADOOP_HOME is not set" sur Windows
        System.setProperty("hadoop.home.dir", "/");
        // --------------------------------

        // 1. Initialisation de la Session Spark
        SparkSession spark = SparkSession.builder()
                .appName("SmartCityTrafficAnalytics")
                .master("local[*]") // Utilise tous les cœurs du CPU
                .config("spark.hadoop.dfs.client.use.datanode.hostname", "true")
                .getOrCreate();

        // Réduire le bruit dans les logs (afficher seulement les erreurs et warnings)
        spark.sparkContext().setLogLevel("WARN");

        // 2. Lecture des données (Raw Zone)
        // On lit les fichiers JSON générés par le Consumer
        String inputPath = "hdfs://localhost:9000/data/raw/traffic/*.json";

        try {
            Dataset<Row> rawData = spark.read().json(inputPath);

            System.out.println("=== 1. Aperçu des données brutes ===");
            rawData.printSchema();
            rawData.show(5);

            // 3. Traitements (Business Logic) [cite: 86-89]

            // KPI A : Vitesse moyenne par Route
            Dataset<Row> avgSpeedRoad = rawData.groupBy("roadId")
                    .agg(
                            avg("averageSpeed").alias("avg_speed"),
                            count("roadId").alias("event_count")
                    );

            // KPI B : Trafic (Volume) par Zone
            Dataset<Row> trafficZone = rawData.groupBy("zone")
                    .agg(
                            sum("vehicleCount").alias("total_vehicles"),
                            avg("occupancyRate").alias("avg_occupancy")
                    );

            // KPI C : Détection de Congestion
            // Règle : Vitesse < 30 km/h ET Occupation > 50%
            Dataset<Row> congestions = rawData.filter(
                    col("averageSpeed").lt(30).and(col("occupancyRate").gt(50))
            );

            // 4. Affichage Console
            System.out.println("=== 2. Vitesse Moyenne par Route ===");
            avgSpeedRoad.show();

            System.out.println("=== 3. Zones Congestionnées ===");
            congestions.select("eventTime", "roadId", "zone", "averageSpeed", "occupancyRate").show();

            // 5. Sauvegarde (Analytics Zone) [cite: 96-98]
            // On écrit le résultat aggrégé pour le Dashboard
            String outputPath = "hdfs://localhost:9000/data/analytics/traffic_zone_stats";

            trafficZone.write()
                    .mode(SaveMode.Overwrite)
                    .parquet(outputPath);

            System.out.println("Succès ! Résultats sauvegardés dans : " + outputPath);

        } catch (Exception e) {
            System.err.println("Erreur lors du traitement Spark : " + e.getMessage());
            System.err.println("Vérifiez que Kafka a bien envoyé des données et que le Consumer les a écrites dans HDFS.");
        }

        // Arrêt propre
        spark.stop();
    }
}