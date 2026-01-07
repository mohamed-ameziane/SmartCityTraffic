package ma.enset.trafficconsumerhdfs.service;

import com.fasterxml.jackson.databind.ObjectMapper;
import ma.enset.trafficconsumerhdfs.model.TrafficEvent;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

import java.io.BufferedWriter;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Service
public class HdfsService {

    @Value("${hdfs.uri}")
    private String hdfsUri;

    @Value("${hdfs.path}")
    private String hdfsPath;

    private final List<TrafficEvent> buffer = new ArrayList<>();
    private final ObjectMapper objectMapper = new ObjectMapper();

    @KafkaListener(topics = "${traffic.kafka.topic}", groupId = "hdfs-group")
    public void consume(TrafficEvent event) {
        System.out.println("Received: " + event.getSensorId());

        buffer.add(event);

        // Batch : On écrit dans HDFS tous les 10 messages reçus
        if (buffer.size() >= 10) {
            writeBufferToHdfs();
            buffer.clear();
        }
    }

    private void writeBufferToHdfs() {
        try {
            // --- MODIFICATION 1 : Le Hack Windows ---
            // Empêche Hadoop de planter s'il ne trouve pas winutils.exe
            System.setProperty("hadoop.home.dir", "/");
            // ---------------------------------------

            // Configuration Hadoop
            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUri);

            // --- MODIFICATION 2 : Compatibilité Docker ---
            // Aide le client à résoudre les noms des conteneurs (namenode/datanode)
            conf.set("dfs.client.use.datanode.hostname", "true");
            conf.set("dfs.replication", "1"); // Force la réplication à 1 pour la démo
            // ---------------------------------------------

            // Gestion de l'utilisateur HDFS (pour éviter les erreurs de permission "Permission denied")
            System.setProperty("HADOOP_USER_NAME", "root");

            // On utilise URI pour forcer l'utilisation du système distribué
            FileSystem fs = FileSystem.get(new URI(hdfsUri), conf);

            // Nom du fichier unique : traffic_DATE_UUID.json
            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = "traffic_" + timestamp + "_" + UUID.randomUUID() + ".json";
            Path path = new Path(hdfsPath + fileName);

            // Écriture du fichier
            try (FSDataOutputStream outputStream = fs.create(path);
                 BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {

                // On écrit la liste entière en JSON
                String jsonContent = objectMapper.writeValueAsString(buffer);
                writer.write(jsonContent);
            }

            System.out.println("Saved batch to HDFS: " + path);
            fs.close();

        } catch (Exception e) {
            System.err.println("Erreur HDFS : " + e.getMessage());
            e.printStackTrace();
        }
    }
}