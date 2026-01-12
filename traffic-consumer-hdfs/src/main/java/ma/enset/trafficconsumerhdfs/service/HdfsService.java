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

        if (buffer.size() >= 10) {
            writeBufferToHdfs();
            buffer.clear();
        }
    }

    private void writeBufferToHdfs() {
        try {
            System.setProperty("hadoop.home.dir", "/");

            Configuration conf = new Configuration();
            conf.set("fs.defaultFS", hdfsUri);

            conf.set("dfs.client.use.datanode.hostname", "true");
            conf.set("dfs.replication", "1");

            System.setProperty("HADOOP_USER_NAME", "root");

            FileSystem fs = FileSystem.get(new URI(hdfsUri), conf);

            String timestamp = LocalDateTime.now().format(DateTimeFormatter.ofPattern("yyyyMMdd_HHmmss"));
            String fileName = "traffic_" + timestamp + "_" + UUID.randomUUID() + ".json";
            Path path = new Path(hdfsPath + fileName);

            try (FSDataOutputStream outputStream = fs.create(path);
                    BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(outputStream))) {

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