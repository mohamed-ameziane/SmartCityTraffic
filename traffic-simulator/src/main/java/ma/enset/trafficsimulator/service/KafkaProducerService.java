package ma.enset.trafficsimulator.service;

import ma.enset.trafficsimulator.model.TrafficEvent;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
public class KafkaProducerService {

    private final KafkaTemplate<String, TrafficEvent> kafkaTemplate;
    private final TrafficGeneratorService generatorService;

    @Value("${traffic.kafka.topic}")
    private String topicName;

    public KafkaProducerService(KafkaTemplate<String, TrafficEvent> kafkaTemplate,
                                TrafficGeneratorService generatorService) {
        this.kafkaTemplate = kafkaTemplate;
        this.generatorService = generatorService;
    }

    // Exécution toutes les 2 secondes (2000 ms)
    @Scheduled(fixedRate = 2000)
    public void sendTrafficEvent() {
        TrafficEvent event = generatorService.generateEvent();

        // Envoi vers Kafka (Clé = SensorID pour garantir l'ordre par capteur)
        kafkaTemplate.send(topicName, event.getSensorId(), event);

        System.out.println("Message sent to Kafka: " + event);
    }
}