package ma.enset.trafficsimulator.service;

import ma.enset.trafficsimulator.model.TrafficEvent;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Tags;
import org.springframework.stereotype.Service;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Random;

@Service
public class TrafficGeneratorService {

    private final Random random = new Random();
    private final MeterRegistry meterRegistry; // Outil pour Prometheus

    private static final String[] SENSORS = {"S-001", "S-002", "S-003", "S-004", "S-005"};
    private static final String[] ROADS = {"R-101", "R-102", "R-103", "R-104"};
    private static final String[] ZONES = {"Centre-Ville", "Banlieue-Nord", "Zone-Industrielle"};
    private static final String[] ROAD_TYPES = {"Autoroute", "Avenue", "Rue"};

    // Injection du MeterRegistry
    public TrafficGeneratorService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public TrafficEvent generateEvent() {
        String sensorId = SENSORS[random.nextInt(SENSORS.length)];
        String zone = ZONES[random.nextInt(ZONES.length)];
        String roadId = ROADS[random.nextInt(ROADS.length)];
        String roadType = ROAD_TYPES[random.nextInt(ROAD_TYPES.length)];

        int vehicleCount = random.nextInt(120);
        double avgSpeed;
        double occupancyRate;

        // Logique de simulation
        if (vehicleCount > 80) { // Congestion
            avgSpeed = 5 + random.nextInt(35);
            occupancyRate = 70 + random.nextInt(30);
        } else { // Fluide
            avgSpeed = 50 + random.nextInt(70);
            occupancyRate = random.nextInt(50);
        }

        // --- ENVOI VERS PROMETHEUS / GRAFANA ---
        // On crée des métriques avec des tags (pour filtrer par route/zone dans Grafana)
        Tags tags = Tags.of("roadId", roadId, "zone", zone, "roadType", roadType);

        // On utilise 'gauge' pour une valeur qui monte et descend
        meterRegistry.gauge("traffic_speed", tags, avgSpeed);
        meterRegistry.gauge("traffic_occupancy", tags, occupancyRate);
        meterRegistry.gauge("traffic_vehicles", tags, vehicleCount);
        // ----------------------------------------

        return new TrafficEvent(
                sensorId, roadId, roadType, zone,
                vehicleCount, avgSpeed, occupancyRate,
                LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME)
        );
    }
}