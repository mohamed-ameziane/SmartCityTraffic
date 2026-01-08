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
    private final MeterRegistry meterRegistry;

    // Map to hold persistent Gauge references
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicInteger> speedGauges = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicInteger> occupancyGauges = new java.util.concurrent.ConcurrentHashMap<>();
    private final java.util.Map<String, java.util.concurrent.atomic.AtomicInteger> vehicleGauges = new java.util.concurrent.ConcurrentHashMap<>();

    private static final String[] SENSORS = { "S-001", "S-002", "S-003", "S-004", "S-005" };
    private static final String[] ROADS = { "R-101", "R-102", "R-103", "R-104" };
    private static final String[] ZONES = { "Centre-Ville", "Banlieue-Nord", "Zone-Industrielle" };
    private static final String[] ROAD_TYPES = { "Autoroute", "Avenue", "Rue" };

    public TrafficGeneratorService(MeterRegistry meterRegistry) {
        this.meterRegistry = meterRegistry;
    }

    public TrafficEvent generateEvent() {
        String sensorId = SENSORS[random.nextInt(SENSORS.length)];
        String zone = ZONES[random.nextInt(ZONES.length)];
        String roadId = ROADS[random.nextInt(ROADS.length)];
        String roadType = ROAD_TYPES[random.nextInt(ROAD_TYPES.length)];

        int vehicleCount = random.nextInt(120);
        int avgSpeedInt;
        int occupancyRate;

        // Logique de simulation
        if (vehicleCount > 80) { // Congestion
            avgSpeedInt = 5 + random.nextInt(35);
            occupancyRate = 70 + random.nextInt(30);
        } else { // Fluide
            avgSpeedInt = 50 + random.nextInt(70);
            occupancyRate = random.nextInt(50);
        }

        // --- ENVOI VERS PROMETHEUS / GRAFANA ---
        // Clé unique pour identifier la série temporelle
        String gaugeKey = sensorId + "-" + roadId + "-" + zone;
        Tags tags = Tags.of("roadId", roadId, "zone", zone, "roadType", roadType);

        // Update Gauges safely
        speedGauges.computeIfAbsent(gaugeKey, k -> {
            java.util.concurrent.atomic.AtomicInteger gauge = new java.util.concurrent.atomic.AtomicInteger(0);
            meterRegistry.gauge("traffic_speed", tags, gauge);
            return gauge;
        }).set(avgSpeedInt);

        occupancyGauges.computeIfAbsent(gaugeKey, k -> {
            java.util.concurrent.atomic.AtomicInteger gauge = new java.util.concurrent.atomic.AtomicInteger(0);
            meterRegistry.gauge("traffic_occupancy", tags, gauge);
            return gauge;
        }).set(occupancyRate);

        vehicleGauges.computeIfAbsent(gaugeKey, k -> {
            java.util.concurrent.atomic.AtomicInteger gauge = new java.util.concurrent.atomic.AtomicInteger(0);
            meterRegistry.gauge("traffic_vehicles", tags, gauge);
            return gauge;
        }).set(vehicleCount);
        // ----------------------------------------

        return new TrafficEvent(
                sensorId, roadId, roadType, zone,
                vehicleCount, (double) avgSpeedInt, (double) occupancyRate,
                LocalDateTime.now().format(DateTimeFormatter.ISO_DATE_TIME));
    }
}
