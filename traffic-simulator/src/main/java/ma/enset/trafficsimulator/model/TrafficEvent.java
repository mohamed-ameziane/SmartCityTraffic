package ma.enset.trafficsimulator.model;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class TrafficEvent {
    private String sensorId;
    private String roadId;
    private String roadType;
    private String zone;
    private int vehicleCount;
    private double averageSpeed;
    private double occupancyRate;
    private String eventTime;
}