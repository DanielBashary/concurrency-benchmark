import org.daniel.metrics.MetricsCollector;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class MetricsCollectorTest {

    private MetricsCollector metricsCollector;

    @BeforeEach
    void setUp() {
        metricsCollector = new MetricsCollector();
    }

    @Test
    void testRecordWriteLatency() {
        metricsCollector.recordWriteLatency(500);
        assertEquals(1, metricsCollector.getWriteOperations());
        assertEquals(500, metricsCollector.getTotalWriteLatency());
    }

    @Test
    void testRecordReadLatency() {
        metricsCollector.recordReadLatency(300);
        assertEquals(1, metricsCollector.getReadOperations());
        assertEquals(300, metricsCollector.getTotalReadLatency());
    }

    @Test
    void testIncrementWriteErrors() {
        metricsCollector.incrementWriteErrors();
        assertEquals(1, metricsCollector.getWriteErrors());
    }

    @Test
    void testIncrementReadErrors() {
        metricsCollector.incrementReadErrors();
        assertEquals(1, metricsCollector.getReadErrors());
    }

    @Test
    void testGetAverageWriteLatency() {
        metricsCollector.recordWriteLatency(500);
        metricsCollector.recordWriteLatency(1000);
        assertEquals(2, metricsCollector.getWriteOperations());
        assertEquals(750, metricsCollector.getAverageWriteLatency());
    }

    @Test
    void testGetAverageReadLatency() {
        metricsCollector.recordReadLatency(300);
        metricsCollector.recordReadLatency(600);
        assertEquals(2, metricsCollector.getReadOperations());
        assertEquals(450, metricsCollector.getAverageReadLatency());
    }
}
