package org.example;

import com.sun.management.OperatingSystemMXBean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;

public class SystemUsageMonitor {
    private static final Logger logger = LoggerFactory.getLogger(SystemUsageMonitor.class);
    private final OperatingSystemMXBean osBean;

    public SystemUsageMonitor() {
        this.osBean = ManagementFactory.getPlatformMXBean(OperatingSystemMXBean.class);
    }

    public void logSystemUsage() {
        logger.info("CPU Load: {}%", osBean.getCpuLoad() * 100);
        logger.info("Available Memory (MB): {}", osBean.getFreeMemorySize() / (1024 * 1024));
    }
}
