package org.xbib.elasticsearch.plugin.jdbc.util;


import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

public final class MemoryManagementUtil {
    private final static ESLogger logger = ESLoggerFactory.getLogger("MemoryManagementUtil");
    private final static int MB = 1024*1024;
    private final static String NEWLINE = System.getProperty("line.separator");

    private MemoryManagementUtil() {}

    public static void logMemoryStatistics() {
        Runtime runtime = Runtime.getRuntime();

        logger.debug(NEWLINE + "## Heap memory statistics [MB] ##" + NEWLINE +
                "Used Memory:" + (runtime.totalMemory() - runtime.freeMemory()) / MB + NEWLINE +
                "Free Memory:" + runtime.freeMemory() / MB + NEWLINE +
                "Total Memory:" + runtime.totalMemory() / MB + NEWLINE +
                "Max Memory:" + runtime.maxMemory() / MB + NEWLINE);
    }

    public static void freeMemoryCheckup() {
        System.gc();
        logger.debug("Current free memory: {} MB", Runtime.getRuntime().freeMemory() / MB);
    }

}
