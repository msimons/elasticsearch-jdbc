package org.xbib.elasticsearch.plugin.jdbc.util;


import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;

public final class MemoryManagementUtil {
    private final static ESLogger logger = ESLoggerFactory.getLogger("MemoryManagementUtil");
    private final static int MB = 1024*1024;
    private final static String NEWLINE = System.getProperty("line.separator");

    private MemoryManagementUtil() {};

    public static void logMemoryStatistics() {
        Runtime runtime = Runtime.getRuntime();

        StringBuilder sb = new StringBuilder(NEWLINE);
        sb.append("## Heap memory statistics [MB] ##").append(NEWLINE);
        sb.append("Used Memory:").append((runtime.totalMemory() - runtime.freeMemory()) / MB).append(NEWLINE);
        sb.append("Free Memory:").append(runtime.freeMemory() / MB).append(NEWLINE);
        sb.append("Total Memory:").append(runtime.totalMemory() / MB).append(NEWLINE);
        sb.append("Max Memory:").append(runtime.maxMemory() / MB).append(NEWLINE);

        logger.debug(sb.toString());
    }

    public static void freeMemoryCheckup() {
//        try {
//            if (Runtime.getRuntime().freeMemory() / MB < 100) {
//                logger.info("Free memory too low. Waiting for free memory... (100 MB expected)");
//                while (Runtime.getRuntime().freeMemory() / MB < 100) {
//                    Thread.sleep(100);
//                    System.gc();
//                    logger.debug("Current free memory: {} MB", Runtime.getRuntime().freeMemory() / MB);
//                }
//                logger.debug("Free memory size back to minimal 100 MB.");
//            }
//        } catch (InterruptedException e) {
//            logger.error("Interrupted sleep");
//        }
        System.gc();
        logger.debug("Current free memory: {} MB", Runtime.getRuntime().freeMemory() / MB);
    }

}
