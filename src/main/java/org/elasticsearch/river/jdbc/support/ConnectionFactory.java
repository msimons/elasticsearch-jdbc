package org.elasticsearch.river.jdbc.support;

import org.apache.commons.dbcp.BasicDataSource;
import org.apache.commons.dbcp.DriverManagerConnectionFactory;
import org.apache.commons.dbcp.PoolableConnectionFactory;
import org.apache.commons.dbcp.PoolingDataSource;
import org.apache.commons.pool.impl.GenericObjectPool;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

/**
 * This connection pool ensures a maximum of prepared connections and will cleanup connections while rivers are not properly closed.
 * @author marcosimons
 */
public class ConnectionFactory {

    private final ESLogger logger = ESLoggerFactory.getLogger(ConnectionFactory.class.getName());
    private static ConnectionFactory instance = null;
    private static final ESLogger LOG = ESLoggerFactory.getLogger(ConnectionFactory.class.getName());
    private static final Long CONNECTION_TIME_BETWEEN_EVICTION_RUNS = 60000L;
    private static final Long CONNECTION_NUM_TESTS_PER_EVICTION_RUN = 25L;
    private static final Long CONNECTION_MIN_EVICTABLE_IDLE_TIME = 300000L;
    private static final String CONNECTION_VALIDATION_QUERY = "SELECT COUNT(1) FROM DUAL";
    private static final Long CONNECTION_MAX_IDLE = 2L;
    private static final int MAX_CONNECTIONS = 15;
    private static final String ORACLE_DRIVER = "oracle.jdbc.driver.OracleDriver";

    private static Map<JDBCSettings,DataSource> dataSources = new HashMap<JDBCSettings, DataSource>();

    private DataSource internalDataSource(JDBCSettings dataSourceInfo) {
        if(!dataSources.containsKey(dataSourceInfo)){
            return null;
        }

        return dataSources.get(dataSourceInfo);
    }

    private synchronized DataSource createDataSource(JDBCSettings jdbcSettings) {
        DataSource dataSource = internalDataSource(jdbcSettings);

        if(dataSource == null) {
            /* Check if the JBDC driver is available */
            try {
                Class.forName(jdbcSettings.driver);
            } catch (ClassNotFoundException cnfe) {
                LOG.error("Cannot load the driver class {}",jdbcSettings.driver);
                throw new RuntimeException("Cannot find a required class:"+ cnfe.getMessage(), cnfe);
            }

            /* First, we'll need a ObjectPool that serves as the actual pool of connections.
             We'll use a GenericObjectPool instance, although any ObjectPool implementation will suffice. */

            GenericObjectPool connectionPool = new GenericObjectPool(null, MAX_CONNECTIONS);

            connectionPool.setTestOnBorrow(true);
            connectionPool.setTestOnReturn(true);
            connectionPool.setTestWhileIdle(true);
            connectionPool.setTimeBetweenEvictionRunsMillis(CONNECTION_TIME_BETWEEN_EVICTION_RUNS);
            connectionPool.setNumTestsPerEvictionRun(CONNECTION_NUM_TESTS_PER_EVICTION_RUN.intValue());
            connectionPool.setMinEvictableIdleTimeMillis(CONNECTION_MIN_EVICTABLE_IDLE_TIME);
            connectionPool.setMaxIdle(CONNECTION_MAX_IDLE.intValue());

            /* Next, we'll create a ConnectionFactory that the pool will use to create Connections.
             We'll use the DriverManagerConnectionFactory, using the connect string passed in the command line arguments. */

            Properties connectionProperties = new Properties();
            connectionProperties.put("user", jdbcSettings.user);
            connectionProperties.put("password", jdbcSettings.password);

            if(ORACLE_DRIVER.equals(jdbcSettings.driver))  {
                logger.debug("detected oracle driver: adding pooling parameters to connection properties set.");
                connectionProperties.put("validationQuery", CONNECTION_VALIDATION_QUERY);
                connectionProperties.put("testOnBorrow", true);
                connectionProperties.put("testWhileIdle", true);
                connectionProperties.put("testOnReturn", true);
                connectionProperties.put("timeBetweenEvictionRunsMillis", CONNECTION_TIME_BETWEEN_EVICTION_RUNS);
                connectionProperties.put("numTestsPerEvictionRun", CONNECTION_NUM_TESTS_PER_EVICTION_RUN);
                connectionProperties.put("minEvictableIdleTimeMillis", CONNECTION_MIN_EVICTABLE_IDLE_TIME);
            }

            org.apache.commons.dbcp.ConnectionFactory connectionFactory = new DriverManagerConnectionFactory(jdbcSettings.url, connectionProperties);

            /* Now we'll create the "real" Connections for the ConnectionFactory with the classes that implement the pooling functionality. */
            new PoolableConnectionFactory(connectionFactory, connectionPool, null,ORACLE_DRIVER.equals(jdbcSettings.driver) ? CONNECTION_VALIDATION_QUERY : null, false, true);

            dataSource = new PoolingDataSource(connectionPool);
            dataSources.put(jdbcSettings,dataSource);
        }


        return dataSource;
    }


    public Connection getConnection(JDBCSettings jdbcSettings) throws SQLException {
        return getDataSource(jdbcSettings).getConnection();
    }

    public DataSource getDataSource(JDBCSettings jdbcSettings) {
        DataSource dataSource = internalDataSource(jdbcSettings);
        if(dataSource == null) {
            dataSource = createDataSource(jdbcSettings);
        }

        return dataSource;
    }

    public static ConnectionFactory getInstance() {
        if(instance == null){
            instance = new ConnectionFactory();
        }

        return instance;
    }
}
