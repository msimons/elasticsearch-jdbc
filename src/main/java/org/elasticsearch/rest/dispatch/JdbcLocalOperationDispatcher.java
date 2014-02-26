package org.elasticsearch.rest.dispatch;

import java.lang.reflect.Field;
import java.util.Map;

import org.elasticsearch.common.collect.ImmutableMap;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.inject.Injector;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.rest.operation.JdbcOperationResponse;
import org.elasticsearch.rest.operation.JdbcRestOperation;
import org.elasticsearch.river.River;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.RiversService;
import org.elasticsearch.river.jdbc.JDBCRiver;

/**
 * Local JDBC River operation dispatcher, using an injected instance of the {@link RiversService}.
 * Since the RiversService does not expose the registered rivers, this class uses reflection to
 * retrieve the internal map of rivers.
 * @author pdegeus
 */
public class JdbcLocalOperationDispatcher implements JdbcOperationDispatcher {

    private static final ESLogger LOG = ESLoggerFactory.getLogger(JdbcLocalOperationDispatcher.class.getName());

    private final RiversService riversService;

    @Inject
    public JdbcLocalOperationDispatcher(Injector injector) {
        this.riversService = injector.getInstance(RiversService.class);
    }

    @Override
    public JdbcOperationResponse dispatch(JdbcRestOperation operation) throws DispatchException {
        JDBCRiver river = findLocalRiver(operation.getRiverName());
        if (river != null) {
            return operation.execute(river);
        }

        return null;
    }

    @SuppressWarnings("unchecked")
    private JDBCRiver findLocalRiver(String riverName) throws DispatchException {
        LOG.debug("Searching for river in local RiversService");

        //Retrieve the locally registered rivers using reflection (UGLY HACK!!)
        //TODO: Obtain the rivers using public API
        ImmutableMap<RiverName, River> rivers;
        try {
            Field field = RiversService.class.getDeclaredField("rivers");
            field.setAccessible(true);
            rivers = (ImmutableMap<RiverName, River>) field.get(riversService);
        } catch (NoSuchFieldException e) {
            throw new DispatchException(e);
        } catch (IllegalAccessException e) {
            throw new DispatchException(e);
        }

        //Check if river is present
        for (Map.Entry<RiverName, River> entry : rivers.entrySet()) {
            RiverName name = entry.getKey();
            if (name.getName().equals(riverName)) {
                if (!name.getType().equals(JDBCRiver.TYPE)) {
                    throw new DispatchException("River '" + riverName + "' is not a jdbc-river, but has type " + name.getType());
                }

                LOG.debug("Local river found: " + entry.getValue());
                return (JDBCRiver) entry.getValue();
            }
        }

        LOG.debug("Local river not found");
        return null;
    }

}
