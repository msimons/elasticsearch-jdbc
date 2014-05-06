package org.elasticsearch.rest.dispatch.cluster;

import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.rest.dispatch.JdbcLocalOperationDispatcher;
import org.elasticsearch.rest.operation.JdbcOperationResponse;
import org.elasticsearch.threadpool.ThreadPool.Names;
import org.elasticsearch.transport.TransportChannel;
import org.elasticsearch.transport.TransportRequestHandler;
import org.elasticsearch.transport.TransportResponse.Empty;

/**
 * Transport request handler for {@link JdbcClusterOperation}.
 * @author pdegeus
 */
public class JdbcClusterOperationListener implements TransportRequestHandler<JdbcClusterOperation> {

    private static final ESLogger LOG = ESLoggerFactory.getLogger(JdbcClusterOperationDispatcher.class.getName());

    /** Custom action identifying the JDBC River operation */
    public static final String ACTION = "river/jdbc/operation";

    private final JdbcLocalOperationDispatcher localDispatcher;

    @Inject
    public JdbcClusterOperationListener(JdbcLocalOperationDispatcher localDispatcher) {
        this.localDispatcher = localDispatcher;
    }

    @Override
    public void messageReceived(JdbcClusterOperation request, TransportChannel channel) throws Exception {
        LOG.debug("Received JdbcClusterOperation: " + request);

        JdbcOperationResponse result = localDispatcher.dispatch(request.getLocalOperation());
        if (result != null) {
            LOG.debug("Local operation executed, returning result: " + result);
            channel.sendResponse(result);
        } else {
            channel.sendResponse(Empty.INSTANCE);
        }
    }

    @Override
    public JdbcClusterOperation newInstance() {
        return new JdbcClusterOperation();
    }

    @Override
    public String executor() {
        return Names.SAME;
    }

    @Override
    public boolean isForceExecution() {
        return true;
    }
}
