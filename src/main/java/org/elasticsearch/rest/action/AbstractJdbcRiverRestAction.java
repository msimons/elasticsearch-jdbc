package org.elasticsearch.rest.action;

import java.io.IOException;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.xcontent.XContentBuilder;
import org.elasticsearch.rest.BaseRestHandler;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.XContentRestResponse;
import org.elasticsearch.rest.XContentThrowableRestResponse;
import org.elasticsearch.rest.action.support.RestXContentBuilder;
import org.elasticsearch.rest.dispatch.DispatchException;
import org.elasticsearch.rest.dispatch.JdbcOperationDispatcher;
import org.elasticsearch.rest.dispatch.cluster.JdbcClusterOperationListener;
import org.elasticsearch.rest.operation.JdbcOperationResponse;
import org.elasticsearch.rest.operation.JdbcRestOperation;
import org.elasticsearch.transport.TransportService;

/**
 * Abstract parent of all JDBC River REST actions, performing the actual operation.
 * @author pdegeus
 */
public abstract class AbstractJdbcRiverRestAction extends BaseRestHandler {

    private static final ESLogger LOG = ESLoggerFactory.getLogger(AbstractJdbcRiverRestAction.class.getName());
    private static volatile boolean clusterListenerRegistered = false;

    private final JdbcOperationDispatcher localDispatcher, clusterDispatcher;

    /**
     * Constructor.
     * @param settings Setting instance.
     * @param client Client instance.
     * @param transportService TransportService instance.
     * @param listener JdbcClusterOperationListener instance.
     * @param localDispatcher Local operation dispatcher.
     * @param clusterDispatcher Cluster operation dispatcher.
     */
    protected AbstractJdbcRiverRestAction(
        Settings settings, Client client, TransportService transportService, JdbcClusterOperationListener listener,
        JdbcOperationDispatcher localDispatcher, JdbcOperationDispatcher clusterDispatcher
    ) {
        super(settings, client);
        this.localDispatcher = localDispatcher;
        this.clusterDispatcher = clusterDispatcher;

        if (!clusterListenerRegistered) {
            LOG.debug("Registering JdbcClusterOperationListener at TransportService with action '{}'", JdbcClusterOperationListener.ACTION);
            transportService.registerHandler(JdbcClusterOperationListener.ACTION, listener);
            clusterListenerRegistered = true;
        }
    }

    /**
     * Execute a REST operation. Checks if the river is present locally, otherwise dispatched the operation
     * across the cluster.
     * @param request Incoming {@link RestRequest}.
     * @param channel {@link RestChannel} to write to.
     * @param operation Operation to perform.
     */
    protected void execute(RestRequest request, RestChannel channel, JdbcRestOperation operation) {
        LOG.info("Received REST operation: " + operation);

        JdbcOperationResponse response = null;
        try {
            response = localDispatcher.dispatch(operation);
            if (response == null) {
                response = clusterDispatcher.dispatch(operation);
            }
        } catch (DispatchException e) {
            errorResponse(request, channel, e);
        }

        if (response == null) {
            respond(false, request, channel, "River not found: " + operation.getRiverName(), RestStatus.NOT_FOUND);
        } else {
            respond(response.isSuccess(), request, channel, response.getMessage(), RestStatus.OK);
        }
    }

    /**
     * Send a response.
     * @param success Success flag.
     * @param request Incoming {@link RestRequest}.
     * @param channel {@link RestChannel} to respond on.
     * @param message Response message, or null.
     * @param status Status code to return.
     */
    protected void respond(boolean success, RestRequest request, RestChannel channel, String message, RestStatus status) {
        try {
            XContentBuilder builder = RestXContentBuilder.restContentBuilder(request);
            builder.startObject();
            builder.field("success", success);
            if (message != null) {
                builder.field("message", message);
            }
            builder.endObject();
            channel.sendResponse(new XContentRestResponse(request, status, builder));
        } catch (IOException e) {
            errorResponse(request, channel, e);
        }
    }

    /**
     * Respond with an error caused by an exception.
     * @param request Incoming {@link RestRequest}.
     * @param channel {@link RestChannel} to respond on.
     * @param e Exception.
     */
    protected void errorResponse(RestRequest request, RestChannel channel, Throwable e) {
        try {
            channel.sendResponse(new XContentThrowableRestResponse(request, e));
        } catch (IOException e1) {
            logger.error("Failed to send failure response", e1);
        }
    }

}
