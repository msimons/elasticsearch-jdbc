package org.elasticsearch.rest.dispatch.cluster;

import org.elasticsearch.cluster.ClusterService;
import org.elasticsearch.cluster.node.DiscoveryNode;
import org.elasticsearch.cluster.node.DiscoveryNodes;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.rest.dispatch.JdbcOperationDispatcher;
import org.elasticsearch.rest.operation.JdbcOperationResponse;
import org.elasticsearch.rest.operation.JdbcRestOperation;
import org.elasticsearch.river.cluster.RiverNodeHelper;
import org.elasticsearch.transport.FutureTransportResponseHandler;
import org.elasticsearch.transport.TransportService;

/**
 * Cluster dispatcher for REST operations.
 * @author pdegeus
 */
public class JdbcClusterOperationDispatcher implements JdbcOperationDispatcher {

    private static final ESLogger LOG = ESLoggerFactory.getLogger(JdbcClusterOperationDispatcher.class.getName());

    private final TransportService transportService;
    private final ClusterService clusterService;

    @Inject
    public JdbcClusterOperationDispatcher(ClusterService clusterService, TransportService transportService) {
        this.clusterService = clusterService;
        this.transportService = transportService;
    }

    @Override
    public JdbcOperationResponse dispatch(JdbcRestOperation operation) {
        LOG.debug("Searching for river on cluster");

        DiscoveryNodes discoNodes = clusterService.state().nodes();
        for (final DiscoveryNode node : discoNodes) {
            if (node.equals(discoNodes.localNode())) {
                // no need to send to our self
                continue;
            }

            if (RiverNodeHelper.isRiverNode(node, operation.getRiverNameObj())) {
                LOG.debug("Found river on node: " + node);

                JdbcClusterOperation transportOperation = new JdbcClusterOperation(operation);

                JdbcOperationResponse response = transportService.submitRequest(
                    node, JdbcClusterOperationListener.ACTION, transportOperation, new FutureTransportResponseHandler<JdbcOperationResponse>() {

                    @Override
                    public JdbcOperationResponse newInstance() {
                        return new JdbcOperationResponse();
                    }
                }).txGet();

                if (response != null) {
                    return response;
                }
            }
        }

        return null;
    }

}
