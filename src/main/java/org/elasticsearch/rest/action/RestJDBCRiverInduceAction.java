/*
 * Licensed to ElasticSearch and Shay Banon under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. ElasticSearch licenses this
 * file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.elasticsearch.rest.action;

import org.elasticsearch.client.Client;
import org.elasticsearch.common.inject.Inject;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.rest.RestChannel;
import org.elasticsearch.rest.RestController;
import org.elasticsearch.rest.RestRequest;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.rest.dispatch.JdbcLocalOperationDispatcher;
import org.elasticsearch.rest.dispatch.cluster.JdbcClusterOperationDispatcher;
import org.elasticsearch.rest.dispatch.cluster.JdbcClusterOperationListener;
import org.elasticsearch.rest.operation.JdbcInduceOperation;
import org.elasticsearch.transport.TransportService;

/**
 * The JDBC River REST induce action. The river can be fired once to run
 * when this action is called from REST.
 * <p>
 * Example:<br/>
 * <code>
 * curl -XPOST 'localhost:9200/_river/my_jdbc_river/_induce'
 * </code>
 * @author Jörg Prante <joergprante@gmail.com>
 * @author pdegeus
 */
public class RestJDBCRiverInduceAction extends AbstractJdbcRiverRestAction {

    @Inject
    public RestJDBCRiverInduceAction(
        Settings settings, RestController controller, Client client, TransportService transportService, JdbcClusterOperationListener listener,
        JdbcLocalOperationDispatcher localDispatcher, JdbcClusterOperationDispatcher clusterDispatcher
    ) {
        super(settings, controller, client, transportService, listener, localDispatcher, clusterDispatcher);
        controller.registerHandler(RestRequest.Method.POST, "/_river/{river}/_induce", this);
    }

    @Override
    protected void handleRequest(RestRequest request, RestChannel channel, Client client) throws Exception {

        //Get and check river name parameter
        String riverName = request.param("river");
        if (riverName == null || riverName.isEmpty()) {
            respond(false, request, channel, "Parameter 'river' is required", RestStatus.BAD_REQUEST);
            return;
        }

        //Execute
        execute(request, channel, new JdbcInduceOperation(riverName));
    }


}
