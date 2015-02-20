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
import org.elasticsearch.rest.operation.JdbcTestOperation;
import org.elasticsearch.transport.TransportService;

/**
 * The JDBC River REST test action. Does nothing, just returns a message.
 * <p>
 * Example:<br/>
 * <code>
 * curl 'localhost:9200/_river/my_jdbc_river/_test'
 * </code>
 * @author pdegeus
 */
public class RestJDBCRiverTestAction extends AbstractJdbcRiverRestAction {

    @Inject
    public RestJDBCRiverTestAction(
        Settings settings, Client client, RestController controller, TransportService transportService, JdbcClusterOperationListener listener,
        JdbcLocalOperationDispatcher localDispatcher, JdbcClusterOperationDispatcher clusterDispatcher
    ) {
        super(settings, controller,client, transportService, listener, localDispatcher, clusterDispatcher);
        controller.registerHandler(RestRequest.Method.GET, "/_river/{river}/_test", this);
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
        execute(request, channel, new JdbcTestOperation(riverName));
    }

}
