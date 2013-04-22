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
package org.elasticsearch.river.jdbc.strategy.table;

import java.io.IOException;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkResponse;
import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleRiverSource;
import org.elasticsearch.river.jdbc.support.Operations;

/**
 * River source implementation of the 'table' strategy
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TableRiverSource extends SimpleRiverSource {

    private final ESLogger logger = ESLoggerFactory.getLogger(TableRiverSource.class.getName());

    private Map<String,Long> lastSequenceMap = new HashMap<String,Long>();

    @Override
    public String strategy() {
        return "table";
    }

    @Override
    public String fetch() throws SQLException, IOException {
        Connection connection = connectionForReading();
        String[] optypes = { Operations.OP_CREATE, Operations.OP_INDEX, Operations.OP_DELETE, Operations.OP_UPDATE };

        long now = System.currentTimeMillis();
        Timestamp timestampFrom = new Timestamp(now - context.pollingInterval().millis());
        Timestamp timestampNow = new Timestamp(now);

        for (String optype : optypes) {
            PreparedStatement statement;
            try {
                statement = getQuery(connection, optype, "\"", "\"");
            } catch (SQLException e) {
                try {
                    // hsqldb
                    statement = getQuery(connection, optype, "", "\"");
                } catch (SQLException e2) {
                    logger.warn("Exception for both default and HSQLDB query", e2);
                    throw e;
                }
            }
            statement.setString(1, optype);
            if (!acknowledge()) {
                logger.trace("({}) Fetching riveritems with source_timestamp between {} and {} ", context.riverName(), timestampFrom, timestampNow);
                statement.setTimestamp(2, timestampFrom);
	            statement.setTimestamp(3, timestampNow);
            } else if (lastSequenceMap.get(optype) != null) {
                logger.trace("({}) Fetching riveritems having _seq > {}", context.riverName(), lastSequenceMap.get(optype));
                statement.setLong(2, lastSequenceMap.get(optype));
            } else {
                logger.trace("({}) Fetching all riveritems (lastSequence==null)", context.riverName());
            }

            ResultSet results;
            try {
                results = executeQuery(statement);
            } catch (SQLException e) {
                // mysql
                statement = getQuery(connection, optype, "", "");

                statement.setString(1, optype);
                if (!acknowledge()) {
	                statement.setTimestamp(2, timestampFrom);
	                statement.setTimestamp(3, timestampNow);
                } else if (lastSequenceMap.get(optype) != null) {
                    statement.setLong(2, lastSequenceMap.get(optype));
                }
                results = executeQuery(statement);
            }

            try {
                TableValueListener listener = new TableValueListener();
                listener.setHighestSequence(lastSequenceMap.get(optype));
                listener.target(context.riverMouth()).digest(context.digesting());
                merge(results, listener); // ignore digest

                //Update sequence for next run
                lastSequenceMap.put(optype,listener.getHighestSequence());
            } catch (Exception e) {
                throw new IOException(e);
            }
            close(results);
            close(statement);
            sendAcknowledge();
        }
        return null;
    }

    private PreparedStatement getQuery(Connection connection, String optype, String tableQuote, String fieldQuote) throws SQLException {
        String sql;
        if (acknowledge()) {

            if (lastSequenceMap.get(optype) == null) {
                sql = String.format(
                    "select * from %s%s%s where %ssource_operation%s = ? order by %s_seq%s",
                    tableQuote, context.riverName(), tableQuote,
                    fieldQuote, fieldQuote, fieldQuote, fieldQuote
                );
            } else {
                sql = String.format(
                    "select * from %s%s%s where %ssource_operation%s = ? and %s_seq%s > ? order by %s_seq%s",
                    tableQuote, context.riverName(), tableQuote,
                    fieldQuote, fieldQuote, fieldQuote, fieldQuote, fieldQuote, fieldQuote
                );
            }

        } else {
            sql = String.format(
                "select * from %s%s%s where %ssource_operation%s = ? and %ssource_timestamp%s between ? and ? order by %ssource_timestamp%s",
                tableQuote, context.riverName(), tableQuote,
                fieldQuote, fieldQuote, fieldQuote, fieldQuote, fieldQuote, fieldQuote
            );
        }

        logger.trace("({}) Created query: {}", context.riverName(), sql);
        return connection.prepareStatement(sql);
    }

    /**
     * Acknowledge a bulk item response back to the river table. Fill columns
     * target_timestamp, target_operation, target_failed, target_message.
     *
     * @param response
     * @throws IOException
     */
    @Override
    public SimpleRiverSource acknowledge(BulkResponse response) throws IOException {
        String riverName = context.riverName();

        if (response == null) {
            logger.warn("({}) can't acknowledge null bulk response", riverName);
            return this;
        }
        
        // if acknowlegde is disabled return current. 
        if (!acknowledge()) {
        	return this;
        }
        
        try {
            for (BulkItemResponse resp : response.items()) {
                PreparedStatement pstmt;
                List<Object> params;
                
                try {
                    pstmt = prepareUpdate("insert into \""+riverName+"_ack\" (\"_index\",\"_type\",\"_id\","+
                    		"\"target_timestamp\",\"target_operation\",\"target_failed\",\"target_message\") values (?,?,?,?,?,?,?)");

                } catch (SQLException e) {
                    try {
                        // hsqldb
                    	pstmt = prepareUpdate("insert into " + riverName + "_ack (\"_index\",\"_type\",\"_id\","+
                        		"\"target_timestamp\",\"target_operation\",\"target_failed\",\"target_message\") values (?,?,?,?,?,?,?)");
                    } catch (SQLException e1) {
                        // mysql
                    	pstmt = prepareUpdate("insert into " + riverName + "_ack (_index,_type,_id,"+
                        		"target_timestamp,target_operation,target_failed,target_message) values (?,?,?,?,?,?,?)");
                    }
                }
                params = new ArrayList<Object>();
                params.add(resp.getIndex());
                params.add(resp.getType());
                params.add(resp.getId());
                params.add(new Timestamp(new Date().getTime()));
                params.add(resp.opType());
                params.add(resp.isFailed());
                params.add(resp.getFailureMessage());
                bind(pstmt, params);
                executeUpdate(pstmt);
                close(pstmt);
            }
        } catch (SQLException ex) {
            throw new IOException(ex);
        }
        return this;
    }
}
