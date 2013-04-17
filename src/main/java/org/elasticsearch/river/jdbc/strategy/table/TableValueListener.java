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
import java.util.Map;

import org.elasticsearch.common.logging.ESLogger;
import org.elasticsearch.common.logging.ESLoggerFactory;
import org.elasticsearch.river.jdbc.strategy.simple.SimpleValueListener;
import org.elasticsearch.river.jdbc.support.StructuredObject;

/**
 * Value listener for the 'table' strategy
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TableValueListener extends SimpleValueListener {

	private static final String SOURCE_OPERATION = "source_operation";
    private static final String SOURCE_TIMESTAMP = "source_timestamp";
    private static final String SEQUENCE = "_seq";

    private final ESLogger logger = ESLoggerFactory.getLogger(TableValueListener.class.getName());

    private Long highSequence;

    @Override
	protected void map(String k, String v, StructuredObject current) throws IOException {
		if (SOURCE_OPERATION.equals(k)) {
			current.optype(v);
		} else { 
			super.map(k, v, current);
		}
	}

	@Override
	protected Map merge(Map map, String key, Object value) {
        // skip elements in content
		if (SOURCE_OPERATION.equals(key) || SOURCE_TIMESTAMP.equals(key)) {
            return map;
        } else if (SEQUENCE.equals(key)) {
            if (value instanceof Long) {
                highSequence = (Long) value;
            } else {
                logger.error("Could not retrieve value of '_seq' field, unsupported type: {}", value.getClass());
            }
            return map;
		} 
		
		return super.merge(map, key, value);
	}

    /**
     * @return The highest source timestamp processed by this listener.
     */
    public Long getHighestSequence() {
        return highSequence;
    }

    /**
     * Set the highest source timestamp.
     */
    public void setHighestSequence(Long highSequence) {
        this.highSequence = highSequence;
    }



}
