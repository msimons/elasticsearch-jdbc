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
import org.elasticsearch.common.joda.time.DateTime;
import org.elasticsearch.common.joda.time.format.DateTimeFormat;
import org.elasticsearch.common.joda.time.format.DateTimeFormatter;

/**
 * Value listener for the 'table' strategy
 *
 * @author Jörg Prante <joergprante@gmail.com>
 */
public class TableValueListener extends SimpleValueListener {

	private static final String SOURCE_OPERATION = "source_operation";
    private static final String SOURCE_TIMESTAMP = "source_timestamp";
    private static final DateTimeFormatter DATE_FORMAT = DateTimeFormat.forPattern("yyyy-MM-dd'T'HH:mm:ss.SSS'Z'");

    private final ESLogger logger = ESLoggerFactory.getLogger(TableValueListener.class.getName());

    private DateTime highSourceTimestamp;

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
		if (SOURCE_OPERATION.equals(key)) {
            return map;
        } else if (SOURCE_TIMESTAMP.equals(key)) {
            if (value != null) {
                //Timestamp retrieved as String: 2013-04-17T13:15:28.945Z
                DateTime curTimestamp = DATE_FORMAT.parseDateTime(value.toString());

                logger.trace("source_timestamp parsed to {}", curTimestamp);
                if (highSourceTimestamp == null || curTimestamp.isAfter(highSourceTimestamp)) {
                    logger.debug("Setting source_timestamp as highest value: {}", curTimestamp);
                    highSourceTimestamp = curTimestamp;
                }
            }
            return map;
		} 
		
		return super.merge(map, key, value);
	}

    /**
     * @return The highest source timestamp processed by this listener.
     */
    public DateTime getHighSourceTimestamp() {
        return highSourceTimestamp;
    }

    /**
     * Set the highest source timestamp.
     */
    public void setHighSourceTimestamp(DateTime highSourceTimestamp) {
        this.highSourceTimestamp = highSourceTimestamp;
    }



}
