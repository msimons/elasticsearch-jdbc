/*
 * Copyright (C) 2014 Jörg Prante
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.xbib.elasticsearch.common.util;

import org.elasticsearch.action.get.GetResponse;
import org.xbib.elasticsearch.jdbc.strategy.Sink;
import org.xbib.elasticsearch.support.client.Ingest;

import java.io.IOException;

/**
 * This class consumes pairs from a key/value stream
 * and transports them to the sink.
 */
public class SinkKeyValueStreamListener<K, V> extends PlainKeyValueStreamListener<K, V> {

    private Sink output;
    private Ingest ingest;

    /* needed for acknowledgement */
    public SinkKeyValueStreamListener<K, V> sqlCommand(SQLCommand sqlCommand) {
        this.sqlCommand = sqlCommand;
        return this;
    }

    public SinkKeyValueStreamListener<K, V> output(Sink output) {
        this.output = output;
        return this;
    }

    public SinkKeyValueStreamListener<K, V> ingest(Ingest ingest) {
        this.ingest = ingest;
        return this;
    }

    public SinkKeyValueStreamListener<K, V> shouldIgnoreNull(boolean shouldIgnoreNull) {
        super.shouldIgnoreNull(shouldIgnoreNull);
        return this;
    }

    public SinkKeyValueStreamListener<K, V> shouldDetectGeo(boolean shouldDetectGeo) {
        super.shouldDetectGeo(shouldDetectGeo);
        return this;
    }

    public SinkKeyValueStreamListener<K, V> shouldDetectJson(boolean shouldDetectJson) {
        super.shouldDetectJson(shouldDetectJson);
        return this;
    }

    /**
     * The object is complete. Push it to the sink.
     *
     * @param object the object
     * @return this value listener
     * @throws java.io.IOException if this method fails
     */
    public SinkKeyValueStreamListener<K, V> end(IndexableObject object) throws IOException {
        if (object.isEmpty()) {
            return this;
        }

        /*  only use jobid with ack */
        if(sqlCommand == null || !sqlCommand.isAcknowledge()){
            object.meta(ControlKeys._job.name(),null);
        }

        if (output != null) {
            if (object.optype() == null) {
                output.index(object, false);
            } else if ("index".equals(object.optype())) {
                output.index(object, false);
            } else if ("create".equals(object.optype())) {
                output.index(object, true);
            } else if ("update".equals(object.optype())) {
                output.update(object);
            } else if ("delete".equals(object.optype())) {
                output.delete(object);
            } else {
                throw new IllegalArgumentException("unknown optype: " + object.optype());
            }
        }
        return this;
    }

    @Override
    public Object fieldValue(String index, String type, String id, String fieldName) {
        if (ingest != null) {
            GetResponse response = ingest.client().prepareGet(index, type, id).setFields(fieldName).execute().actionGet();
            if (response.isExists() && response.getField(fieldName) != null) {
                return response.getField(fieldName).getValue();
            }
        }
        return null;
    }
}
