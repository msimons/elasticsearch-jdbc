package org.elasticsearch.rest.operation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.river.jdbc.JDBCRiver;

/**
 * Test operation with simple response text.
 * @author pdegeus
 */
public class JdbcTestOperation extends JdbcRestOperation {

    /**
     * Constructor.
     * @param riverName River name.
     */
    public JdbcTestOperation(String riverName) {
        super(JdbcRestOperationType.TEST, riverName);
    }

    @Override
    public JdbcOperationResponse execute(JDBCRiver river) {
        return new JdbcOperationResponse(true, "Tested on " + river);
    }

    @Override
    public void readFrom(StreamInput in) {
    }

    @Override
    public void writeTo(StreamOutput out) {
    }

}
