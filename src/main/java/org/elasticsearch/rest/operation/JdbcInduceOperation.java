package org.elasticsearch.rest.operation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.river.jdbc.JDBCRiver;

/**
 * JDBC River induce operation.
 * @author pdegeus
 */
public class JdbcInduceOperation extends JdbcRestOperation {

    /**
     * Constructor.
     * @param riverName Target river name.
     */
    public JdbcInduceOperation(String riverName) {
        super(JdbcRestOperationType.INDUCE, riverName);
    }

    @Override
    public JdbcOperationResponse execute(JDBCRiver river) {
        river.induce();
        return new JdbcOperationResponse(true, null);
    }

    @Override
    public void readFrom(StreamInput in) {
    }

    @Override
    public void writeTo(StreamOutput out) {
    }

}
