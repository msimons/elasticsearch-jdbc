package org.elasticsearch.rest.dispatch.cluster;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.rest.operation.JdbcRestOperation;
import org.elasticsearch.rest.operation.JdbcRestOperation.JdbcRestOperationType;
import org.elasticsearch.rest.operation.JdbcRestOperationBuilder;
import org.elasticsearch.transport.TransportRequest;

/**
 * Transport version of the {@link JdbcRestOperation}.
 * @author pdegeus
 */
public class JdbcClusterOperation extends TransportRequest {

    private JdbcRestOperation localOperation;

    /**
     * Default constructor.
     */
    public JdbcClusterOperation() {
    }

    /**
     * Constructor.
     * @param localOperation Operation to transport.
     */
    public JdbcClusterOperation(JdbcRestOperation localOperation) {
        this.localOperation = localOperation;
    }

    /**
     * @return Retrieve local operation.
     */
    public JdbcRestOperation getLocalOperation() {
        return localOperation;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        JdbcRestOperationType type = JdbcRestOperationType.valueOf(in.readString());
        String riverName = in.readString();

        localOperation = new JdbcRestOperationBuilder(type, riverName).build();
        localOperation.readFrom(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeString(localOperation.getType().name());
        out.writeString(localOperation.getRiverName());
        localOperation.writeTo(out);
    }

    @Override
    public String toString() {
        return String.format("[%s::%s]", getClass().getSimpleName(), localOperation);
    }
}
