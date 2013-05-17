package org.elasticsearch.rest.operation;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.river.RiverName;
import org.elasticsearch.river.jdbc.JDBCRiver;

/**
 * Abstract parent of all JDBC River REST operations.
 * @author pdegeus
 */
public abstract class JdbcRestOperation {

    /**
     * Operation type enum.
     */
    public enum JdbcRestOperationType {
        /** Induce operation */
        INDUCE,
        /** Test operation */
        TEST
    }

    private final JdbcRestOperationType type;
    private final String riverName;

    /**
     * Constructor.
     * @param type Operation type.
     * @param riverName River name.
     */
    public JdbcRestOperation(JdbcRestOperationType type, String riverName) {
        this.type = type;
        this.riverName = riverName;
    }

    /**
     * @return Operation type.
     */
    public JdbcRestOperationType getType() {
        return type;
    }

    /**
     * @return River name this operation is targeting.
     */
    public String getRiverName() {
        return riverName;
    }

    /**
     * @return River name in a {@link RiverName} object.
     */
    public RiverName getRiverNameObj() {
        return new RiverName(JDBCRiver.TYPE, riverName);
    }

    /**
     * Executes this operation on the given {@link JDBCRiver} instance.
     * @param river River instance to execute on.
     * @return Operation result.
     */
    public abstract JdbcOperationResponse execute(JDBCRiver river);

    /**
     * Reconstruct specific data for this operation from the given {@link StreamInput}.
     * @param in Stream input.
     */
    public abstract void readFrom(StreamInput in);

    /**
     * Write specific data for this operation to the given {@link StreamOutput}.
     * @param out Stream output.
     */
    public abstract void writeTo(StreamOutput out);

    @Override
    public String toString() {
        return String.format("[%s::%s]", getClass().getSimpleName(), getRiverName());
    }

}
