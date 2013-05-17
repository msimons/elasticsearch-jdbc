package org.elasticsearch.rest.operation;

import org.elasticsearch.rest.operation.JdbcRestOperation.JdbcRestOperationType;

/**
 * Factory builder to construct {@link JdbcRestOperation JdbcRestOperations}.
 * @author pdegeus
 */
public class JdbcRestOperationBuilder {

    private JdbcRestOperation operation;

    /**
     * Constructor.
     * @param type Operation type.
     * @param riverName River name to work on.
     */
    public JdbcRestOperationBuilder(JdbcRestOperationType type, String riverName) {
        switch (type) {
            case INDUCE:
                operation = new JdbcInduceOperation(riverName);
                break;
            case TEST:
                operation = new JdbcTestOperation(riverName);
                break;
            default:
                throw new IllegalArgumentException("Unknown operation type: " + type);
        }
    }

    /**
     * @return JdbcRestOperation instance.
     */
    public JdbcRestOperation build() {
        return operation;
    }

}
