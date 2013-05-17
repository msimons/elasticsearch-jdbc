package org.elasticsearch.rest.dispatch;

import org.elasticsearch.rest.operation.JdbcOperationResponse;
import org.elasticsearch.rest.operation.JdbcRestOperation;

/**
 * Operation dispatcher interface.
 * @author pdegeus
 */
public interface JdbcOperationDispatcher {

    /**
     * Dispatches the given operation, if possible using this dispatcher.
     * @param operation Operation.
     * @return Operation response, or null if dispatching is not possible.
     * @throws DispatchException On any fatal error.
     */
    JdbcOperationResponse dispatch(JdbcRestOperation operation) throws DispatchException;

}
