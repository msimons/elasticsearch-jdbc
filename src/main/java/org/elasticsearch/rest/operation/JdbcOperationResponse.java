package org.elasticsearch.rest.operation;

import java.io.IOException;

import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;
import org.elasticsearch.transport.TransportResponse;

/**
 * JDBC REST operation transportable response data container.
 * @author pdegeus
 */
public class JdbcOperationResponse extends TransportResponse {

    private boolean success;
    private String message;

    /**
     * Default constructor.
     */
    public JdbcOperationResponse() {
    }

    /**
     * Constructor with data.
     * @param success Success flag.
     * @param message Response message.
     */
    public JdbcOperationResponse(boolean success, String message) {
        this.success = success;
        this.message = message;
    }

    /**
     * @return Success flag.
     */
    public boolean isSuccess() {
        return success;
    }

    /**
     * @return Response message.
     */
    public String getMessage() {
        return message;
    }

    @Override
    public void readFrom(StreamInput in) throws IOException {
        super.readFrom(in);

        success = in.readBoolean();
        message = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);

        out.writeBoolean(success);
        out.writeOptionalString(message);
    }

    @Override
    public String toString() {
        return String.format("[%s::%s::%s]", getClass().getSimpleName(), (success ? "success" : "failure"), message);
    }

}
