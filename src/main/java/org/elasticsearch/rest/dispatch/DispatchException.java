package org.elasticsearch.rest.dispatch;

/**
 * Operation dispatch exception.
 * @author pdegeus
 */
public class DispatchException extends Exception {

    private static final long serialVersionUID = -6292192975759284149L;

    /**
     * Constructor.
     * @param s Message.
     */
    public DispatchException(String s) {
        super(s);
    }

    /**
     * Constructor.
     * @param s Message.
     * @param throwable Cause.
     */
    public DispatchException(String s, Throwable throwable) {
        super(s, throwable);
    }

    /**
     * Constructor.
     * @param throwable Cause.
     */
    public DispatchException(Throwable throwable) {
        super(throwable);
    }

}
