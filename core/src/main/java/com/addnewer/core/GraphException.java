package com.addnewer.core;

import org.slf4j.helpers.MessageFormatter;

/**
 * @author pinru
 * @version 1.0
 */
class GraphException extends RuntimeException{
    public GraphException(String message,Object... objects) {
        super(MessageFormatter.basicArrayFormat(message,objects));
    }

    public GraphException(Throwable cause) {
        super(cause);
    }
}
