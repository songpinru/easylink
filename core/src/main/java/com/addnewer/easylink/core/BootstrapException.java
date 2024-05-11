package com.addnewer.easylink.core;

import org.slf4j.helpers.MessageFormatter;

/**
 * @author pinru
 * @version 1.0
 */
class BootstrapException extends RuntimeException {
    public BootstrapException(String message, Object... args) {
        super(MessageFormatter.basicArrayFormat(message, args));
    }

    public BootstrapException(String message, Throwable cause, Object... args) {
        super(MessageFormatter.basicArrayFormat(message, args), cause);
    }

    public BootstrapException(Throwable cause) {
        super(cause);
    }
}
