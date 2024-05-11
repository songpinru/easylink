package com.addnewer.config;

import org.slf4j.helpers.MessageFormatter;

public class ConfigurationException extends RuntimeException {

    public ConfigurationException(String message, Object... args) {
        super(MessageFormatter.basicArrayFormat(message, args));

    }

    public ConfigurationException(Throwable cause) {
        super(cause);
    }
}
