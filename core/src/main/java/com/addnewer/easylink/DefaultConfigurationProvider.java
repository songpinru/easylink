package com.addnewer.easylink;

import com.addnewer.easylink.core.ComponentsProvider;

import java.util.List;

/**
 * @author pinru
 * @version 1.0
 */
public class DefaultConfigurationProvider implements ComponentsProvider {
    @Override
    public List<Class<?>> get() {
        return List.of(DefaultConfiguration.class);
    }
}