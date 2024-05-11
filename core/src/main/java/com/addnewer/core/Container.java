package com.addnewer.core;

import java.util.HashMap;
import java.util.Map;

/**
 * @author pinru
 * @version 1.0
 * @date 2024/5/4
 */
class Container {
    private final Map<Key, Object> beans = new HashMap<>();

    public <T> T getBean(Key key) {
        return (T) beans.get(key);
    }

    public <T> void addBean(Key key, T value) {
        beans.put(key, value);
    }
    public boolean containsKey(Key key) {
        return beans.containsKey(key);
    }


    @Override
    public String toString() {
        return beans.keySet().toString();
    }
}
