package com.addnewer.easylink.core;

/**
 * @author pinru
 * @version 1.0
 * @date 2024/5/4
 */
interface InjectFunction<T> {
    T provide(Container container);
}
