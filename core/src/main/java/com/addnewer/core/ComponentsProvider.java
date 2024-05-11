package com.addnewer.core;


import java.util.List;

/**
 * 辅助@Service的接口
 *
 * @author pinru
 * @version 1.0
 */
public interface ComponentsProvider {
    List<Class<?>> get();
}
