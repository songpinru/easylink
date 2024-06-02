package com.addnewer.easylink.jdbc;

import com.addnewer.easylink.core.AutoInject;
import com.google.auto.service.AutoService;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;


/**
 * @author pinru
 * @version 1.0
 */
@AutoService(AutoInject.class)
public class AutoInjectDataSourceProperty implements AutoInject<DatasourceProperty> {


    @Override
    public boolean isAutoInject(Class<?> clazz) {
        return clazz.equals(DatasourceProperty.class);
    }

    @Override
    public DatasourceProperty auto( String name, ParameterTool config) {
        try {
             return DatasourceUtil.getDatasourceProperty(config, name);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException | IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


}
