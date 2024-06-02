package com.addnewer.easylink.jdbc.cdc;

import com.addnewer.easylink.auto.AutoUtil;
import com.addnewer.easylink.core.AutoInject;
import com.google.auto.service.AutoService;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;


/**
 * @author pinru
 * @version 1.0
 */
@AutoService(AutoInject.class)
public class AutoInjectDataSourceProperty implements AutoInject<MysqlSourceProperty> {


    @Override
    public boolean isAutoInject(Class<?> clazz) {
        return clazz.equals(MysqlSourceProperty.class);
    }

    @Override
    public MysqlSourceProperty auto(String name, ParameterTool config) {
        try {
            return AutoUtil.createBean(MysqlSourceProperty.class, name, config);
        } catch (NoSuchMethodException | InvocationTargetException | InstantiationException |
                 IllegalAccessException e) {
            throw new RuntimeException(e);
        }
    }


}
