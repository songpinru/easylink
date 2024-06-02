package com.addnewer.easylink.jdbc;

import com.addnewer.easylink.auto.AutoUtil;
import org.apache.flink.api.java.utils.ParameterTool;

import java.lang.reflect.InvocationTargetException;


/**
 * @author pinru
 * @version 1.0
 */
public class DatasourceUtil {

    public static DatasourceProperty getDatasourceProperty(ParameterTool config, String name) throws NoSuchMethodException, InvocationTargetException, InstantiationException, IllegalAccessException {
        return AutoUtil.createBean(DatasourceProperty.class, name, config);
    }


}
