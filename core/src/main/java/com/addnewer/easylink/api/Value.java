package com.addnewer.easylink.api;

import java.lang.annotation.*;

/**
 * 在Component中注入字段值,要求只能是基础类型或者字符串
 * @author pinru
 * @version 1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.FIELD})
public @interface Value {
    String value();
}
