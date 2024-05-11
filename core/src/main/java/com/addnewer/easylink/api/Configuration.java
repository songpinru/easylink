package com.addnewer.easylink.api;


import java.lang.annotation.*;

/**
 * 配置依赖时使用,内部可以使用{@link Bean}来注入对象
 *
 * @author pinru
 * @version 1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface Configuration {
}
