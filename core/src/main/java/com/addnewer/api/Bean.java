package com.addnewer.api;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 在{@link Configuration}标注的类里，注入方法返回的对象
 * @author pinru
 * @version 1.0
 */
@Target(METHOD)
@Retention(RUNTIME)
@Documented
public @interface Bean {
}
