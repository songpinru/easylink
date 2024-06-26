package com.addnewer.easylink.api;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.CONSTRUCTOR;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 在类里使用，标注使用哪个构造方法注入
 * @author pinru
 * @version 1.0
 */
@Target({CONSTRUCTOR})
@Retention(RUNTIME)
@Documented
public @interface Inject {
}
