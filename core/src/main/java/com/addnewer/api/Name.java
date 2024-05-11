package com.addnewer.api;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.*;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * 注入同一个类的不同对象时使用，用于区分
 *
 * @author pinru
 * @version 1.0
 */
@Target({PARAMETER, METHOD, CONSTRUCTOR})
@Retention(RUNTIME)
@Documented
public @interface Name {

    String value() default "";
}
