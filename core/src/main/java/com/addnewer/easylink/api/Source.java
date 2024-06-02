package com.addnewer.easylink.api;

import java.lang.annotation.Documented;
import java.lang.annotation.Retention;
import java.lang.annotation.Target;

import static java.lang.annotation.ElementType.METHOD;
import static java.lang.annotation.RetentionPolicy.RUNTIME;

/**
 * @author pinru
 * @version 1.0
 */
@Target(METHOD)
@Retention(RUNTIME)
@Documented
public @interface Source {
}
