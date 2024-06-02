package com.addnewer.easylink.auto;

import java.lang.annotation.*;

/**
 * @author pinru
 * @version 1.0
 */
@Documented
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE})
public @interface Prefix {
    String value();
}
