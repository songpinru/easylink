package com.addnewer.easylink.test;

import java.lang.annotation.*;

/**
 * @author pinru
 * @version 1.0
 */
@Documented
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
public @interface Components {
    Component[] value();
}
