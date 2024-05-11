package com.addnewer.api;

import java.lang.annotation.*;

/**
 * 把当前类或者方法返回的类绑定到某个接口,当提供的Bean类型和参数中的不一致时使用
 *
 * @author pinru
 * @version 1.0
 */
@Target({ElementType.TYPE, ElementType.METHOD})
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface Bind {
    Class<?> value();
}
