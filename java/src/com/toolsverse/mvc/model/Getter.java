/*
 * Getter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

import com.toolsverse.util.Null;

/**
 * The Getter annotation is used in the Model to define a getter method.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface Getter
{
    
    /**
     * Name of the attribute
     *
     * @return the string
     */
    public String name();
    
    /**
     * The class of the object used as a parameter for the attribute. If != null && != Null.class it will be automatically instantiated and set for the attribute.
     *
     * @return the class
     */
    public Class<?> paramsClass() default Null.class;
}
