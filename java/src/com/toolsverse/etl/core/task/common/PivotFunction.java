/*
 * PivotFunction.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation is used to identify pivot function, such as SUM(), AVG(), etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface PivotFunction
{
    /**
     * Name of the function
     *
     * @return the name of the function 
     */
    public String name();
    
    /**
     * Pattern for the function call
     *
     * @return the pattern for the function call
     */
    public String pattern();
    
}
