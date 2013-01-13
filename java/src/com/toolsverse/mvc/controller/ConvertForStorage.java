/*
 * ConvertForStorage.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The ConvertForStorage annotation is used in the ControllerAdapter to define a method where convertForStorage event is handled. 
 * 
 * convertForStorage event happens before value is sent from the view to the model. Regularly no conversion require so value just passing through. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ConvertForStorage
{
    
    /**
     * Name of the attribute.
     *
     * @return the string
     */
    public String name();
}
