/*
 * ViewAction.java
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
 * The ViewAction annotation is used in the ControllerAdapter to define the method where action event is handled.
 * 
 * For example when user clicks on the button with the name "save" the action event "save" is generated and sent to this method.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.METHOD)
public @interface ViewAction
{
    
    /**
     * Name of the command.
     *
     * @return the string
     */
    public String name();
}
