/*
 * ObjectFactoryModule.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.factory;

import com.toolsverse.config.SystemConfig;

/**
 * An interface which must be implement by the class if it is designed to bind interfaces to implementations. Used by ObjectFactory class.'
 
 * @see ObjectFactory
 * @see SystemConfig
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ObjectFactoryModule
{
    
    /**
     * Binds fromName to toName. In practice binds an interface to the implementation.
     *
     * @param fromName the "from" class name
     * @param toName the "to" class name
     */
    void bind(String fromName, String toName);
    
    /**
     * Gets the binded name by the original name.
     *
     * @param name the original name
     * @return the binded name
     */
    String get(String name);
}
