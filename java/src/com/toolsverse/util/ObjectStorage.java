/*
 * ObjectStorage.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

/**
 * An object that maps keys to the values. The key is always a string.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ObjectStorage
{
    
    /**
     * Gets the string representation of the value by the key. 
     * 
     * @param key the key
     * 
     * @return the string representation of the value or null if there is no mapping between key and value
     */
    String getString(String key);
    
    /**
     * Gets the value by the key.
     * 
     * @param key the key
     * 
     * @return the value or null if there is no mapping between key and value
     */
    Object getValue(String key);
    
    /**
     * Associates value with the key. Key can not be null.
     * 
     * @param key the key
     * @param value the value
     */
    void setValue(String key, Object value);
}
