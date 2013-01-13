/*
 * KeyValue.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

/**
 * <code>KeyValue</code> is the key-value class where the key and the value are objects.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 1.0
 */

public class KeyValue extends TypedKeyValue<Object, Object>
{
    
    /**
     * Instantiates a new key-value pair.
     * 
     * @param key the key
     * @param value the value
     */
    public KeyValue(Object key, Object value)
    {
        super(key, value);
    }
    
}
