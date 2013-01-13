/*
 * ObjectStorageMap.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.HashMap;
import java.util.Map;

/**
 * The HashMap which implements an ObjectStorage interface.
 * 
 * @see com.toolsverse.util.ObjectStorage
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class ObjectStorageMap extends HashMap<String, Object> implements
        ObjectStorage
{
    
    /**
     * Instantiates a new empty ObjectStorageMap.
     */
    public ObjectStorageMap()
    {
    }
    
    /**
     * Instantiates a new ObjectStorageMap from the existing Map.
     * 
     * @param value the Map to initialize ObjectStorageMap 
     */
    public ObjectStorageMap(Map<String, String> value)
    {
        if (value != null)
            putAll(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getString(java.lang.String)
     */
    public String getString(String key)
    {
        Object value = get(key);
        
        if (value == null)
            return null;
        
        return value.toString();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#getValue(java.lang.String)
     */
    public Object getValue(String key)
    {
        return get(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.ObjectStorage#setValue(java.lang.String,
     * java.lang.Object)
     */
    public void setValue(String key, Object value)
    {
        put(key, value);
    }
}
