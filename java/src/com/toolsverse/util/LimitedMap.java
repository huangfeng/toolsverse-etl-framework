/*
 * LimitedMap.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * This class is a LinkedHashMap with a limited number of entries.
 * If new entry is added and max limit is exceeded the eldest entry is removed.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class LimitedMap<K, V> extends LinkedHashMap<K, V>
{
    
    private int _maxSize;
    
    /**
     * Instantiates a new empty LimitedMap with a no limit restiction.
     */
    public LimitedMap()
    {
        this(-1);
    }
    
    /**
     * Instantiates a new empty LimitedMap with a specified limit.
     * 
     * @param maxSize the max size
     */
    public LimitedMap(int maxSize)
    {
        super();
        
        setMaxSize(maxSize);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.LinkedHashMap#removeEldestEntry(java.util.Map.Entry)
     */
    @Override
    protected boolean removeEldestEntry(Map.Entry<K, V> eldest)
    {
        return _maxSize > 0 && size() > _maxSize;
    }
    
    /**
     * Sets the max size.
     * 
     * @param value the new max size
     */
    public void setMaxSize(int value)
    {
        _maxSize = value;
    }
    
}
