/*
 * CollectionUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

/**
 * The general collection manipulation utilities.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 1.0
 */

public class CollectionUtils
{
    
    /**
     * Gets the first key with the input value in the map.
     * 
     * @param map
     *            the input map
     * @param value
     *            the value somewhere in the map
     * @return the first key found
     */
    public static Object getKeyFromValue(Map<?, ?> map, Object value)
    {
        if (map == null || value == null)
            return null;
        
        for (Object key : map.keySet())
            if (value.equals(map.get(key)))
                return key;
        
        return null;
    }
    
    /**
     * Gets the list of keys from the map
     * 
     * @param map
     *            the input map
     * @return the list of keys
     */
    public static List<?> getKeys(Map<?, ?> map)
    {
        if (map == null)
            return null;
        
        return new ArrayList<Object>(map.keySet());
    }
    
    /**
     * Gets the list of keys with the input value in the map.
     * 
     * @param map
     *            the input map
     * @param value
     *            the value somewhere in the map
     * @return the list of the all found keys
     */
    public static List<?> getKeysFromMap(Map<?, ?> map, Object value)
    {
        if (map == null || value == null)
            return null;
        
        List<Object> keys = new ArrayList<Object>();
        for (Object key : map.keySet())
            if (value.equals(map.get(key)))
            {
                keys.add(key);
            }
        
        return keys;
    }
    
    /**
     * Converts iterator to List
     * 
     * @param iter
     *            the input iterator
     * @return the list of objects in the iterator
     */
    public static List<?> iterator2List(Iterator<?> iter)
    {
        List<Object> list = new ArrayList<Object>();
        
        if (iter == null)
            return list;
        
        while (iter.hasNext())
            list.add(iter.next());
        
        return list;
    }
    
}