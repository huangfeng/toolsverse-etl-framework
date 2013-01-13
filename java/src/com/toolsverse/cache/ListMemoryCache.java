/* 
 * ListMemoryCache.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.ArrayList;
import java.util.List;

/**
 * ArrayList implementation of the Cache interface. Basically key is an index in the list.
 *
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ListMemoryCache<V> implements Cache<Number, V>
{
    
    /** The storage. */
    private List<V> _storage;
    
    /**
     * Instantiates a new ListMemoryCache.
     */
    public ListMemoryCache()
    {
        init();
    }
    
    /**
     * Adds the value to the cache
     *
     * @param value the value
     * @return the value
     */
    public V add(V value)
    {
        getStorage().add(value);
        
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clear()
     */
    public void clear()
    {
        getStorage().clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clearByKey(java.lang.Object)
     */
    public void clearByKey(Number key)
    {
        getStorage().remove(key.intValue());
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clearByValue(java.lang.Object)
     */
    public void clearByValue(V value)
    {
        getStorage().remove(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#get(java.lang.Object)
     */
    public V get(Number key)
    {
        return getStorage().get(key.intValue());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.CacheProvider#getCache()
     */
    public Cache<Number, V> getCache()
    {
        return this;
    }
    
    /**
     * Gets the storage.
     *
     * @return the storage
     */
    public List<V> getStorage()
    {
        return _storage;
    }
    
    /**
     * Instantiate storage.
     */
    protected void init()
    {
        _storage = new ArrayList<V>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#put(java.lang.Object, java.lang.Object)
     */
    public V put(Number key, V value)
    {
        getStorage().add(key.intValue(), value);
        
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#size()
     */
    public int size()
    {
        return getStorage().size();
    }
    
}
