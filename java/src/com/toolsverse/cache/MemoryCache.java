/*
 * MemoryCache.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.HashMap;
import java.util.Map;

import com.toolsverse.util.CollectionUtils;

/**
 * HashMap implementation of the Cache interface.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MemoryCache<K, V> implements Cache<K, V>, CacheProvider<K, V>
{
    
    /** The storage. */
    private Map<K, V> _storage;
    
    /**
     * Instantiates a new MemoryCache.
     */
    public MemoryCache()
    {
        init();
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
    public void clearByKey(K key)
    {
        getStorage().remove(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clearByValue(java.lang.Object)
     */
    @SuppressWarnings("unchecked")
    public void clearByValue(V value)
    {
        clearByKey((K)CollectionUtils.getKeyFromValue(getStorage(), value));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#get(java.lang.Object)
     */
    public V get(K key)
    {
        return getStorage().get(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.CacheProvider#getCache()
     */
    public Cache<K, V> getCache()
    {
        return this;
    }
    
    /**
     * Gets the storage.
     *
     * @return the storage
     */
    public Map<K, V> getStorage()
    {
        return _storage;
    }
    
    /**
     * Instantiates storage.
     */
    protected void init()
    {
        _storage = new HashMap<K, V>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#put(java.lang.Object, java.lang.Object)
     */
    public V put(K key, V value)
    {
        getStorage().put(key, value);
        
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
