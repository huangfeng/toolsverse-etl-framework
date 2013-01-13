/*
 * LinkedMemoryCache
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.Map;

import com.toolsverse.util.LimitedMap;

/**
 * LimitedMap implementation of the Cache interface. It it possible to configure maximum size of the cache. If max size is reached the oldest element gets
 * replaced with a new one.
 *
 * @see com.toolsverse.util.LimitedMap
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0 
 * @since 1.0
 */

public class LinkedMemoryCache<K, V> extends MemoryCache<K, V>
{
    
    /** The storage. */
    private LimitedMap<K, V> _storage;
    
    /**
     * Instantiates a new LinkedMemoryCache. There is no limit on cache size.
     */
    public LinkedMemoryCache()
    {
        init();
    }
    
    /**
     * Instantiates a new LinkedMemoryCache. Uses <code>maxSize</code> to set a maximum size of the cache.
     *
     * @param maxSize the max size of the cache
     */
    public LinkedMemoryCache(int maxSize)
    {
        init();
        
        _storage.setMaxSize(maxSize);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#getStorage()
     */
    @Override
    public Map<K, V> getStorage()
    {
        return _storage;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#init()
     */
    @Override
    protected void init()
    {
        _storage = new LimitedMap<K, V>();
    }
    
    /**
     * Sets the maximum size of the cache
     *
     * @param maxSize the new max size of the cache
     */
    public void setMaxSize(int maxSize)
    {
        _storage.setMaxSize(maxSize);
    }
}
