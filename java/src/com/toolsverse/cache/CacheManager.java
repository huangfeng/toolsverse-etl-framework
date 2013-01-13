/*
 * CacheManager.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class is the centralized storage of all registered caches.  Basically if you want to clear multiple caches at once - just add them to the CacheManager and then
 * call clear() in due time. There are other applications of the CacheManager as well, such as: get a reference to the particular cache from any point in the code,
 * etc. The CacheManager is thread safe.
 *
 * @see com.toolsverse.cache.Cache
 *
 * @author Maksym Sherbinin
 * @version 2.0 
 * @since 1.0
 */

public class CacheManager
{
    // volatile is needed so that multiple thread can reconcile the _instance
    
    /** The instance of the CacheManager. */
    private static CacheManager _instance = new CacheManager();
    
    /**
     * Returns instance of the CacheManager
     *
     * @return the cache manager
     */
    public static CacheManager instance()
    {
        return _instance;
    }
    
    /** Registered caches */
    private Map<String, Cache<?, ?>> _map;
    
    /**
     * Instantiates a new CacheManager.
     */
    private CacheManager()
    {
        synchronized (this)
        {
            _map = new ConcurrentHashMap<String, Cache<?, ?>>();
        }
    }
    
    /**
     * Adds the cache to the storage using topic as a key.
     *
     * @param topic the key
     * @param cache the cache
     */
    public void add(String topic, Cache<?, ?> cache)
    {
        _map.put(topic, cache);
    }
    
    /**
     * Clears all registered caches.
     */
    public void clear()
    {
        for (String topic : _map.keySet())
            clear(topic);
    }
    
    /**
     * Clears the cache associated with the topic.
     *
     * @param topic the topic
     */
    public void clear(String topic)
    {
        Cache<?, ?> cache = get(topic);
        
        if (cache != null)
            cache.clear();
    }
    
    /**
     * Gets the cache associated with the topic.
     *
     * @param topic the topic
     * @return the cache
     */
    public Cache<?, ?> get(String topic)
    {
        return _map.get(topic);
    }
    
    /**
     * Removes the cache associated with the topic from the storage.
     *
     * @param topic the topic
     * @param cache the cache
     */
    public void remove(String topic, Cache<?, ?> cache)
    {
        _map.remove(topic);
    }
}
