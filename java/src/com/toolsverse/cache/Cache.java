/*
 * Cache.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.io.Serializable;

/**
 * <code>Cache</code> interface provides set of methods for different cache
 * implementations, such as MemoryCache or SyncMemoryCache, etc.
 * 
 * <p>
 * <b>Example</b> of using cache object:
 * <code>destination.getCache().get(CURRENT_KEY);</code>
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Cache<K, V> extends CacheProvider<K, V>, Serializable
{
    
    /**
     * Clears the cache.
     */
    void clear();
    
    /**
     * Removes cache entry by key.
     * 
     * @param key
     *            the key
     */
    void clearByKey(K key);
    
    /**
     * Removes cache entry by value.
     *
     * @param value the value
     */
    void clearByValue(V value);
    
    /**
     * Gets the object stored in the cache by key.
     *
     * @param key the key
     * @return the object
     */
    V get(K key);
    
    /**
     * Puts the object into the cache.
     *
     * @param key the key
     * @param value the value
     * @return the object
     */
    V put(K key, V value);
    
    /**
     * Returns size of the cache.
     * 
     * @return the int
     */
    int size();
}
