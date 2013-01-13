/*
 * SynchLinkedMemoryCache
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

/**
 * The thread safe version of the LinkedMemoryCache class. 
 *
 * @see com.toolsverse.cache.LinkedMemoryCache
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0 
 * @since 1.0
 */

public class SynchLinkedMemoryCache<K, V> extends LinkedMemoryCache<K, V>
{
    
    /**
     * Instantiates a new SynchLinkedMemoryCache. There is no limit on cache size.
     */
    public SynchLinkedMemoryCache()
    {
        super();
    }
    
    /**
     * Instantiates a new SynchLinkedMemoryCache. Uses <code>maxSize</code> to set a maximum size of the cache.
     *
     * @param maxSize the max size of the cache
     */
    public SynchLinkedMemoryCache(int maxSize)
    {
        super(maxSize);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#clear()
     */
    @Override
    public synchronized void clear()
    {
        super.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clearByKey(java.lang.Object)
     */
    @Override
    public synchronized void clearByKey(K key)
    {
        super.clearByKey(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.Cache#clearByValue(java.lang.Object)
     */
    @Override
    public synchronized void clearByValue(V value)
    {
        super.clearByValue(value);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#get(java.lang.Object)
     */
    @Override
    public synchronized V get(K key)
    {
        return super.get(key);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#put(java.lang.Object,
     * java.lang.Object)
     */
    @Override
    public synchronized V put(K key, V value)
    {
        return super.put(key, value);
    }
    
}
