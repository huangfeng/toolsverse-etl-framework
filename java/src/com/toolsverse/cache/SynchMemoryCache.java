/*
 * SynchMemoryCache
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * ConcurrentHashMap implementation of the Cache interface. This version is thread safe.
 *
 * @param <K> the key type
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SynchMemoryCache<K, V> extends MemoryCache<K, V>
{
    
    /** The storage. */
    private Map<K, V> _synchStorage;
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#getStorage()
     */
    @Override
    public Map<K, V> getStorage()
    {
        return _synchStorage;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.MemoryCache#init()
     */
    @Override
    protected void init()
    {
        _synchStorage = new ConcurrentHashMap<K, V>();
    }
    
}
