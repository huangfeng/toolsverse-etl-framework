/*
 * SynchListMemoryCache.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.util.List;
import java.util.Vector;

/**
 * Thread safe version of the ListMemoryCache. The instance of the Vector class is used as a storage.
 *
 * @see com.toolsverse.cache.ListMemoryCache
 *
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0 
 * @since 1.0
 */

public class SynchListMemoryCache<V> extends ListMemoryCache<V>
{
    
    /** The storage. */
    private List<V> _synchStorage;
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.ListMemoryCache#getStorage()
     */
    @Override
    public List<V> getStorage()
    {
        return _synchStorage;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.cache.ListMemoryCache#init()
     */
    @Override
    protected void init()
    {
        _synchStorage = new Vector<V>();
    }
    
}
