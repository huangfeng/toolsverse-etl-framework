/*
 * LocalStorageProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage.impl.local;

import java.util.Map;

import com.toolsverse.cache.Cache;
import com.toolsverse.cache.MemoryCache;
import com.toolsverse.cache.SynchMemoryCache;
import com.toolsverse.storage.StorageProvider;

/**
 * The MemoryCache implementation of the StorageProvider interface.
 * 
 * @see com.toolsverse.cache.MemoryCache
 * @see com.toolsverse.cache.SynchMemoryCache
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class LocalStorageProvider implements StorageProvider
{
    
    /** The cache. */
    private Cache<String, StorageObject> _cache;
    
    /**
     * Instantiates a new local storage provider.
     */
    public LocalStorageProvider()
    {
        this(false);
    }
    
    /**
     * Instantiates a new local storage provider.
     *
     * @param synch if true the SynchMemoryCache will be used to store properties, otherwise - MemoryCache
     */
    public LocalStorageProvider(boolean synch)
    {
        if (synch)
            _cache = new SynchMemoryCache<String, StorageObject>();
        else
            _cache = new MemoryCache<String, StorageObject>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperties()
     */
    public Map<String, StorageObject> getProperties()
    {
        if (_cache instanceof SynchMemoryCache)
            return ((SynchMemoryCache<String, StorageObject>)_cache)
                    .getStorage();
        else
            return ((MemoryCache<String, StorageObject>)_cache).getStorage();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperty(java.lang.String)
     */
    public Object getProperty(String key)
    {
        StorageObject storageObject = _cache.get(key);
        
        return storageObject != null ? storageObject.getValue() : null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#getProperty(java.lang.String,
     * java.lang.Object)
     */
    public Object getProperty(String key, Object def)
    {
        StorageObject storageObject = _cache.get(key);
        
        if (storageObject != null)
            return storageObject.getValue();
        else
            return def;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.storage.StorageProvider#removeProperty(java.lang.String)
     */
    public Object removeProperty(String key)
    {
        Object value = _cache.get(key);
        
        _cache.clearByKey(key);
        
        return value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.storage.StorageProvider#setProperty(java.lang.String,
     * com.toolsverse.storage.StorageProvider.StorageObject)
     */
    public Object setProperty(String key, StorageObject value)
    {
        _cache.put(key, value);
        
        return value;
    }
}