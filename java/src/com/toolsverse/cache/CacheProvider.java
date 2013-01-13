/*
 * CacheProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.cache;

import java.io.Serializable;

/**
 * Implement this interface if you want you class to be a cache provider. All default cache implementation also a cache providers. 
 * 
 * @see com.toolsverse.cache.Cache
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface CacheProvider<K, V> extends Serializable
{
    
    /**
     * Gets the cache.
     *
     * @return the cache
     */
    Cache<K, V> getCache();
}
