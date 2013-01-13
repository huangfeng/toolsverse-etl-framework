/*
 * PersistableStorage.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage;

import java.util.List;
import java.util.Map;

import com.toolsverse.storage.StorageProvider.StorageObject;

/**
 * The interface for the classes which are used as a storage for the properties. The possible implementations are: 
 * file based storage, database storage, HTML5 storage,etc. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface PersistableStorage
{
    
    /**
     * Initializes the storage.
     *
     * @throws Exception in case of any error
     */
    void init()
        throws Exception;
    
    /**
     * Loads properties.
     *
     * @throws Exception in case of any error
     */
    void load()
        throws Exception;
    
    /**
     * Loads only properties which belong to the given keys list.
     *
     * @param keys the keys
     * @throws Exception in case of any error
     */
    void load(List<String> keys)
        throws Exception;
    
    /**
     * Stores properties.
     *
     * @throws Exception in case of any error
     */
    void store()
        throws Exception;
    
    /**
     * Stores only given properties.
     *
     * @param properties the properties
     * @throws Exception in case of any error
     */
    void store(Map<String, StorageObject> properties)
        throws Exception;
    
}