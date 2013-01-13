/*
 * StorageService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage.service;

import java.util.List;
import java.util.Map;

import com.toolsverse.service.Service;
import com.toolsverse.storage.StorageProvider.StorageObject;

/**
 * The class that loads and stores properties must implement this interface.
 * 
 * @see StorageObject
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface StorageService extends Service
{
    
    /**
     * Loads all properties.
     *
     * @return the map where key is a property name and value is a StorageObject.
     * @throws Exception in case of any error
     */
    Map<String, StorageObject> load()
        throws Exception;
    
    /**
     * Loads only properties from the give key list.
     *
     * @param keys the keys
     * @return the map where key is a property name and value is a StorageObject.
     * @throws Exception in case of any error
     */
    Map<String, StorageObject> load(List<String> keys)
        throws Exception;
    
    /**
     * Stores properties.
     *
     * @param properties the properties
     * @throws Exception in case of any error
     */
    void store(Map<String, StorageObject> properties)
        throws Exception;
}
