/*
 * PersistableStorageAdapter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage;

import java.util.List;
import java.util.Map;

import com.toolsverse.service.ServiceFactory;
import com.toolsverse.storage.StorageProvider.StorageObject;
import com.toolsverse.storage.service.StorageService;

/**
 * The adapter between StorageProvider and StorageService. The particular (active) StorageService is instantiated based on the current execution 
 * mode (client, web, etc).
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class PersistableStorageAdapter
{
    
    /**
     * Gets the storage service.
     *
     * @return the storage service
     * @throws Exception in case of any error
     */
    private StorageService getStorageService()
        throws Exception
    {
        return (StorageService)ServiceFactory.getService(StorageService.class);
    }
    
    /**
     * Loads properties using active StorageService and sets them in the given StorageProvider.
     *
     * @param provider the provider
     * @throws Exception in case of any error
     */
    public void load(StorageProvider provider)
        throws Exception
    {
        StorageService service = getStorageService();
        
        Map<String, StorageObject> properties = service.load();
        
        update(provider, properties);
    }
    
    /**
     * Loads properties using given keys list and active StorageService and sets them in the given StorageProvider.
     *
     * @param provider the provider
     * @param keys the keys
     * @throws Exception in case of any error
     */
    public void load(StorageProvider provider, List<String> keys)
        throws Exception
    {
        StorageService service = getStorageService();
        
        Map<String, StorageObject> properties = service.load(keys);
        
        update(provider, properties);
    }
    
    /**
     * Stores all properties from the given StorageProvider using active StorageService.
     *
     * @param provider the provider
     * @throws Exception in case of any error
     */
    public void store(StorageProvider provider)
        throws Exception
    {
        store(provider, provider.getProperties());
    }
    
    /**
     * Stores given properties from the given StorageProvider using active StorageService.
     *
     * @param provider the provider
     * @param properties the properties
     * @throws Exception in case of any error
     */
    public void store(StorageProvider provider,
            Map<String, StorageObject> properties)
        throws Exception
    {
        if (properties == null && properties.size() == 0)
            return;
        
        StorageService service = getStorageService();
        
        service.store(properties);
        
        update(provider, properties);
    }
    
    /**
     * Updates properties using StorageProvider.
     *
     * @param provider the provider
     * @param properties the properties
     */
    private void update(StorageProvider provider,
            Map<String, StorageObject> properties)
    {
        if (properties != null)
            for (String key : properties.keySet())
            {
                provider.setProperty(key, properties.get(key));
            }
    }
    
}