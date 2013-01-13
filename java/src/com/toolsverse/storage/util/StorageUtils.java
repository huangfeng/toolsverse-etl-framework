/*
 * StorageUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage.util;

import java.util.Map;

import com.toolsverse.storage.StorageProvider;
import com.toolsverse.storage.StorageProvider.StorageObject;
import com.toolsverse.util.Utils;

/**
 * StorageUtils is collection of static methods used by StorageManager.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class StorageUtils
{
    
    /**
     * Parses the text and updates storageProvider.  Expected format: key;value|key;value.
     *
     * @param storageProvider the storage provider
     * @param text the text
     */
    public static void setProperties(StorageProvider storageProvider,
            String text)
    {
        if (Utils.isNothing(text) || storageProvider == null)
            return;
        
        Map<String, StorageObject> props = storageProvider.getProperties();
        
        if (props == null || props.size() == 0)
            return;
        
        String[] pairs = text.split("\\|", -1);
        
        for (String pair : pairs)
        {
            if (Utils.isNothing(pair))
                continue;
            
            int index = pair.indexOf(";");
            
            String key = index > 0 ? pair.substring(0, index) : pair;
            
            String value = index > 0 ? pair.substring(index + 1) : null;
            
            StorageObject storageObject = props.get(key);
            
            if (storageObject != null)
            {
                storageObject.setValue(value);
                
                storageProvider.setProperty(key, storageObject);
            }
        }
    }
    
}