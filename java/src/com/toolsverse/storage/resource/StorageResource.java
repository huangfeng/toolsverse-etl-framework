/*
 * StorageResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.storage.resource;

import com.toolsverse.config.SystemConfig;

/**
 * The messages used by the storage framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum StorageResource
{
    // messages
    
    CANNOT_INIT_NO_PROVIDER(
            "Cannot initialize storage manager. The storage provider not found.");
    
    /** The _value. */
    private String _value;
    
    /**
     * Instantiates a new security resource.
     * 
     * @param value
     *            the value
     */
    StorageResource(String value)
    {
        _value = value;
    }
    
    /**
     * Gets the value of this enum constant.
     * 
     * @return the value
     */
    public String getValue()
    {
        return SystemConfig.instance().getObjectProperty(this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString()
    {
        return _value;
    }
}
