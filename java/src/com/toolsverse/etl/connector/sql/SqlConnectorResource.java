/*
 * SqlConnectorResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.sql;

import com.toolsverse.config.SystemConfig;

/**
 * Messages used by SqlConnector
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum SqlConnectorResource
{
    // errors
    VALIDATION_ERROR_NO_METADATA_FOUND("No Metadata found.");
    
    private String _value;
    
    SqlConnectorResource(String value)
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
    
    @Override
    public String toString()
    {
        return _value;
    }
}
