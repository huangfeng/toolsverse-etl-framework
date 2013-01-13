/*
 * TextConnectorResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.text;

import com.toolsverse.config.SystemConfig;

/**
 * Message used by TextConnector.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum TextConnectorResource
{
    // errors
    VALIDATION_ERROR_DELIMITER_NOT_SPECIFIED("Delimiter not specified.");
    
    private String _value;
    
    TextConnectorResource(String value)
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
