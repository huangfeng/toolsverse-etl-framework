/*
 * FileConnectorResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.config.SystemConfig;

/**
 * Messages and errors used by file-based DataSetConnectors.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum FileConnectorResource
{
    // messages
    
    /** The file persisted. */
    FILE_PERSISTED("File %1 was created."),
    
    FILES_PERSISTED("%1 files were created."),
    
    /** The file populated. */
    FILE_POPULATED("File %1 was loaded."),
    
    // errorss
    /** The validation error file doesn exist. */
    VALIDATION_ERROR_FILE_DOESN_EXIST("File does not exist."),
    
    /** The validation error file name not specified. */
    VALIDATION_ERROR_FILE_NAME_NOT_SPECIFIED("File Name not specified.");
    
    /** The value. */
    private String _value;
    
    /**
     * Instantiates a new FileConnectorResource.
     *
     * @param value the value
     */
    FileConnectorResource(String value)
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
