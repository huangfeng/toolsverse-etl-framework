/*
 * ConnectorResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.config.SystemConfig;

/**
 * Messages used by DataSetConnector
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0 
 */

public enum ConnectorResource
{
    FILE_DOES_NOT_EXIST("File does not exist"),
    FILE_NAME_HAS_WILDCARD("File name has wildcard"),
    NOT_A_DB_CONNECTION("Not a database connection"),
    UNKNOWN_CONNECTION_TYPE("Unknown connection type"),
    
    // errors
    TRANSFORMATION_ERROR_NO_SOURCE("Xsl transformation was not successfull. Source file not found."),
    TRANSFORMATION_ERROR_NO_DEST("Xsl transformation was not successfull. Destination file not defined."),
    TRANSFORMATION_ERROR_NO_XSL("Xsl transformation was not successfull. Xsl file not found."),
    VALIDATION_ERROR_DATA_SET_NULL("Data Set is null."),
    VALIDATION_ERROR_PARAMS_NULL("There are no parameters."),
    VALIDATION_ERROR_DRIVER_NULL("Driver is null."),
    VALIDATION_ERROR_DATA_SET_NO_NAME("Data Set has no Name."),
    VALIDATION_ERROR_DATA_SET_NO_FIELDS("Data Set has no Fields."),
    VALIDATION_ERROR_DATA_SET_NO_DATA("Data Set has no Data.");
    
    private String _value;
    
    ConnectorResource(String value)
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
