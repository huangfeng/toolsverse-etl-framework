/*
 * Resource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.resource;

import com.toolsverse.config.SystemConfig;

/**
 * Messages used by Toolsverse framework.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public enum Resource
{
    // errors
    ERROR_GENERAL("General error."),
    ERROR_READING_LICENSE("Error reading license."),
    ERROR_READING_CLIENT_KEYSTORE("Error reading client keystore."),
    ERROR_OUT_OF_MEMORY("Memory usage low. Please consider restarting application."),
    ERROR_CLASS_NOT_FOUND("Class not found."),
    ERROR_ACCESS_MODEL("Error accessing model."),
    ERROR_POPULATE_MODEL("Error populating model."),
    ERROR_DESERIALIZE_MODEL("Error deserializing model."),
    ERROR_SERIALIZE_MODEL("Error serializing model."),
    FAILED_TO_EXECUTE_WEB_SERVICE("Failed to execute WEB service using url->%1. Response code->%2."),
    ERROR_LOCATING_WEB_SERVICE_URL("Error locating WEB service URL."),
    ERROR_INSTANTIATING_CLASS("Error instantiating class."),
    ERROR_INSTANTIATING_CLASS_CUST_EXCEPTION("Error instantiating class. Class cannot be cast to the desired interface."),
    ERROR_LOADING_PROPS("Error loading system properties."),
    ERROR_READING_RESOURCE("Error reading resource %1."),
    ERROR_CLOSING_RESOURCE("Error closing resource."),
    ERROR_DECRYPTING("Error decrypting."),
    ERROR_ENCRYPTING("Error encrypting."),
    ERROR_EXECUTING_METHOD("Error execuring method."),
    ERROR_ACCESSING_BLOB_STREAM("Can't access Blob stream."),
    WRONG_ARGUMENT_TYPE("Argument has a wrong type."),
    ARGUMENT_IS_NULL("Argument is null."),
    WRONG_ARGUMENT_VALUE("Argument has a wrong value."),
    ERROR_ADDING_CLASSPATH("Error adding resource to the classpath."),
    ERROR_LOADING_EXT("Error loading extension."),
    
    // defaults
    DEF_DATE_FORMAT("MM/dd/yyyy"),
    DEF_DATE_TIME_FORMAT("MM/dd/yyyy HH:mm:ss"),
    DEF_TIME_FORMAT("HH:mm:ss"),
    
    // messages
    WEB_SERVICES_READY_TO_USE("WEB services are configured and ready to use.");
    
    private String _value;
    
    Resource(String value)
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
