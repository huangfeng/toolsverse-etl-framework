/*
 * TestResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.resource;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.driver.GenericJdbcDriver;

/**
 * Resource.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public enum TestResource
{
    // default context
    DEFAULT_FILTER_CONTEXT("test"),
    
    // constants
    TEST_HOME_PATH("/data-test/"),
    TEST_CONFIG_FILE_NAME("test.xml"),
    TEST_ALIAS_NAME("test database"),
    TEST_ALIAS_URL("jdbc:derby:" + SystemConfig.WORKING_PATH
            + TEST_HOME_PATH.getValue() + "javadb"),
    TEST_ALIAS_DRIVER("org.apache.derby.jdbc.EmbeddedDriver"),
    TEST_ALIAS_DRIVER_CLASS_NAME(GenericJdbcDriver.class.getName()),
    TEST_USER(""),
    TEST_PASSWORD("");
    
    private String _value;
    
    TestResource(String value)
    {
        _value = value;
    }
    
    public String getValue()
    {
        return _value;
    }
    
    @Override
    public String toString()
    {
        return _value;
    }
}
