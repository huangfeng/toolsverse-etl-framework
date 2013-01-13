/*
 * MetadataProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.util.Map;

import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.driver.Driver;

/**
 * This interface defines methods to retrieve field definitions from database objects such as tables, views, synonyms, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface MetadataProvider
{
    /**
     * Gets the database object name.
     *
     * @return the database object name
     */
    String getDatabaseObjectName();
    
    /**
     * Gets the driver.
     *
     * @return the driver
     */
    Driver getDriver();
    
    /**
     * Gets the field definitions for the object.
     *
     * @param objectName the object name
     * @return the meta data
     */
    Map<String, FieldDef> getMetaData(String objectName);
    
    /**
     * Checks if provider is ready.
     *
     * @return true, if provider is ready
     */
    boolean isReady();
}