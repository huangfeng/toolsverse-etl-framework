/*
 * MetadataExtractor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata;

import java.sql.Connection;
import java.util.Map;

import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.driver.Driver;

/**
 * Extracts fields definitions for the data set using given sql.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface MetadataExtractor
{
    
    /**
     * Extracts fields definitions for the object or given sql.
     *
     * @param objectName the object name
     * @param connection the connection
     * @param driver the driver
     * @param sql the sql
     * @param useTypes if true populates exact field types, for example VARCHAR2(100) NOT NULL, etc. Otherwise just name and type.
     * @param keepOrder if true the fields in the map must be in the same order as in data set
     * @return the fields definitions
     */
    public Map<String, FieldDef> getMetaData(String objectName,
            Connection connection, Driver driver, String sql, boolean useTypes,
            boolean keepOrder);
    
}
