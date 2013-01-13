/*
 * ConnectionParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

/**
 * Basic interface for connection parameters.
 * 
 * @see com.toolsverse.etl.common.Alias
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ConnectionParams
{
    
    /**
     * Copy.
     *
     * @param id the unique id
     * @return the connection params
     */
    ConnectionParams copy(String id);
    
    /**
     * Gets the name.
     * 
     * @return the name
     */
    String getName();
    
    /**
     * Gets the unique id.
     * 
     * @return the unique id
     */
    String getUniqueId();
    
    /**
     * Gets the unique property.
     * 
     * @return the unique property
     */
    String getUniqueProperty();
    
    /**
     * Checks if it is a database connection.
     * 
     * @return true, if it is a database connection
     */
    boolean isDbConnection();
}
