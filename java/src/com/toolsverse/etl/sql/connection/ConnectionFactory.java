/*
 * ConnectionFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.ConnectionParams;

/**
 * ConnectionFactory provides an interface for getting JDBC connection.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface ConnectionFactory
{
    
    /**
     * Gets the connection by ConnectionParams.
     *
     * @param params the ConnectionParams
     * @return the connection
     * @throws Exception in case of any error
     */
    Connection getConnection(ConnectionParams params)
        throws Exception;
    
    /**
     * Gets the connection by name.
     *
     * @param name the name
     * @return the connection
     * @throws Exception in case of any error
     */
    Connection getConnection(String name)
        throws Exception;
    
    /**
     * Checks if connection is ready to commit. It usually means that the connection currently referenced just from one place.
     *
     * @param con the connection
     * @return true, if is ready to commit
     */
    boolean isReadyToCommit(Connection con);
    
    /**
     * Releases connection.
     *
     * @param con the connection
     */
    void releaseConnection(Connection con);
}
