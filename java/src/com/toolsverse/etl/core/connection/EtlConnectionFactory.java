/*
 * EtlConnectionFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.sql.connection.ConnectionFactory;

/**
 * The ETL extension of ConnectionFactory interface. The idea is to have a class which is responsible for creating
 * and closing of the database connections used by ETl framework. Also, it must maintain lists of connections and aliases
 * and provide lookup methods like "find connection by alias" and vise versa. 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface EtlConnectionFactory extends ConnectionFactory
{
    
    /**
     * Creates a new connection if needed and adds it to the map of named connections.
     *
     * @param con the connection
     * @param params the ConnectionParams
     * @param name the name
     * @param defName the default name
     * @return the connection
     * @throws Exception in case of any error
     */
    Connection addConnection(Connection con, ConnectionParams params,
            String name, String defName)
        throws Exception;
    
    /**
     * Gets the connection params for the given connection.
     *
     * @param conn the connection
     * @return the connection params
     */
    ConnectionParams getConnectionParams(Connection conn);
    
    /**
     * Gets the connection params by the given name.
     *
     * @param name the name
     * @return the connection params
     */
    ConnectionParams getConnectionParams(String name);
    
    /**
     * Releases all connections.
     */
    void releaseConnections();
}
