/*
 * ConnectionProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverses
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.ConnectionParams;

/**
 * A strategy for obtaining JDBC connections.
 *
 * @see com.toolsverse.etl.common.ConnectionParams
 * 
 * @param <C> the generic ConnectionParams
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface ConnectionProvider<C extends ConnectionParams>
{
    
    /**
     * Gets the connection.
     *
     * @param params the ConnectionParams
     * @return the jdbc connection
     * @throws Exception in case of any error
     */
    Connection getConnection(C params)
        throws Exception;
    
    /**
     * Releases connection.
     *
     * @param con the jdbc connection
     * @throws Exception in case of any error
     */
    void releaseConnection(Connection con)
        throws Exception;
}
