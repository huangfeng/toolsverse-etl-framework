/*
 * SessionConnectionProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.ConnectionParams;

/**
 * The ConnectionProvider which links connections to the current session. Usually used by the server side code together with http session.  
 *
 * @see com.toolsverse.etl.sql.connection.ConnectionProvider
 *
 * @param <C> the generic ConnectionParams
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface SessionConnectionProvider<C extends ConnectionParams> extends
        ConnectionProvider<C>
{
    
    /**
     * Gets the connection.
     *
     * @param params the ConnectionParams
     * @param isNew the "is new" flag. If true returns new jdbc connection, otherwise - connection cached on the current session. <code>params<code> 
     * used as a key for the cache 
     * 
     * @return the jdbc connection
     * @throws Exception in case of any error
     */
    Connection getConnection(C params, boolean isNew)
        throws Exception;
    
    /**
     * Releases connection.
     *
     * @param params the ConnectionParams
     * @throws Exception in case of any error
     */
    void releaseConnection(C params)
        throws Exception;
}
