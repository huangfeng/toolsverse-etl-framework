/*
 * DummyConnectionFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.ConnectionParamsProvider;

/**
 * DummyConnectionFactory always returns connection which was passed to it in the constructor. It never release connection.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class DummyConnectionFactory implements ConnectionFactory,
        ConnectionParamsProvider<Alias>
{
    
    /** The _connection. */
    private Connection _connection;
    
    /**
     * Instantiates a new dummy connection factory.
     *
     * @param connection the connection
     */
    public DummyConnectionFactory(Connection connection)
    {
        _connection = connection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionFactory#getConnection(com
     * .toolsverse.etl.common.ConnectionParams)
     */
    public Connection getConnection(ConnectionParams params)
        throws Exception
    {
        return _connection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionFactory#getConnection(java
     * .lang.String)
     */
    public Connection getConnection(String name)
        throws Exception
    {
        return _connection;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.ConnectionParamsProvider#getConnectionParams()
     */
    public Alias getConnectionParams()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionFactory#isReadyToCommit(java
     * .sql.Connection)
     */
    public boolean isReadyToCommit(Connection con)
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionFactory#releaseConnection
     * (java.sql.Connection)
     */
    public void releaseConnection(Connection con)
    {
        
    }
}
