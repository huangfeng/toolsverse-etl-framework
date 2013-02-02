/*
 * ConnectionFactoryWrapper.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.log.Logger;

/**
 * The implementation of the ConnectionFactory which always returns the same connection. The connection is passed as a parameter in the constructor. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ConnectionFactoryWrapper implements ConnectionFactory
{
    
    /** The connection. */
    private Connection _connection;
    
    /** The "close when done" flag. */
    private boolean _closeWhenDone;
    
    /**
     * Instantiates a new connection factory wrapper.
     *
     * @param connection the connection
     * @param closeWhenDone the "close when done" flag. If true the connection will be closed when <code>releaseConnection</code> is called.
     */
    public ConnectionFactoryWrapper(Connection connection, boolean closeWhenDone)
    {
        _connection = connection;
        _closeWhenDone = closeWhenDone;
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
        return true;
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
        if (_closeWhenDone && _connection != null)
            try
            {
                try
                {
                    _connection.rollback();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.INFO, this,
                            Resource.ERROR_GENERAL.getValue(), ex);
                }
                finally
                {
                    _connection.close();
                }
                
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, this,
                        Resource.ERROR_GENERAL.getValue(), ex);
            }
        
    }
}
