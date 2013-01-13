/*
 * AliasSessionConnectionProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.storage.StorageManager;
import com.toolsverse.storage.StorageProvider;
import com.toolsverse.storage.StorageProvider.StorageObject;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * The implementation of the SessionConnectionProvider which uses Alias as a ConnectionParams.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class AliasSessionConnectionProvider implements
        SessionConnectionProvider<Alias>
{
    
    /** The connection provider. */
    private ConnectionProvider<Alias> _provider;
    
    /**
     * Instantiates a new alias session connection provider.
     */
    @SuppressWarnings("unchecked")
    public AliasSessionConnectionProvider()
    {
        _provider = (ConnectionProvider<Alias>)ObjectFactory.instance().get(
                ConnectionProvider.class.getName(),
                AliasConnectionProvider.class.getName(), true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionProvider#getConnection(com
     * .toolsverse.etl.common.ConnectionParams)
     */
    public Connection getConnection(Alias params)
        throws Exception
    {
        return getConnection(params, true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.SessionConnectionProvider#getConnection
     * (com.toolsverse.etl.common.ConnectionParams, boolean)
     */
    public Connection getConnection(Alias params, boolean isNew)
        throws Exception
    {
        Object value = StorageManager.instance().getProperty(
                params.getUniqueId());
        
        Connection con = value instanceof Connection ? (Connection)value : null;
        
        if (con != null || !isNew)
            return con;
        
        con = _provider.getConnection(params);
        
        if (con != null)
        {
            StorageManager.instance().setProperty(params.getUniqueId(),
                    new StorageObject(StorageProvider.SESSION, con));
        }
        
        return con;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.SessionConnectionProvider#releaseConnection
     * (com.toolsverse.etl.common.ConnectionParams)
     */
    public void releaseConnection(Alias params)
        throws Exception
    {
        Connection con = getConnection(params, false);
        
        if (con != null)
        {
            StorageManager.instance().removeProperty(params.getUniqueId());
            
            releaseConnection(con);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionProvider#releaseConnection
     * (java.sql.Connection)
     */
    public void releaseConnection(Connection con)
        throws Exception
    {
        _provider.releaseConnection(con);
    }
}
