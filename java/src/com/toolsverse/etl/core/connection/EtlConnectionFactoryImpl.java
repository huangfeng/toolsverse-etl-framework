/*
 * EtlConnectionFactoryImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.connection;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.sql.connection.AliasConnectionProvider;
import com.toolsverse.etl.sql.connection.ConnectionProvider;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the EtlConnectionFactory interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlConnectionFactoryImpl implements EtlConnectionFactory
{
    
    /** The connections. */
    transient private ListHashMap<String, Connection> _connections;
    
    /** The map where key is an connection and value is a ConnectionParams. */
    transient private Map<Connection, ConnectionParams> _aliasConnections;
    
    /** The aliases. */
    transient private Map<String, ConnectionParams> _aliases;
    
    /**
     * Instantiates a new etl connection factory.
     */
    public EtlConnectionFactoryImpl()
    {
        _connections = null;
        _aliasConnections = null;
        _aliases = null;
    }
    
    /**
     * Adds the alias.
     *
     * @param name the name
     * @param params the ConnectionParams
     */
    private void addAlias(String name, ConnectionParams params)
    {
        if (params == null)
            return;
        
        if (_aliases == null)
            _aliases = new HashMap<String, ConnectionParams>();
        
        _aliases.put(name, params);
    }
    
    /**
     * Adds the <code>params</code> to the map where keys are connections and values are ConnectionParams.
     *
     * @param connection the connection
     * @param params the ConnectionParams
     */
    private void addAliasConnection(Connection connection,
            ConnectionParams params)
    {
        if (params == null || connection == null)
            return;
        
        if (_aliasConnections == null)
            _aliasConnections = new HashMap<Connection, ConnectionParams>();
        
        _aliasConnections.put(connection, params);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.connection.EtlConnectionFactory#addConnection
     * (java.sql.Connection, com.toolsverse.etl.common.ConnectionParams,
     * java.lang.String, java.lang.String)
     */
    public Connection addConnection(Connection con, ConnectionParams params,
            String name, String defName)
        throws Exception
    {
        if (Utils.isNothing(name) && Utils.isNothing(defName))
            return null;
        
        name = !Utils.isNothing(name) ? name : defName;
        
        if (con == null)
            con = getConnection(params);
        
        addConnection(name, con, params);
        
        return con;
    }
    
    /**
     * Adds the connection to the map of named connections and params to the map of named ConnectionParams.
     *
     * @param name the name
     * @param connection the connection
     * @param params the params
     */
    private void addConnection(String name, Connection connection,
            ConnectionParams params)
    {
        if (_connections == null)
            _connections = new ListHashMap<String, Connection>();
        
        if (connection != null)
            _connections.put(name, connection);
        
        addAlias(name, params);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.ConnectionFactory#getConnection(com
     * .toolsverse.etl.common.ConnectionParams)
     */
    @SuppressWarnings("unchecked")
    public Connection getConnection(ConnectionParams params)
        throws Exception
    {
        if (params == null || !params.isDbConnection())
            return null;
        
        ConnectionProvider<ConnectionParams> connectionProvider = (ConnectionProvider<ConnectionParams>)ObjectFactory
                .instance().get(ConnectionProvider.class.getName(),
                        AliasConnectionProvider.class.getName(), true);
        
        Connection con = connectionProvider.getConnection(params);
        
        addAliasConnection(con, params);
        
        return con;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.connection.EtlConnectionFactory#getConnection
     * (java.lang.String)
     */
    public Connection getConnection(String name)
    {
        if (_connections == null || Utils.isNothing(name))
            return null;
        
        return _connections.get(name);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.connection.EtlConnectionFactory#getConnectionParams
     * (java.sql.Connection)
     */
    public ConnectionParams getConnectionParams(Connection conn)
    {
        if (_aliasConnections == null)
            return null;
        
        return _aliasConnections.get(conn);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.connection.EtlConnectionFactory#getConnectionParams
     * (java.lang.String)
     */
    public ConnectionParams getConnectionParams(String name)
    {
        if (_aliases == null)
            return null;
        
        return _aliases.get(name);
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
        return con != null;
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
        if (con != null)
            try
            {
                ConnectionParams params = getConnectionParams(con);
                
                con.commit();
                
                con.close();
                
                if (params != null)
                {
                    _aliasConnections.remove(con);
                    
                    _aliases.remove(params.getName());
                }
            }
            catch (Exception e)
            {
                Logger.log(Logger.SEVERE, this,
                        Resource.ERROR_GENERAL.getValue(), e);
            }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.connection.EtlConnectionFactory#releaseConnections
     * ()
     */
    public void releaseConnections()
    {
        if (_connections != null)
        {
            for (int i = 0; i < _connections.size(); i++)
            {
                Connection con = _connections.get(i);
                releaseConnection(con);
            }
            
            _connections.clear();
        }
        
        if (_aliases != null)
            _aliases.clear();
    }
}
