/*
 * GenericConnectionFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.config.SqlConfig;
import com.toolsverse.exception.DefaultExceptionHandler;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the ConnectionFactory interface. Connections linked to the current execution thread (and all inherited threads).
 * Sequential <code>GenericConnectionFactory#getConnection(...)</code> calls with the same parameter will return the same connection until 
 * the final <code>GenericConnectionFactory#releaseConnection(...)</code> is called.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class GenericConnectionFactory implements ConnectionFactory
{
    /**
     * The Class StoredConnection.
     */
    private class StoredConnection
    {
        
        /** The name. */
        private final String _name;
        
        /** The connection. */
        private Connection _connection = null;
        
        /** The references count. */
        private int _count = 0;
        
        /**
         * Instantiates a new stored connection.
         *
         * @param name the name
         * @param connection the connection
         */
        public StoredConnection(String name, Connection connection)
        {
            _name = name;
            
            _connection = connection;
            
            _count = 1;
        }
        
        /**
         * Adds the connection. Increments references count.
         */
        public void addConnection()
        {
            _count++;
        }
        
        /**
         * Gets the connection.
         * 
         * @return the connection
         */
        public Connection getConnection()
        {
            return _connection;
        }
        
        /**
         * Gets the name.
         * 
         * @return the name
         */
        public String getName()
        {
            return _name;
        }
        
        /**
         * Removes the connection. Decrements references count.
         * 
         * @return the new references count
         */
        public int removeConnection()
        {
            if (_count > 0)
                _count--;
            
            return _count;
        }
    }
    
    /** The map of stored connections linked to the current and inherited threads. */
    private final ThreadLocal<Map<String, StoredConnection>> _transactions = new InheritableThreadLocal<Map<String, StoredConnection>>();
    
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
        if (params == null)
            return null;
        
        Map<String, StoredConnection> connections = getConnections();
        
        StoredConnection storedConnection = connections.get(params.getName());
        
        if (storedConnection != null)
        {
            storedConnection.addConnection();
            
            return storedConnection.getConnection();
        }
        
        ConnectionProvider<ConnectionParams> connectionProvider = (ConnectionProvider<ConnectionParams>)ObjectFactory
                .instance().get(ConnectionProvider.class.getName(),
                        AliasConnectionProvider.class.getName(), true);
        
        storedConnection = new StoredConnection(params.getName(),
                connectionProvider.getConnection(params));
        
        connections.put(params.getName(), storedConnection);
        
        return storedConnection.getConnection();
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
        return getConnection(SqlConfig.instance().getAlias(name));
    }
    
    /**
     * Gets the map of named connections.
     *
     * @return the map of named connections
     */
    private Map<String, StoredConnection> getConnections()
    {
        Map<String, StoredConnection> connections = _transactions.get();
        
        if (connections == null)
        {
            connections = new HashMap<String, StoredConnection>();
            
            _transactions.set(connections);
        }
        
        return connections;
    }
    
    /**
     * Gets the stored connection.
     *
     * @param con the connection
     * @param connections the connections
     * @return the stored connection
     */
    private StoredConnection getStoredConnection(Connection con,
            Map<String, StoredConnection> connections)
    {
        for (StoredConnection storeConnection : connections.values())
        {
            if (storeConnection.getConnection().equals(con))
                return storeConnection;
        }
        
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
        if (con == null)
            return false;
        
        Map<String, StoredConnection> connections = getConnections();
        
        StoredConnection storedConnection = getStoredConnection(con,
                connections);
        
        if (storedConnection == null)
            return false;
        
        return storedConnection._count == 1;
    }
    
    /**
     * Releases connection.
     *
     * @param con the connection
     */
    private void release(Connection con)
    {
        try
        {
            ConnectionProvider<?> connectionProvider = (ConnectionProvider<?>)ObjectFactory
                    .instance().get(ConnectionProvider.class.getName(),
                            AliasConnectionProvider.class.getName(), true);
            
            connectionProvider.releaseConnection(con);
        }
        catch (Exception ex)
        {
            DefaultExceptionHandler.instance()
                    .logException(Logger.SEVERE, this,
                            EtlResource.ERROR_RELEASING_CONNECTION.getValue(),
                            ex);
        }
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
        if (con == null)
            return;
        
        Map<String, StoredConnection> connections = getConnections();
        
        StoredConnection storedConnection = getStoredConnection(con,
                connections);
        
        if (storedConnection == null)
            return;
        
        if (storedConnection.removeConnection() == 0)
        {
            connections.remove(storedConnection.getName());
            
            release(con);
        }
    }
    
}
