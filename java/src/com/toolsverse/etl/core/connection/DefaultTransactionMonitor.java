/*
 * DefaultTransactionMonitor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.connection;

import java.sql.Connection;
import java.util.Vector;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the TransactionMonitor interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DefaultTransactionMonitor implements TransactionMonitor
{
    
    /** The connections. */
    private Vector<Connection> _connections;
    
    /** The files. */
    private Vector<String> _files;
    
    /** The connection factory. */
    private EtlConnectionFactory _connectionFactory;
    
    /**
     * Instantiates a new default transaction monitor.
     */
    public DefaultTransactionMonitor()
    {
        _connectionFactory = null;
        _connections = new Vector<Connection>();
        _files = new Vector<String>();
    }
    
    /**
     * Instantiates a new default transaction monitor.
     *
     * @param connectionFactory the connection factory
     */
    public DefaultTransactionMonitor(EtlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
        
        _connections = new Vector<Connection>();
        _files = new Vector<String>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.TransactionMonitor#addConnection(java
     * .sql.Connection)
     */
    public boolean addConnection(Connection conn)
    {
        if (conn == null || _connections.contains(conn))
            return false;
        
        _connections.add(conn);
        
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.TransactionMonitor#addFile(java.lang
     * .String)
     */
    public boolean addFile(String name)
    {
        if (!Utils.isNothing(name) && _files.contains(name))
            return false;
        
        _files.add(name);
        
        return true;
    }
    
    /**
     * Cleans up.
     */
    private void cleanUp()
    {
        _connections.clear();
        _files.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.sql.connection.TransactionMonitor#commit()
     */
    public void commit()
        throws Exception
    {
        try
        {
            if (_connections.size() == 0)
                return;
            
            Alias alias = null;
            
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.COMMIT_MSG.getValue());
            
            for (Connection con : _connections)
            {
                if (_connectionFactory != null)
                    alias = (Alias)_connectionFactory.getConnectionParams(con);
                else
                    alias = null;
                
                if (alias == null || alias.isLocalCommit())
                {
                    con.commit();
                }
            }
        }
        finally
        {
            cleanUp();
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.TransactionMonitor#rollback(java.lang
     * .Exception)
     */
    public void rollback(Exception ex)
        throws Exception
    {
        try
        {
            for (String fName : _files)
                FileUtils.deleteFile(fName);
            
            if (_connections.size() == 0)
                return;
            
            if (ex != null)
                Logger.log(
                        Logger.INFO,
                        EtlLogger.class,
                        EtlResource.EXCEPTION_MSG.getValue()
                                + Utils.getStackTraceAsString(ex));
            else
                Logger.log(Logger.INFO, EtlLogger.class,
                        EtlResource.EXCEPTION_MSG.getValue());
            
            for (Connection con : _connections)
            {
                con.rollback();
            }
        }
        finally
        {
            cleanUp();
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.sql.connection.TransactionMonitor#setConnectionFactory
     * (com.toolsverse.etl.core.connection.EtlConnectionFactory)
     */
    public void setConnectionFactory(EtlConnectionFactory connectionFactory)
    {
        _connectionFactory = connectionFactory;
    }
    
}
