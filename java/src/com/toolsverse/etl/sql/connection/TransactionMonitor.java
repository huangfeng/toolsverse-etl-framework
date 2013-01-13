/*
 * TransactionMonitor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import java.sql.Connection;

import com.toolsverse.etl.core.connection.EtlConnectionFactory;

/**
 * The purpose of this class is to ensure that the transaction processes completed or, if an error occurs, to take appropriate actions.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface TransactionMonitor
{
    
    /**
     * Adds the connection.
     *
     * @param conn the connection
     * @return true, if successful
     */
    boolean addConnection(Connection conn);
    
    /**
     * Adds the file.
     *
     * @param name the file name
     * @return true, if successful
     */
    boolean addFile(String name);
    
    /**
     * Commits transaction.
     *
     * @throws Exception in case of any error
     */
    void commit()
        throws Exception;
    
    /**
     * Rollbacks transaction.
     *
     * @param ex the exception
     * @throws Exception in case of any error
     */
    void rollback(Exception ex)
        throws Exception;
    
    /**
     * Sets the connection factory.
     *
     * @param connectionFactory the new connection factory
     */
    void setConnectionFactory(EtlConnectionFactory connectionFactory);
}
