/*
 * ConnectionFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.sql.config.SqlConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * ConnectionFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ConnectionFactoryTest
{
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        SystemConfig.instance().bind(ConnectionFactory.class.getName(),
                GenericConnectionFactory.class.getName());
    }
    
    @Test
    public void testGetConnectionByName()
        throws Exception
    {
        ConnectionFactory connectionFactory = (ConnectionFactory)ObjectFactory
                .instance().get(ConnectionFactory.class.getName());
        
        Connection con = null;
        
        try
        {
            con = connectionFactory.getConnection("system");
            
            assertNotNull(con);
        }
        finally
        {
            connectionFactory.releaseConnection(con);
        }
    }
    
    @Test
    public void testGetConnectionByParams()
        throws Exception
    {
        ConnectionFactory connectionFactory = (ConnectionFactory)ObjectFactory
                .instance().get(ConnectionFactory.class.getName());
        
        Connection con = null;
        
        try
        {
            con = connectionFactory.getConnection(SqlConfig.instance()
                    .getAlias("system"));
            
            assertNotNull(con);
        }
        finally
        {
            connectionFactory.releaseConnection(con);
        }
    }
    
    @Test
    public void testInit()
        throws Exception
    {
        ConnectionFactory connectionFactory = (ConnectionFactory)ObjectFactory
                .instance().get(ConnectionFactory.class.getName());
        
        assertTrue(connectionFactory instanceof GenericConnectionFactory);
    }
    
    @Test
    public void testIsReadyToCommit()
        throws Exception
    {
        ConnectionFactory connectionFactory = (ConnectionFactory)ObjectFactory
                .instance().get(ConnectionFactory.class.getName());
        
        Connection con = connectionFactory.getConnection(SqlConfig.instance()
                .getAlias("system"));
        
        assertTrue(connectionFactory.isReadyToCommit(con));
        
        Connection con2 = connectionFactory.getConnection(SqlConfig.instance()
                .getAlias("system"));
        
        assertTrue(con == con2);
        
        assertTrue(!connectionFactory.isReadyToCommit(con));
        assertTrue(!connectionFactory.isReadyToCommit(con2));
        
        connectionFactory.releaseConnection(con2);
        
        assertTrue(connectionFactory.isReadyToCommit(con));
        
        connectionFactory.releaseConnection(con);
    }
    
    @Test
    public void testReleaseConnection()
        throws Exception
    {
        ConnectionFactory connectionFactory = (ConnectionFactory)ObjectFactory
                .instance().get(ConnectionFactory.class.getName());
        
        Connection con = connectionFactory.getConnection(SqlConfig.instance()
                .getAlias("system"));
        
        assertNotNull(con);
        
        connectionFactory.releaseConnection(con);
        
        int i = 0;
        
        while (i < 100 && !con.isClosed())
        {
            Thread.sleep(100);
            
            i++;
        }
        
        assertTrue(con.isClosed());
    }
}