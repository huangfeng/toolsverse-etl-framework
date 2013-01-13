/*
 * EtlConnectionFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.connection;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.sql.Connection;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * EtlConnectionFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlConnectionFactoryTest
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
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    private Alias getDbAlias()
    {
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        return alias;
    }
    
    private Alias getXmlAlias()
    {
        Alias alias = new Alias();
        alias.setName("source_xml");
        alias.setUrl(SystemConfig.instance().getDataFolderName() + "SOURCE.xml");
        alias.setConnectorClassName(XmlConnector.class.getName());
        
        return alias;
    }
    
    @Test
    public void testAddConnection()
        throws Exception
    {
        EtlConnectionFactory etlConnectionFactory = null;
        
        try
        {
            etlConnectionFactory = (EtlConnectionFactory)ObjectFactory
                    .instance().get(EtlConnectionFactoryImpl.class.getName());
            
            Alias alias = getDbAlias();
            
            etlConnectionFactory.addConnection(null, alias, "test", null);
            
            Connection con = etlConnectionFactory.getConnection("test");
            
            assertNotNull(con);
            
            ConnectionParams connectionParams = etlConnectionFactory
                    .getConnectionParams(con);
            
            assertTrue(connectionParams == alias);
            
            connectionParams = etlConnectionFactory.getConnectionParams("test");
            
            assertTrue(connectionParams == alias);
        }
        finally
        {
            if (etlConnectionFactory != null)
                etlConnectionFactory.releaseConnections();
        }
    }
    
    @Test
    public void testAddDestConnection()
        throws Exception
    {
        EtlConnectionFactory etlConnectionFactory = null;
        
        try
        {
            etlConnectionFactory = (EtlConnectionFactory)ObjectFactory
                    .instance().get(EtlConnectionFactoryImpl.class.getName());
            
            Alias dest = getDbAlias();
            
            etlConnectionFactory.addConnection(null, dest, null,
                    EtlConfig.DEST_CONNECTION_NAME);
            
            Connection con = etlConnectionFactory
                    .getConnection(EtlConfig.DEST_CONNECTION_NAME);
            
            assertNotNull(con);
            
            ConnectionParams connectionParams = etlConnectionFactory
                    .getConnectionParams(con);
            
            assertTrue(connectionParams == dest);
            
            connectionParams = etlConnectionFactory
                    .getConnectionParams(EtlConfig.DEST_CONNECTION_NAME);
            
            assertTrue(connectionParams == dest);
        }
        finally
        {
            if (etlConnectionFactory != null)
                etlConnectionFactory.releaseConnections();
        }
    }
    
    @Test
    public void testAddSourceConnection()
        throws Exception
    {
        EtlConnectionFactory etlConnectionFactory = null;
        
        try
        {
            etlConnectionFactory = (EtlConnectionFactory)ObjectFactory
                    .instance().get(EtlConnectionFactoryImpl.class.getName());
            
            Alias source = getDbAlias();
            
            etlConnectionFactory.addConnection(null, source, null,
                    EtlConfig.SOURCE_CONNECTION_NAME);
            
            Connection con = etlConnectionFactory
                    .getConnection(EtlConfig.SOURCE_CONNECTION_NAME);
            
            assertNotNull(con);
            
            ConnectionParams connectionParams = etlConnectionFactory
                    .getConnectionParams(con);
            
            assertTrue(connectionParams == source);
            
            connectionParams = etlConnectionFactory
                    .getConnectionParams(EtlConfig.SOURCE_CONNECTION_NAME);
            
            assertTrue(connectionParams == source);
        }
        finally
        {
            if (etlConnectionFactory != null)
                etlConnectionFactory.releaseConnections();
        }
    }
    
    @Test
    public void testTipicalScenario()
        throws Exception
    {
        EtlConnectionFactory etlConnectionFactory = null;
        
        try
        {
            etlConnectionFactory = (EtlConnectionFactory)ObjectFactory
                    .instance().get(EtlConnectionFactoryImpl.class.getName());
            
            Alias source = getDbAlias();
            
            etlConnectionFactory.addConnection(null, source, null,
                    EtlConfig.SOURCE_CONNECTION_NAME);
            
            Alias dest = getDbAlias();
            
            etlConnectionFactory.addConnection(null, dest, null,
                    EtlConfig.DEST_CONNECTION_NAME);
            
            Alias alias = getDbAlias();
            
            etlConnectionFactory.addConnection(null, alias, "test", null);
            
            Alias xmlAlias = getXmlAlias();
            
            etlConnectionFactory.addConnection(null, xmlAlias, "xml", null);
            
            Connection con = etlConnectionFactory
                    .getConnection(EtlConfig.SOURCE_CONNECTION_NAME);
            assertNotNull(con);
            
            con = etlConnectionFactory
                    .getConnection(EtlConfig.DEST_CONNECTION_NAME);
            assertNotNull(con);
            
            con = etlConnectionFactory.getConnection("test");
            assertNotNull(con);
            
            con = etlConnectionFactory.getConnection("xml");
            assertTrue(con == null);
            
            ConnectionParams connectionParams = etlConnectionFactory
                    .getConnectionParams("xml");
            assertTrue(connectionParams == xmlAlias);
            
            etlConnectionFactory.releaseConnections();
            
            con = etlConnectionFactory
                    .getConnection(EtlConfig.SOURCE_CONNECTION_NAME);
            assertTrue(con == null);
            
            con = etlConnectionFactory
                    .getConnection(EtlConfig.DEST_CONNECTION_NAME);
            assertTrue(con == null);
            
            con = etlConnectionFactory.getConnection("test");
            assertTrue(con == null);
            
            con = etlConnectionFactory.getConnection("xml");
            assertTrue(con == null);
            
            connectionParams = etlConnectionFactory.getConnectionParams("xml");
            assertTrue(con == null);
        }
        finally
        {
            if (etlConnectionFactory != null)
                etlConnectionFactory.releaseConnections();
        }
    }
}