/*
 * SqlConnectorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.sql;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.connector.text.TextConnector;
import com.toolsverse.etl.connector.text.TextConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.GenericJdbcDriver;
import com.toolsverse.etl.sql.connection.AliasConnectionProvider;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * SqlConnectorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlConnectorTest
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
    
    @AfterClass
    public static void tearDown()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "source.*");
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getScriptsFolder(), "test.*");
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getScriptsFolder(), "source.*");
    }
    
    @Test
    public void testConnector()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        AliasConnectionProvider connectionProvider = new AliasConnectionProvider();
        
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        
        DataSet sourceDataSet = new DataSet();
        sourceDataSet.setName("source");
        
        try
        {
            con = connectionProvider.getConnection(alias
                    .getConnectionParams());
            
            st = con.prepareStatement("select * from source");
            
            rs = st.executeQuery();
            
            SqlConnectorParams sqlParams = new SqlConnectorParams(rs, config,
                    true, -1);
            
            SqlConnector sqlConnector = new SqlConnector();
            
            sqlConnector.populate(sqlParams, sourceDataSet, driver);
            
            assertTrue(sourceDataSet.getFieldCount() > 0);
            assertTrue(sourceDataSet.getRecordCount() > 0);
            
            TextConnector textConnector = new TextConnector();
            
            TextConnectorParams textParams = new TextConnectorParams(config,
                    true, -1, "|", true);
            
            textConnector.persist(textParams, sourceDataSet, driver);
            
            DataSet destDataSet = new DataSet();
            destDataSet.setName("source");
            
            textConnector.populate(textParams, destDataSet, driver);
            
            assertTrue(sourceDataSet.equals(destDataSet));
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            connectionProvider.releaseConnection(con);
        }
        
    }
    
    @Test
    public void testMaxRows()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        AliasConnectionProvider connectionProvider = new AliasConnectionProvider();
        
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        
        DataSet sourceDataSet = new DataSet();
        sourceDataSet.setName("source");
        
        try
        {
            con = connectionProvider.getConnection(alias
                    .getConnectionParams());
            
            st = con.prepareStatement("select * from source");
            
            rs = st.executeQuery();
            
            SqlConnectorParams sqlParams = new SqlConnectorParams(rs, config,
                    true, -1);
            
            SqlConnector sqlConnector = new SqlConnector();
            
            sqlParams.setMaxRows(2);
            sqlConnector.populate(sqlParams, sourceDataSet, driver);
            
            assertTrue(sourceDataSet.getFieldCount() > 0);
            assertTrue(sourceDataSet.getRecordCount() == 2);
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            connectionProvider.releaseConnection(con);
        }
        
    }
    
    @Test
    public void testPersist()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        Alias alias = new Alias();
        alias.setName(TestResource.TEST_ALIAS_NAME.getValue());
        alias.setUrl(TestResource.TEST_ALIAS_URL.getValue());
        alias.setJdbcDriverClass(TestResource.TEST_ALIAS_DRIVER.getValue());
        alias.setUserId(TestResource.TEST_USER.getValue());
        alias.setPassword(TestResource.TEST_PASSWORD.getValue());
        
        AliasConnectionProvider connectionProvider = new AliasConnectionProvider();
        
        Connection con = null;
        PreparedStatement st = null;
        ResultSet rs = null;
        
        DataSet dataSet = new DataSet();
        dataSet.setName("source");
        
        try
        {
            con = connectionProvider.getConnection(alias
                    .getConnectionParams());
            
            st = con.prepareStatement("select * from source");
            
            rs = st.executeQuery();
            
            SqlConnectorParams sqlParams = new SqlConnectorParams(rs, config,
                    true, -1);
            
            SqlConnector sqlConnector = new SqlConnector();
            
            sqlConnector.populate(sqlParams, dataSet, driver);
            
            assertTrue(dataSet.getFieldCount() > 0);
            assertTrue(dataSet.getRecordCount() > 0);
            
            sqlConnector.persist(sqlParams, dataSet, driver);
            
            File file = new File(sqlParams.getFileName(dataSet.getName()));
            assertTrue(file.exists());
            assertTrue(file.length() > 0);
        }
        finally
        {
            SqlUtils.cleanUpSQLData(st, null, this);
            connectionProvider.releaseConnection(con);
        }
        
    }
    
}
