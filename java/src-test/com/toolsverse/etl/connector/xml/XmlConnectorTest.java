/*
 * XmlConnectorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.xml;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetGenerator;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * XmlConnectorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class XmlConnectorTest
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
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testConnectorStream()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xml");
        
        params.setOutputStream(output);
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xml");
        
        params.setInputStream(input);
        
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testDateFormat()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        params.setDateTimeFormat("MM/dd/yyyy HH:mm:ss");
        params.setDateFormat("MM/dd/yyyy");
        params.setTimeFormat("HH:mm:ss");
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
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
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        params.setMaxRows(2);
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(!sourceDataSet.equals(destDataSet));
        assertTrue(destDataSet.getRecordCount() == 2);
    }
    
    @Test
    public void testTransform()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        params.setXslFileName(SystemConfig.instance().getHome()
                + "dataset2webrowset.xsl");
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        params.setXslFileName(SystemConfig.instance().getHome()
                + "webrowset2dataset.xsl");
        
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testTransformStream()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        EtlConfig config = new EtlConfig();
        config.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(
                "com.toolsverse.etl.driver.MockExtendedCallableDriver", null,
                null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xml");
        
        params.setOutputStream(output);
        params.setXslFileName(SystemConfig.instance().getHome()
                + "dataset2webrowset.xsl");
        
        xmlConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xml");
        
        params.setInputStream(input);
        params.setXslFileName(SystemConfig.instance().getHome()
                + "webrowset2dataset.xsl");
        
        xmlConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
}
