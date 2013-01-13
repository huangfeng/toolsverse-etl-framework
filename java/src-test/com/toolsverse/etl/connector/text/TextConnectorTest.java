/*
 * TextConnectorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.text;

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
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.connector.xml.XmlConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.GenericJdbcDriver;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * TextConnectorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TextConnectorTest
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
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), ";", true);
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.txt");
        
        params.setOutputStream(output);
        params.setFirstRowData(false);
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.txt");
        
        params.setInputStream(input);
        params.setFirstRowData(false);
        
        textConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testConnectorDelFormat()
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
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), ";", true);
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.txt");
        
        params.setOutputStream(output);
        params.setFirstRowData(false);
        params.setCharSeparator("\"");
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.txt");
        
        params.setInputStream(input);
        params.setFirstRowData(false);
        
        textConnector.populate(params, destDataSet, driver);
        
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
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), ";", true);
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        textConnector.populate(params, destDataSet, driver);
        
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
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), ";", true);
        params.setDateTimeFormat("MM/dd/yyyy HH:mm:ss");
        params.setDateFormat("MM/dd/yyyy");
        params.setTimeFormat("HH:mm:ss");
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        textConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testFixedLength()
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
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), "|", true);
        params.setFields("5|20|20|20|20|20|20");
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        textConnector.populate(params, destDataSet, driver);
        
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
        
        Driver driver = etlFactory.getDriver(GenericJdbcDriver.class.getName(),
                null, null);
        
        assertNotNull(driver);
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), ";", true);
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        params.setMaxRows(2);
        textConnector.populate(params, destDataSet, driver);
        
        assertTrue(!sourceDataSet.equals(destDataSet));
        assertTrue(destDataSet.getRecordCount() == 2);
    }
    
    @Test
    public void testMetadataAndHeadLess()
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
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), "|", true);
        params.setFirstRowData(true);
        params.setPersistMetaData(false);
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        textConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testMetadataLess()
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
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep(), "|", true);
        params.setFirstRowData(false);
        params.setPersistMetaData(false);
        
        textConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        textConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testXml2Text()
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
        
        DataSet sourceDataSet = DataSetGenerator.getTestDataSet();
        assertNotNull(sourceDataSet);
        
        sourceDataSet.setName("test");
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams xmlParams = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        xmlConnector.persist(xmlParams, sourceDataSet, driver);
        
        xmlConnector.populate(xmlParams, sourceDataSet, driver);
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams textParams = new TextConnectorParams(config, false,
                config.getLogStep(), SqlUtils.DEFAULT_DELIMITER, true);
        
        DataSet destDataSet = DataSetGenerator.getTestDataSet();
        destDataSet.setName("test");
        
        textConnector.persist(textParams, destDataSet, driver);
        
        textConnector.populate(textParams, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
}
