/*
 * ExcelConnectorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.excel;

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
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * ExcelConnectorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ExcelConnectorTest
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
    public void testExcelConnector()
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
        
        ExcelConnector excelConnector = new ExcelConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelConnectorStream()
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
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xls");
        
        ExcelConnector excelConnector = new ExcelConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        params.setOutputStream(output);
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xls");
        
        params.setInputStream(input);
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelConnectorStreamXlsx()
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
        
        FileOutputStream output = new FileOutputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xlsx");
        
        ExcelXlsxConnector excelConnector = new ExcelXlsxConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        params.setOutputStream(output);
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        FileInputStream input = new FileInputStream(SystemConfig.instance()
                .getDataFolderName() + "test.xlsx");
        
        params.setInputStream(input);
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelConnectorXlsx()
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
        
        ExcelXlsxConnector excelConnector = new ExcelXlsxConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelDateFormats()
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
        
        ExcelConnector excelConnector = new ExcelConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        params.setDateTimeFormat("MM/dd/yyyy HH:mm:ss");
        params.setDateFormat("MM/dd/yyyy");
        params.setTimeFormat("HH:mm:ss");
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelDateFormatsXlsx()
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
        
        ExcelXlsxConnector excelConnector = new ExcelXlsxConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        params.setDateTimeFormat("MM/dd/yyyy HH:mm:ss");
        params.setDateFormat("MM/dd/yyyy");
        params.setTimeFormat("HH:mm:ss");
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testExcelMaxRows()
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
        
        ExcelConnector excelConnector = new ExcelConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        params.setMaxRows(2);
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(!sourceDataSet.equals(destDataSet));
        
        assertTrue(destDataSet.getRecordCount() == 2);
    }
    
    @Test
    public void testExcelMaxRowsXlsx()
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
        
        ExcelXlsxConnector excelConnector = new ExcelXlsxConnector();
        
        ExcelConnectorParams params = new ExcelConnectorParams(config, false,
                config.getLogStep());
        
        excelConnector.persist(params, sourceDataSet, driver);
        
        DataSet destDataSet = new DataSet();
        destDataSet.setName("test");
        
        params.setMaxRows(2);
        excelConnector.populate(params, destDataSet, driver);
        
        assertTrue(!sourceDataSet.equals(destDataSet));
        
        assertTrue(destDataSet.getRecordCount() == 2);
    }
    
    @Test
    public void testXml2Excel()
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
        
        ExcelConnector excelConnector = new ExcelConnector();
        
        ExcelConnectorParams excelParams = new ExcelConnectorParams(config,
                false, config.getLogStep());
        
        DataSet destDataSet = DataSetGenerator.getTestDataSet();
        destDataSet.setName("test");
        
        excelConnector.persist(excelParams, destDataSet, driver);
        
        excelConnector.populate(excelParams, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
    @Test
    public void testXml2ExcelXlsx()
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
        
        ExcelXlsxConnector excelConnector = new ExcelXlsxConnector();
        
        ExcelConnectorParams excelParams = new ExcelConnectorParams(config,
                false, config.getLogStep());
        
        DataSet destDataSet = DataSetGenerator.getTestDataSet();
        destDataSet.setName("test");
        
        excelConnector.persist(excelParams, destDataSet, driver);
        
        excelConnector.populate(excelParams, destDataSet, driver);
        
        assertTrue(sourceDataSet.equals(destDataSet));
    }
    
}
