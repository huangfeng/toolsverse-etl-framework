/*
 * EtlFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.sql.SQLException;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.connector.excel.ExcelConnector;
import com.toolsverse.etl.connector.excel.ExcelXlsxConnector;
import com.toolsverse.etl.connector.text.TextConnector;
import com.toolsverse.etl.connector.text.TextConnectorParams;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.connector.xml.XmlConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.connection.DefaultTransactionMonitor;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * EtlFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ExtractorTest
{
    private static final String SCENARIO_NAME = "extractor.xml";
    
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
                .getDataFolderName(), "*.*");
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getScriptsFolder(), "*.*");
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
    
    private Alias getExcelAlias()
    {
        Alias alias = new Alias();
        alias.setName("source_excel");
        alias.setUrl(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_EXCEL.xls");
        alias.setConnectorClassName(ExcelConnector.class.getName());
        
        return alias;
    }
    
    private Alias getExcelXlsxAlias()
    {
        Alias alias = new Alias();
        alias.setName("source_excel");
        alias.setUrl(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_EXCEL_XLSX.xlsx");
        alias.setConnectorClassName(ExcelXlsxConnector.class.getName());
        
        return alias;
    }
    
    private TypedKeyValue<EtlConfig, Scenario> getSettings(
            String scenarioFileName)
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        etlConfig.init();
        
        etlConfig.addConnection(null, getDbAlias(), null,
                EtlConfig.SOURCE_CONNECTION_NAME);
        etlConfig.addConnection(null, getDbAlias(), "test", null);
        etlConfig.addConnection(null, getXmlAlias(), "source_xml", null);
        etlConfig.addConnection(null, getTextAlias(), "source_text", null);
        etlConfig.addConnection(null, getExcelAlias(), "source_excel", null);
        etlConfig.addConnection(null, getExcelXlsxAlias(), "source_excel_xlsx",
                null);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Scenario scenario = etlFactory.getScenario(etlConfig, scenarioFileName,
                EtlFactory.PARSE_ALL);
        
        return new TypedKeyValue<EtlConfig, Scenario>(etlConfig, scenario);
    }
    
    private Alias getTextAlias()
    {
        Alias alias = new Alias();
        alias.setName("source_text");
        alias.setUrl(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        alias.setConnectorClassName(TextConnector.class.getName());
        alias.setParams("delimiter=';';metadata=false;firstrow=false");
        
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
    public void testEmptyDataSet()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("EMPTY_DATA_SET");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() == null);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testEmptySource()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("EMPTY");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() == 0);
            
            assertTrue(source.getDataSet().getFieldCount() == 0);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testException()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("EXCEPTION");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            try
            {
                extractor.extract(config, scenario, source, null, null);
            }
            catch (Exception ex)
            {
                assertTrue(ex instanceof SQLException);
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testExtractAndPersistText()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_PERSIST_TEXT");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_TEXT.dat");
            
            assertTrue(file.exists());
            
            TextConnector textConnector = new TextConnector();
            
            TextConnectorParams params = new TextConnectorParams(config, false,
                    config.getLogStep());
            params.setDelimiter(";");
            params.setPersistMetaData(false);
            params.setFirstRowData(false);
            
            DataSet newDataSet = new DataSet();
            newDataSet.setName(source.getName());
            newDataSet.setEncode(source.isEncoded());
            newDataSet.setKeyFields(source.getKeyFields());
            newDataSet.setDriver(source.getDataSet().getDriver());
            
            textConnector.populate(params, newDataSet, null);
            
            assertTrue(source.getDataSet().equals(newDataSet));
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testExtractAndPersistTextEmpty()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get(
                    "SOURCE_PERSIST_TEXT_EMPTY");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() == null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_TEXT_EMPTY.dat");
            
            assertTrue(file.exists());
            
            TextConnector textConnector = new TextConnector();
            
            TextConnectorParams params = new TextConnectorParams(config, false,
                    config.getLogStep());
            params.setDelimiter(";");
            params.setPersistMetaData(false);
            params.setFirstRowData(false);
            
            DataSet newDataSet = new DataSet();
            newDataSet.setName(source.getName());
            newDataSet.setEncode(source.isEncoded());
            newDataSet.setKeyFields(source.getKeyFields());
            newDataSet.setDriver(new EtlFactory().getDriver(
                    scenario.getDriverClassName(), null, null));
            
            textConnector.populate(params, newDataSet, null);
            
            assertTrue(newDataSet != null);
            
            assertTrue(newDataSet.getRecordCount() > 0);
            
            assertTrue(newDataSet.getFieldCount() > 0);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testExtractSaveSql()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            scenario.setOnPopulateDataSet(Scenario.ON_ACTION_SAVE);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_COND");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            File file = new File(SystemConfig.instance().getScriptsFolder()
                    + "s_SOURCE_COND.sql");
            
            assertTrue(file.exists());
            
            String sql = FileUtils.loadTextFile(SystemConfig.instance()
                    .getScriptsFolder() + "s_SOURCE_COND.sql");
            
            assertTrue("select *  from source where source_num = ?".equals(sql));
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testExtractXml()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SAVE);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE.xml");
            
            assertTrue(file.exists());
            
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SKIP);
            
            Source xmlSource = scenario.getSources().get("SOURCE_POPULATE_XML");
            
            assertNotNull(xmlSource);
            
            extractor.extract(config, scenario, xmlSource, null, null);
            
            assertTrue(xmlSource.getDataSet() != null);
            
            assertTrue(source.getDataSet().equals(xmlSource.getDataSet()));
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testIgnoreException()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("IGNORE_EXCEPTION");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() == null);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInlineTaskExcel()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_PERSIST_EXCEL");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_EXCEL.xls");
            
            assertTrue(file.exists());
            
            Source excelSource = scenario.getSources()
                    .get("SOURCE_TASKS_EXCEL");
            
            assertNotNull(excelSource);
            
            extractor.extract(config, scenario, excelSource, null, null);
            
            assertTrue(excelSource.getDataSet() != null);
            
            assertTrue(excelSource.getDataSet().getRecordCount() > 0);
            
            assertTrue(excelSource.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < excelSource.getDataSet()
                    .getRecordCount(); index++)
            {
                assertTrue(index + 1 == ((Number)excelSource.getDataSet()
                        .getRecord(index).get(0)).intValue());
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInlineTaskExcelXlsx()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get(
                    "SOURCE_PERSIST_EXCEL_XLSX");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_EXCEL_XLSX.xlsx");
            
            assertTrue(file.exists());
            
            Source excelSource = scenario.getSources().get(
                    "SOURCE_TASKS_EXCEL_XLSX");
            
            assertNotNull(excelSource);
            
            extractor.extract(config, scenario, excelSource, null, null);
            
            assertTrue(excelSource.getDataSet() != null);
            
            assertTrue(excelSource.getDataSet().getRecordCount() > 0);
            
            assertTrue(excelSource.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < excelSource.getDataSet()
                    .getRecordCount(); index++)
            {
                assertTrue(index + 1 == ((Number)excelSource.getDataSet()
                        .getRecord(index).get(0)).intValue());
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInlineTaskSql()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_TASKS");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < source.getDataSet().getRecordCount(); index++)
            {
                assertTrue(index + 1 == ((Number)source.getDataSet()
                        .getRecord(index).get(0)).intValue());
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInlineTaskText()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_PERSIST_TEXT");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_TEXT.dat");
            
            assertTrue(file.exists());
            
            Source textSource = scenario.getSources().get("SOURCE_TASKS_TEXT");
            
            assertNotNull(textSource);
            
            extractor.extract(config, scenario, textSource, null, null);
            
            assertTrue(textSource.getDataSet() != null);
            
            assertTrue(textSource.getDataSet().getRecordCount() > 0);
            
            assertTrue(textSource.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < textSource.getDataSet()
                    .getRecordCount(); index++)
            {
                assertTrue(index + 1 == ((Number)textSource.getDataSet()
                        .getRecord(index).get(0)).intValue());
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInlineTaskXml()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SAVE);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE.xml");
            
            assertTrue(file.exists());
            
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SKIP);
            
            Source xmlSource = scenario.getSources().get("SOURCE_TASKS_XML");
            
            assertNotNull(xmlSource);
            
            extractor.extract(config, scenario, xmlSource, null, null);
            
            assertTrue(xmlSource.getDataSet() != null);
            
            assertTrue(xmlSource.getDataSet().getRecordCount() > 0);
            
            assertTrue(xmlSource.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < xmlSource.getDataSet().getRecordCount(); index++)
            {
                assertTrue(index + 1 == ((Number)xmlSource.getDataSet()
                        .getRecord(index).get(0)).intValue());
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testSimpleExtract()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testSimpleExtractAndPersist()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SAVE);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE.xml");
            
            assertTrue(file.exists());
            
            XmlConnector xmlConnector = new XmlConnector();
            
            XmlConnectorParams params = new XmlConnectorParams(config, false,
                    config.getLogStep());
            
            DataSet newDataSet = new DataSet();
            newDataSet.setName(source.getName());
            newDataSet.setEncode(source.isEncoded());
            newDataSet.setKeyFields(source.getKeyFields());
            newDataSet.setDriver(source.getDataSet().getDriver());
            
            xmlConnector.populate(params, newDataSet, null);
            
            assertTrue(source.getDataSet().equals(newDataSet));
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testSimpleExtractTestConnection()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_TEST");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testTransactionMonitor()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE_PERSIST_TEXT");
            
            assertNotNull(source);
            
            TransactionMonitor transactionMonitor = (TransactionMonitor)ObjectFactory
                    .instance().get(TransactionMonitor.class.getName(),
                            DefaultTransactionMonitor.class.getName(), null,
                            null, false, true);
            
            transactionMonitor.setConnectionFactory(config
                    .getConnectionFactory());
            
            Extractor extractor = new Extractor(transactionMonitor);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            File file = new File(SystemConfig.instance().getDataFolderName()
                    + "SOURCE_PERSIST_TEXT.dat");
            
            assertTrue(file.exists());
            
            transactionMonitor.rollback(null);
            
            assertTrue(!file.exists());
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testUsageExtract()
        throws Exception
    {
        Scenario scenario = null;
        EtlConfig config = null;
        
        try
        {
            TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
            
            config = typedKeyValue.getKey();
            scenario = typedKeyValue.getValue();
            
            assertNotNull(scenario);
            
            assertNotNull(scenario.getSources());
            
            Source source = scenario.getSources().get("SOURCE");
            
            assertNotNull(source);
            
            Source sourceCond = scenario.getSources().get("SOURCE_COND");
            
            assertNotNull(sourceCond);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, sourceCond, null, null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(sourceCond.getDataSet() != null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(sourceCond.getDataSet().getRecordCount() < source
                    .getDataSet().getRecordCount());
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
}