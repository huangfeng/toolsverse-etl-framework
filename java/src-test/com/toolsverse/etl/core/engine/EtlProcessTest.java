/*
 * EtlProcessTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.connector.text.TextConnector;
import com.toolsverse.etl.connector.text.TextConnectorParams;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.connector.xml.XmlConnectorParams;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlProcess.EtlMode;
import com.toolsverse.etl.core.function.DefFunctions;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.log.Logger;

/**
 * EtlProcessTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlProcessTest
{
    private static final String SCENARIO_NAME = "etlprocess.xml";
    
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
    }
    
    @AfterClass
    public static void tearDown()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
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
    
    private TypedKeyValue<EtlConfig, Scenario> getSettings(
            String scenarioFileName)
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        etlConfig.init();
        
        etlConfig.addConnection(null, getDbAlias(), null,
                EtlConfig.SOURCE_CONNECTION_NAME);
        etlConfig.addConnection(null, getTextAlias(), "source_text", null);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Scenario scenario = etlFactory.getScenario(etlConfig, scenarioFileName,
                EtlFactory.PARSE_ALL);
        
        return new TypedKeyValue<EtlConfig, Scenario>(etlConfig, scenario);
    }
    
    private TypedKeyValue<EtlConfig, Scenario> getSettingsForMappedFields(
            String scenarioFileName)
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        etlConfig.init();
        
        etlConfig.addConnection(null, getDbAlias(), null,
                EtlConfig.SOURCE_CONNECTION_NAME);
        etlConfig.addConnection(null, getXmlAlias(), "source_xml_mapped", null);
        
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
        alias.setParams("metadata=false;firstrow=false");
        
        return alias;
    }
    
    private Alias getXmlAlias()
    {
        Alias alias = new Alias();
        alias.setName("source_xml_mapped");
        alias.setUrl(SystemConfig.instance().getDataFolderName()
                + "SOURCE_XML_MAPPED.xml");
        alias.setConnectorClassName(XmlConnector.class.getName());
        
        return alias;
    }
    
    @Test
    public void testEmbeddedMode()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        destination.setStream(true);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.EMBEDDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        
        assertTrue(file.exists());
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getTextAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        textConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
    }
    
    @Test
    public void testExecute()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlMode.INCLUDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        
        assertTrue(file.exists());
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getTextAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        textConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
    }
    
    @Test
    public void testExtractLoad()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        destination.setStream(true);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.INCLUDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        
        assertTrue(file.exists());
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getTextAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        textConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
    }
    
    @Test
    public void testFunctions()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        destination.setStream(true);
        
        Variable var = new Variable();
        var.setName("SOURCE_NUM");
        var.setFunction("getPk");
        var.setFunctionClassName(DefFunctions.class.getName());
        
        destination.getVariables().put("SOURCE_NUM", var);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.INCLUDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        
        assertTrue(file.exists());
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getTextAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        textConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
        
        for (int row = 0; row < dataSet.getRecordCount(); row++)
        {
            Object value = dataSet.getFieldValue(row, 0);
            assertTrue(String.valueOf(row + 1).equals(value.toString()));
        }
    }
    
    @Test
    public void testMapFields()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettingsForMappedFields("mapfields.xml");
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlMode.INCLUDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_XML_MAPPED.xml");
        
        assertTrue(file.exists());
        
        XmlConnector xmlConnector = new XmlConnector();
        
        XmlConnectorParams params = new XmlConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getXmlAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        xmlConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
        
        assertTrue(dataSet.getFieldCount() == 1);
        
        assertNotNull(dataSet.getFieldDef("MAP_DESCRIPTION"));
    }
    
    @Test
    public void testScriptFunctions()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "*.*");
        
        Scenario scenario = null;
        EtlConfig config = null;
        
        TypedKeyValue<EtlConfig, Scenario> typedKeyValue = getSettings(SCENARIO_NAME);
        
        config = typedKeyValue.getKey();
        scenario = typedKeyValue.getValue();
        
        assertNotNull(config);
        assertNotNull(scenario);
        
        assertNotNull(scenario.getDestinations());
        
        Destination destination = scenario.getDestinations().get("SOURCE");
        assertNotNull(destination);
        destination.setStream(true);
        
        Variable var = new Variable();
        var.setName("SOURCE_NUM");
        var.setFunction(DefFunctions.SCRIPT_FUNCTION);
        var.setFunctionClassName(DefFunctions.class.getName());
        var.setCode("var value; value = '123';");
        var.setLang("JavaScript");
        
        destination.getVariables().put("SOURCE_NUM", var);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        assertNotNull(driver);
        
        EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.INCLUDED);
        EtlRequest request = new EtlRequest(config, scenario, Logger.SEVERE);
        
        EtlResponse response = etlProcess.execute(request);
        
        assertNotNull(response);
        
        assertTrue(EtlConfig.RETURN_OK == response.getRetCode());
        
        File file = new File(SystemConfig.instance().getDataFolderName()
                + "SOURCE_PERSIST_TEXT.dat");
        
        assertTrue(file.exists());
        
        TextConnector textConnector = new TextConnector();
        
        TextConnectorParams params = new TextConnectorParams(config, false,
                config.getLogStep());
        
        params.init(getTextAlias());
        
        DataSet dataSet = new DataSet();
        dataSet.setName(destination.getName());
        
        textConnector.populate(params, dataSet, driver);
        
        assertTrue(!dataSet.isEmpty());
        
        for (int row = 0; row < dataSet.getRecordCount(); row++)
        {
            Object value = dataSet.getFieldValue(row, 0);
            assertTrue("123".equals(value.toString()));
        }
    }
}