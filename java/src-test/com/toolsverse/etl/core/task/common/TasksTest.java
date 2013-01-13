/*
 * TasksTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.core.engine.Extractor;
import com.toolsverse.etl.core.engine.Scenario;
import com.toolsverse.etl.core.engine.Source;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * TasksTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TasksTest
{
    private static final String SCENARIO_NAME = "tasks.xml";
    
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
        
        EtlFactory etlFactory = new EtlFactory();
        
        Scenario scenario = etlFactory.getScenario(etlConfig, scenarioFileName,
                EtlFactory.PARSE_ALL);
        
        return new TypedKeyValue<EtlConfig, Scenario>(etlConfig, scenario);
    }
    
    @Test
    public void testEvalTask()
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
            
            Source source = scenario.getSources().get("SOURCE_EVAL");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < source.getDataSet().getRecordCount(); index++)
            {
                Object value = source.getDataSet().getFieldValue(index, 1);
                
                if (value != null)
                    assertTrue("123".equals(value.toString()));
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testRegexpTask()
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
            
            Source source = scenario.getSources().get("SOURCE_REGEXP");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() > 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
            
            for (int index = 0; index < source.getDataSet().getRecordCount(); index++)
            {
                Object value = source.getDataSet().getFieldValue(index, 1);
                
                if (value != null)
                    assertTrue(value.toString().indexOf("replaced") >= 0);
            }
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testValidationTask()
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
            
            Source source = scenario.getSources().get("SOURCE_VALID");
            
            assertNotNull(source);
            
            Extractor extractor = new Extractor(null);
            
            extractor.extract(config, scenario, source, null, null);
            
            assertTrue(source.getDataSet() != null);
            
            assertTrue(source.getDataSet().getRecordCount() == 0);
            
            assertTrue(source.getDataSet().getFieldCount() > 0);
        }
        finally
        {
            if (config != null)
                config.getConnectionFactory().releaseConnections();
        }
    }
}