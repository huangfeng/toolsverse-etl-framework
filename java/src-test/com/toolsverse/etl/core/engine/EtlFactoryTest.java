/*
 * EtlFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.MockExtendedCallableDriver;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * EtlFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlFactoryTest
{
    private static final String DRIVER_CLASS_NAME = "com.toolsverse.etl.driver.MockExtendedCallableDriver";
    
    private static final String SCENARIO_NAME = "test.xml";
    
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
    
    private Scenario getScenario(String scenarioFileName, int parseScope)
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Scenario scenario = etlFactory.getScenario(etlConfig, scenarioFileName,
                parseScope);
        
        return scenario;
    }
    
    private TypedKeyValue<EtlConfig, Scenario> getSettings(
            String scenarioFileName)
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        etlConfig.init();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Scenario scenario = etlFactory.getScenario(etlConfig, scenarioFileName,
                EtlFactory.PARSE_ALL);
        
        return new TypedKeyValue<EtlConfig, Scenario>(etlConfig, scenario);
    }
    
    @Test
    public void testAssignVars()
        throws Exception
    {
        Scenario scenarioFrom = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenarioFrom);
        
        Scenario scenarioTo = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenarioTo);
        
        EtlUtils.substituteVars(scenarioFrom.getVariables());
        
        scenarioFrom.assignVars(scenarioTo);
        
        assertNotNull(scenarioTo.getVariable("TEST_SUBST"));
        
        assertTrue("some value".equals(scenarioTo.getVariable("TEST_SUBST")
                .getValue()));
    }
    
    @Test
    public void testGetDriver()
        throws Exception
    {
        Scenario scenario = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenario);
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = etlFactory.getDriver(scenario.getDriverClassName(),
                null, null);
        
        assertTrue(driver instanceof MockExtendedCallableDriver);
        
        assertTrue(driver.getCaseSensitive() == Driver.CASE_SENSITIVE_LOWER);
    }
    
    @Test
    public void testGetDriverClassName()
        throws Exception
    {
        Scenario scenario = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenario);
        
        EtlFactory etlFactory = new EtlFactory();
        
        assertTrue(DRIVER_CLASS_NAME.equals(etlFactory
                .getDriverClassName(scenario.getDriverClassName())));
    }
    
    @Test
    public void testGetDrivers()
        throws Exception
    {
        TypedKeyValue<EtlConfig, Scenario> settings = getSettings(SCENARIO_NAME);
        
        EtlConfig config = settings.getKey();
        
        Scenario scenario = settings.getValue();
        
        assertNotNull(config);
        
        assertNotNull(scenario);
        
        assertNotNull(config.getDrivers());
        
        assertTrue(config.getDrivers().size() > 0);
    }
    
    @Test
    public void testGetScenario()
        throws Exception
    {
        Scenario scenario = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenario);
        
        assertNotNull(scenario.getId());
        
        assertTrue(scenario.isReady());
        
        assertTrue("This is a description".equals(scenario.getDescription()));
        
        assertNotNull(scenario.getVariables());
        
        assertTrue(scenario.getVariables().size() > 0);
        
        assertNotNull(scenario.getSources());
        
        assertTrue(scenario.getSources().size() > 0);
        
        assertNotNull(scenario.getDestinations());
        
        assertTrue(scenario.getDestinations().size() > 0);
        
        Source source = scenario.getSources().get("PROPERTY");
        
        assertNotNull(source);
        
        assertTrue(!Utils.isNothing(source.getSql()));
        
        assertNotNull(source.getTasks());
        
        assertTrue(source.getTasks().size() > 0);
        
        Destination dest = scenario.getDestinations().get("CLOBS");
        
        assertNotNull(dest);
        
        assertNotNull(dest.getVariables());
        
        assertTrue(dest.getVariables().size() > 0);
    }
    
    @Test
    public void testIsReady()
        throws Exception
    {
        Scenario scenario = getScenario(SCENARIO_NAME,
                EtlFactory.PARSE_STRUCTURE_ONLY);
        
        assertNotNull(scenario);
        
        assertTrue(!scenario.isReady());
        
        scenario = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenario);
        
        assertTrue(scenario.isReady());
        
    }
    
    @Test
    public void testSubstituteVars()
        throws Exception
    {
        Scenario scenario = getScenario(SCENARIO_NAME, EtlFactory.PARSE_ALL);
        
        assertNotNull(scenario);
        
        EtlUtils.substituteVars(scenario.getVariables());
        
        assertNotNull(scenario.getVariable("TEST_SUBST"));
        
        assertTrue("some value".equals(scenario.getVariable("TEST_SUBST")
                .getValue()));
    }
    
}
