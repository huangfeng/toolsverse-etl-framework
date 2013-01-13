/*
 * EtlConfigTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.config;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.KeyValue;

/**
 * EtlConfigTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlConfigTest
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
    }
    
    @Test
    public void testAddConnectionToMap()
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        
        etlConfig.init();
        
        assertTrue(etlConfig.getAliasesMap() == null);
        
        etlConfig.addAliasToMap("test1", new Alias());
        etlConfig.addAliasToMap("test2", new Alias());
        
        assertNotNull(etlConfig.getAliasesMap());
        
        assertTrue(etlConfig.getAliasesMap().size() == 2);
    }
    
    @Test
    public void testEtlConfigInit()
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        
        etlConfig.init();
        
        assertTrue(FileUtils
                .getUnixFolderName(
                        FilenameUtils.normalize(SystemConfig.instance()
                                .getDataFolderName()
                                + EtlConfig.DEFAULT_SCENARIO_PATH)).equals(
                        etlConfig.getScenarioPath()));
        
        assertTrue((FileUtils.getUnixFolderName(FilenameUtils
                .normalize(SystemConfig.instance().getConfigFolderName())) + EtlConfig.DEFAULT_ETL_CONFIG)
                .equals(etlConfig.getXmlConfigFileName()));
        
        assertTrue(etlConfig.getLogStep() == EtlConfig.DEFAULT_LOG_STEP);
        
        assertNotNull(etlConfig.getConnectionFactory());
        assertTrue(etlConfig.getConnectionFactory().getClass().getName()
                .equals(EtlConfig.DEFAULT_CONNECTION_FACTORY_CLASS));
        
        assertNotNull(etlConfig.getCache());
        assertTrue(etlConfig.getCache().getClass().getName()
                .equals(EtlConfig.DEFAULT_CACHE_CLASS));
        
        assertTrue(etlConfig.getExecute() == null);
    }
    
    @Test
    public void testGetAction()
    {
        EtlConfig etlConfig = new EtlConfig();
        
        assertTrue(EtlConfig.EXTRACT == etlConfig
                .getAction(EtlConfig.EXTRACT_STR));
        
        assertTrue(EtlConfig.EXTRACT_LOAD == etlConfig.getAction("abc"));
        
        assertTrue(EtlConfig.EXTRACT == etlConfig
                .getActionByDesc(EtlConfig.EXTRACT_DESC_STR));
        
        assertTrue(EtlConfig.EXTRACT_LOAD == etlConfig.getActionByDesc("abc"));
        
        assertTrue(EtlConfig.EXTRACT_DESC_STR.equals(etlConfig
                .getDescByAction(EtlConfig.EXTRACT_STR)));
        
        assertTrue(etlConfig.getDescByAction("abc") == null);
    }
    
    @Test
    public void testGetActions()
    {
        EtlConfig etlConfig = new EtlConfig();
        
        List<KeyValue> actions;
        
        actions = etlConfig.getActions("  ");
        
        assertNotNull(actions);
        assertTrue(actions.size() == 3);
        assertTrue(actions.get(1).getKey().equals(EtlConfig.EXTRACT));
        assertTrue(actions.get(1).getValue().equals(EtlConfig.EXTRACT_DESC_STR));
        assertTrue(actions.get(0).getKey().equals(EtlConfig.EXTRACT_LOAD));
        assertTrue(actions.get(0).getValue()
                .equals(EtlConfig.EXTRACT_LOAD_DESC_STR));
        
        actions = etlConfig.getActions("abc");
        assertNotNull(actions);
        assertTrue(actions.size() == 3);
        assertTrue(actions.get(1).getKey().equals(EtlConfig.EXTRACT));
        assertTrue(actions.get(1).getValue().equals(EtlConfig.EXTRACT_DESC_STR));
        assertTrue(actions.get(0).getKey().equals(EtlConfig.EXTRACT_LOAD));
        assertTrue(actions.get(0).getValue()
                .equals(EtlConfig.EXTRACT_LOAD_DESC_STR));
        
        actions = etlConfig.getActions(EtlConfig.EXTRACT_STR);
        assertNotNull(actions);
        assertTrue(actions.size() == 1);
        assertTrue(actions.get(0).getKey().equals(EtlConfig.EXTRACT));
        assertTrue(actions.get(0).getValue().equals(EtlConfig.EXTRACT_DESC_STR));
        
        actions = etlConfig.getActions(EtlConfig.EXTRACT_STR + "|"
                + EtlConfig.LOAD_STR);
        assertNotNull(actions);
        assertTrue(actions.size() == 2);
        assertTrue(actions.get(0).getKey().equals(EtlConfig.EXTRACT));
        assertTrue(actions.get(0).getValue().equals(EtlConfig.EXTRACT_DESC_STR));
        assertTrue(actions.get(1).getKey().equals(EtlConfig.LOAD));
        assertTrue(actions.get(1).getValue().equals(EtlConfig.LOAD_DESC_STR));
    }
    
    @Test
    public void testGetDrivers()
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        
        try
        {
            etlConfig.initConfigXml();
            
            assertNotNull(etlConfig.getDrivers());
        }
        finally
        {
            etlConfig.getConnectionFactory().releaseConnections();
        }
    }
    
    @Test
    public void testInitConfigXml()
        throws Exception
    {
        EtlConfig etlConfig = new EtlConfig();
        
        try
        {
            etlConfig.initConfigXml();
            
            assertTrue(etlConfig.getLogStep() == 777);
            
            assertNotNull(etlConfig.getCache());
            assertTrue(etlConfig.getCache().getClass().getName()
                    .equals("com.toolsverse.cache.MemoryCache"));
            
            assertNotNull(etlConfig.getExecute());
            assertTrue(etlConfig.getExecute().size() == 1);
            assertNotNull(etlConfig.getExecute().get(0));
            assertTrue(etlConfig.getExecute().get(0).getName()
                    .equalsIgnoreCase("test.xml"));
            assertTrue(etlConfig.getExecute().get(0).getAction() == EtlConfig.EXTRACT_LOAD);
            
            assertTrue("c:/oracle".equalsIgnoreCase(etlConfig
                    .getString("oracle.oraclehome")));
            assertTrue("c:/informix".equalsIgnoreCase(etlConfig
                    .getString("informix.home")));
        }
        finally
        {
            etlConfig.getConnectionFactory().releaseConnections();
        }
    }
    
}
