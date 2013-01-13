/*
 * SystemConfigTest.java
 * 
 * Copyright 2010-2012 Toolsverse, Inc. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.config;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.Utils;

/**
 * SystemConfigTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SystemConfigTest
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
    
    @Test
    public void testAppName()
    {
        assertTrue("test".equals(SystemConfig.instance().getAppName()));
    }
    
    @Test
    public void testGetDataFolder()
    {
        String data = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()
                        + SystemConfig.DATA_FOLDER_PATH));
        
        assertTrue(data.equals(SystemConfig.instance().getDataFolderName()));
        
        assertTrue(FileUtils.fileExists(data));
    }
    
    @Test
    public void testGetDecodeSystemPropertyTest()
    {
        SystemConfig.instance().setSystemProperty("var", "1");
        
        SystemConfig.instance().setSystemProperty("abc", "abc");
        SystemConfig.instance().setSystemProperty("xyz", "xyz");
        
        SystemConfig.instance().setSystemProperty("check",
                "@decode(var,@1,abc,@2,xyz)");
        
        assertTrue("abc".equals(SystemConfig.instance().getSystemProperty(
                "check")));
        
        SystemConfig.instance().setSystemProperty("var", "2");
        
        assertTrue("xyz".equals(SystemConfig.instance().getSystemProperty(
                "check")));
        
        SystemConfig.instance().getSystemProperties().remove("var");
        SystemConfig.instance().setSystemProperty("check",
                "@decode(var,@1,abc,@2,xyz,@null,@something)");
        
        assertTrue("something".equals(SystemConfig.instance()
                .getSystemProperty("check")));
    }
    
    @Test
    public void testGetErrorsFolder()
    {
        String errors = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()
                        + SystemConfig.DATA_FOLDER_PATH
                        + SystemConfig.ERRORS_PATH));
        
        assertTrue(errors.equals(SystemConfig.instance().getErrorsFolder()));
        
        assertTrue(FileUtils.fileExists(errors));
    }
    
    @Test
    public void testGetHome()
    {
        String home = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()));
        
        assertTrue(home.equals(SystemConfig.instance().getHome()));
        
        assertTrue(FileUtils.fileExists(home));
    }
    
    @Test
    public void testGetLibsPath()
    {
        String jarsPath = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue())
                + SystemConfig.LIB_PATH);
        
        assertTrue(jarsPath.equals(SystemConfig.instance().getLibsPath()));
    }
    
    @Test
    public void testGetMode()
    {
        try
        {
            assertTrue(SystemConfig.CLIENT_MODE.equals(SystemConfig.instance()
                    .getMode()));
            
            SystemConfig.instance().setSystemProperty(SystemConfig.SERVER_URL,
                    "abc");
            
            assertTrue(SystemConfig.CLIENT_SERVER_MODE.equals(SystemConfig
                    .instance().getMode()));
            
            SystemConfig.instance().setSystemProperty(
                    SystemConfig.DEPLOYMENT_PROPERTY,
                    SystemConfig.SERVER_DEPLOYMENT);
            
            assertTrue(SystemConfig.WEB_MODE.equals(SystemConfig.instance()
                    .getMode()));
        }
        finally
        {
            SystemConfig.instance().setSystemProperty(SystemConfig.SERVER_URL,
                    null);
            
            SystemConfig.instance().setSystemProperty(
                    SystemConfig.DEPLOYMENT_PROPERTY,
                    SystemConfig.TEST_DEPLOYMENT);
            
        }
    }
    
    @Test
    public void testGetObjectProperty()
    {
        SystemConfig.instance().setSystemProperty(
                String.class.getName() + ".value", "abc");
        
        assertTrue("abc".equals(SystemConfig.instance().getObjectProperty(
                "value")));
        
        SystemConfig.instance().setSystemProperty(
                String.class.getName() + ".value", "xyz");
        
        assertTrue("xyz".equals(SystemConfig.instance().getObjectProperty(
                "value")));
        
        assertTrue("something".equals(SystemConfig.instance()
                .getObjectProperty("something")));
    }
    
    @Test
    public void testGetPropsByMask()
    {
        Map<String, String> props = SystemConfig.instance().getPropsByMask(
                "sdadat5rhhgfgdhfhgdhg");
        
        assertNotNull(props);
        
        assertTrue(props.size() == 0);
        
        props = SystemConfig.instance().getPropsByMask(
                SystemConfig.DEPLOYMENT_PROPERTY);
        
        assertNotNull(props);
        
        assertTrue(props.size() == 1);
    }
    
    @Test
    public void testGetRootDataFolder()
    {
        String data = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()
                        + SystemConfig.DATA_FOLDER_PATH));
        
        assertTrue(data.equals(SystemConfig.instance().getRootDataFolderPath()));
        
        assertTrue(FileUtils.fileExists(data));
    }
    
    @Test
    public void testGetScriptsFolder()
    {
        String scripts = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()
                        + SystemConfig.DATA_FOLDER_PATH
                        + SystemConfig.SCRIPTS_PATH));
        
        assertTrue(scripts.equals(SystemConfig.instance().getScriptsFolder()));
        
        assertTrue(FileUtils.fileExists(scripts));
    }
    
    @Test
    public void testGetSystemPropertyTest()
    {
        String home = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue()));
        
        assertTrue(home.equals(FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.instance().getSystemProperty(
                        SystemConfig.HOME_PATH_PROPERTY, null)))));
    }
    
    @Test
    public void testIsClient()
    {
        assertTrue(SystemConfig.instance().isClient());
    }
    
    @Test
    public void testIsClientServer()
    {
        try
        {
            assertTrue(!SystemConfig.instance().isClientServer());
            
            SystemConfig.instance().setSystemProperty(SystemConfig.SERVER_URL,
                    "abc");
            
            assertTrue(SystemConfig.instance().isClientServer());
        }
        finally
        {
            SystemConfig.instance().setSystemProperty(SystemConfig.SERVER_URL,
                    null);
        }
    }
    
    @Test
    public void testIsServer()
    {
        try
        {
            assertTrue(!SystemConfig.instance().isServer());
            
            SystemConfig.instance().setSystemProperty(
                    SystemConfig.DEPLOYMENT_PROPERTY,
                    SystemConfig.SERVER_DEPLOYMENT);
            
            assertTrue(SystemConfig.instance().isServer());
        }
        finally
        {
            SystemConfig.instance().setSystemProperty(
                    SystemConfig.DEPLOYMENT_PROPERTY,
                    SystemConfig.TEST_DEPLOYMENT);
        }
    }
    
    @Test
    public void testPluginsLibsPath()
    {
        String jarsPath = FilenameUtils.separatorsToUnix(FilenameUtils
                .normalize(SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue())
                + SystemConfig.PLUGIN_PATH);
        
        assertTrue(jarsPath.equals(SystemConfig.instance().getPluginsPath()));
    }
    
}
