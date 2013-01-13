/*
 * AppInfoCollectorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.collector;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * AppInfoCollectorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class AppInfoCollectorTest
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
    public void testAppInfo()
        throws Exception
    {
        AppInfo appInfo = new AppInfo();
        
        appInfo.setClientHost("  ");
        
        assertTrue(AppInfo.getEncodedName(AppInfo.LOCAL_HOST).equals(
                appInfo.getUniqueName()));
        
        appInfo.setClientHost(null);
        
        assertTrue(AppInfo.getEncodedName(AppInfo.LOCAL_HOST).equals(
                appInfo.getUniqueName()));
        
        appInfo.setClientHost("169.198.0.1");
        
        assertTrue(AppInfo.getEncodedName("16919801").equals(
                appInfo.getUniqueName()));
        
        appInfo.setClientHost("maximus.toolsverse.com");
        
        assertTrue(AppInfo.getEncodedName("maximustoolsversecom").equals(
                appInfo.getUniqueName()));
    }
    
    @Test
    public void testEncoding()
        throws Exception
    {
        AppInfo appInfo = new AppInfo();
        
        for (int i = 0; i < 10000; i++)
        {
            String host = "host" + Utils.getUUIDName();
            
            appInfo.setClientHost(host);
            
            assertTrue(AppInfo.getEncodedName(host).equals(
                    appInfo.getUniqueName()));
        }
    }
    
    @Test
    public void testGetAppInfo()
        throws Exception
    {
        AppInfoCollector appInfoCollector = (AppInfoCollector)ObjectFactory
                .instance().get(AppInfoCollector.class.getName(),
                        AppInfoCollectorDefault.class.getName(), false);
        
        assertNotNull(appInfoCollector);
        
        assertNotNull(appInfoCollector.getAppInfo());
        
        assertTrue(!Utils.isNothing(appInfoCollector.getAppInfo()
                .getClientAddr()));
        assertTrue(!Utils.isNothing(appInfoCollector.getAppInfo()
                .getClientHost()));
        assertTrue(!Utils.isNothing(appInfoCollector.getAppInfo()
                .getUniqueName()));
        assertTrue(!Utils.isNothing(appInfoCollector.getAppInfo().getAppHome()));
        assertTrue(!Utils.isNothing(appInfoCollector.getAppInfo().getAppRoot()));
        assertTrue(appInfoCollector.getAppInfo().getAppHome()
                .equals(appInfoCollector.getAppInfo().getAppRoot() + "/"));
    }
    
}