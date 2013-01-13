/*
 * ObjectFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.factory;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * ObjectFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ObjectFactoryTest
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
    public void testCastToFail()
    {
        Object testInterface = null;
        
        try
        {
            testInterface = ObjectFactory.instance()
                    .get(TestInterface.class.getName(),
                            TestInterfaceSingleton.class);
        }
        catch (RuntimeException ex)
        {
            testInterface = null;
        }
        
        assertTrue(testInterface == null);
    }
    
    @Test
    public void testCastToSuccess()
    {
        SystemConfig.instance().bind(TestInterfaceInherited.class.getName(),
                TestClassInherited.class.getName());
        
        Object testInterface = ObjectFactory.instance().get(
                TestInterfaceInherited.class.getName(),
                TestInterfaceInherited.class);
        
        assertTrue(testInterface != null);
    }
    
    @Test
    public void testClientObject()
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                TestInterface.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                TestClassClient.class.getName());
        
        SystemConfig.instance().bind(
                TestInterface.class.getName() + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                TestClassServer.class.getName());
        
        TestInterface testInterface = (TestInterface)ObjectFactory.instance()
                .get(TestInterface.class.getName());
        
        assertTrue(testInterface.toString().equals(
                TestClassClient.class.getName()));
    }
    
    @Test
    public void testGet()
    {
        SystemConfig.instance().bind(TestInterface.class.getName(),
                TestClass.class.getName());
        
        TestInterface testInterface = (TestInterface)ObjectFactory.instance()
                .get(TestInterface.class.getName());
        
        assertNotNull(testInterface);
    }
    
    @Test
    public void testGetDefault()
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.TEST_DEPLOYMENT);
        
        Object testClass = ObjectFactory.instance().get("sdffdgfddfg",
                TestClass.class.getName(), false);
        
        assertNotNull(testClass);
        
        assertTrue(testClass instanceof TestClass);
    }
    
    @Test
    public void testGetSingleton()
    {
        SystemConfig.instance().bind(TestInterfaceSingleton.class.getName(),
                TestClassSingleton.class.getName());
        
        TestInterfaceSingleton testInterface1 = (TestInterfaceSingleton)ObjectFactory
                .instance().get(TestInterfaceSingleton.class.getName(), true);
        
        TestInterfaceSingleton testInterface2 = (TestInterfaceSingleton)ObjectFactory
                .instance().get(TestInterfaceSingleton.class.getName(), true);
        
        assertNotNull(testInterface1);
        
        assertNotNull(testInterface2);
        
        assertSame(testInterface1, testInterface2);
    }
    
    @Test
    public void testGetWithRealClassName()
    {
        SystemConfig.instance().bind(TestInterface.class.getName(),
                TestClass.class.getName());
        
        TestInterface testInterface1 = (TestInterface)ObjectFactory.instance()
                .get("first", TestInterface.class.getName());
        
        TestInterface testInterface2 = (TestInterface)ObjectFactory.instance()
                .get("second", TestInterface.class.getName());
        
        assertNotNull(testInterface1);
        assertNotNull(testInterface2);
        
        assertTrue(testInterface1 != testInterface2);
        
        testInterface1 = (TestInterface)ObjectFactory.instance().get("first",
                null, TestInterface.class.getName(), null, true, true);
        
        TestInterface testInterface11 = (TestInterface)ObjectFactory.instance()
                .get("first", null, TestInterface.class.getName(), null, true,
                        true);
        
        assertTrue(testInterface1 == testInterface11);
    }
    
    @Test
    public void testServerObject()
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.SERVER_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                TestInterface.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                TestClassServer.class.getName());
        
        SystemConfig.instance().bind(
                TestInterface.class.getName() + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                TestClassServer.class.getName());
        
        TestInterface testInterface = (TestInterface)ObjectFactory.instance()
                .get(TestInterface.class.getName());
        
        assertTrue(testInterface.toString().equals(
                TestClassServer.class.getName()));
    }
    
}
