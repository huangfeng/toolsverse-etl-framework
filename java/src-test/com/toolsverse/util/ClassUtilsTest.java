/*
 * ClassUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotNull;

import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * ClassUtilsTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ClassUtilsTest
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
    public void testClone()
        throws Exception
    {
        Integer test = 123;
        
        assertEquals(test, ClassUtils.clone(test));
    }
    
    @Test
    public void testGetClassesRecursive()
    {
        List<Class<?>> classes = ClassUtils.getClasses(
                "com.toolsverse.etl.driver",
                com.toolsverse.etl.driver.Driver.class, true);
        
        assertNotNull(classes);
        
        assertTrue(classes.contains(
        		com.toolsverse.etl.driver.GenericJdbcDriver.class));
    }
    
    @Test
    public void testGetClassesRegular()
    {
        List<Class<?>> classes = ClassUtils.getClasses(
                "com.toolsverse.etl.driver",
                com.toolsverse.etl.driver.Driver.class, false);
        
        assertNotNull(classes);
        
        assertTrue(classes.contains(
        		com.toolsverse.etl.driver.GenericJdbcDriver.class));
    }
}
