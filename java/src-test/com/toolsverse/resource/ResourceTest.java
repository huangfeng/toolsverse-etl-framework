/*
 * ResourceTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.resource;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * ResourceTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ResourceTest
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
    public void testGetValue()
    {
        String value = Resource.ERROR_GENERAL.getValue();
        
        assertNotNull(value);
        
        SystemConfig.instance().setSystemProperty(
                Resource.class.getName() + "." + value, "abc");
        
        assertTrue("abc".equals(Resource.ERROR_GENERAL.getValue()));
        
        SystemConfig.instance().setSystemProperty(
                Resource.class.getName() + "." + value, value);
        
        assertTrue(value.equals(Resource.ERROR_GENERAL.getValue()));
    }
    
    @Test
    public void testToString()
    {
        assertTrue(Resource.ERROR_GENERAL.toString().equals(
                Resource.ERROR_GENERAL.getValue()));
        
        String value = Resource.ERROR_GENERAL.toString();
        
        SystemConfig.instance().setSystemProperty(
                Resource.class.getName() + "." + value, "abc");
        
        assertTrue("abc".equals(Resource.ERROR_GENERAL.getValue()));
        
        assertTrue(value.equals(Resource.ERROR_GENERAL.toString()));
        
        SystemConfig.instance().setSystemProperty(
                Resource.class.getName() + "." + value, value);
        
        assertTrue(Resource.ERROR_GENERAL.toString().equals(
                Resource.ERROR_GENERAL.getValue()));
    }
    
}
