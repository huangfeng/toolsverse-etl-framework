/*
 * CollectionUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * CollectionUtilsTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class CollectionUtilsTest
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
    public void testCollectionUtils()
    {
        Map<String, String> map = new LinkedHashMap<String, String>();
        
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        
        assertTrue(CollectionUtils.getKeyFromValue(map, "a").equals("1"));
        assertTrue(CollectionUtils.getKeyFromValue(map, "b").equals("2"));
        assertTrue(CollectionUtils.getKeyFromValue(map, "c").equals("3"));
        
        assertNotNull(CollectionUtils.getKeys(map));
        assertTrue(CollectionUtils.getKeys(map).size() == 3);
        
        assertTrue(CollectionUtils.getKeys(map).get(0).equals("1"));
        assertTrue(CollectionUtils.getKeys(map).get(1).equals("2"));
        assertTrue(CollectionUtils.getKeys(map).get(2).equals("3"));
    }
    
    @Test
    @SuppressWarnings("unchecked")
    public void testGetKeysFromMap()
    {
        Map<String, String> map = new LinkedHashMap<String, String>();
        
        map.put("1", "a");
        map.put("2", "b");
        map.put("3", "c");
        map.put("4", "a");
        map.put("5", "a");
        
        List<String> keys = (List<String>)CollectionUtils.getKeysFromMap(map,
                "a");
        assertNotNull(keys);
        assertTrue(keys.size() == 3);
        assertTrue("1".equals(keys.get(0)));
        assertTrue("4".equals(keys.get(1)));
        assertTrue("5".equals(keys.get(2)));
        
        keys = (List<String>)CollectionUtils.getKeysFromMap(null, "a");
        assertTrue(keys == null);
        
        keys = (List<String>)CollectionUtils.getKeysFromMap(map, null);
        assertTrue(keys == null);
        
        keys = (List<String>)CollectionUtils.getKeysFromMap(map, "x");
        assertNotNull(keys);
        assertTrue(keys.size() == 0);
    }
    
}
