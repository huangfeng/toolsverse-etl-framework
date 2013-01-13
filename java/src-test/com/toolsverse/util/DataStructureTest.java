/*
 * DataStructureTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.HashMap;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * DataStructureTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DataStructureTest
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
    public void testDeleteOnlyUpdateList()
    {
        UpdateList<String> list = new UpdateArrayList<String>(true);
        assertNotNull(list.getInserts());
        
        list.add("1");
        list.add("2");
        list.add("3");
        
        assertTrue(list.size() == 3);
        assertTrue(list.isDirty());
        
        assertTrue(list.getInserts().size() == 0);
        
        list.remove("2");
        
        assertTrue(list.size() == 2);
        
        assertTrue(list.getDeletes().size() == 1);
        assertTrue(list.getDeletes().get(0).equals("2"));
        
        list.remove("3");
        
        assertTrue(list.size() == 1);
        
        assertTrue(list.getDeletes().size() == 2);
        assertTrue(list.getDeletes().get(1).equals("3"));
        
        list.baseline();
        assertTrue(list.getInserts().size() == 0);
        assertTrue(list.getDeletes().size() == 0);
        assertTrue(!list.isDirty());
    }
    
    @Test
    public void testIndexList()
    {
        IndexList<String> list = new IndexArrayList<String>();
        
        list.add("1");
        list.add("2");
        list.add("3");
        
        assertTrue(list.size() == 3);
        
        list.setSelectedIndex(1);
        
        assertTrue(list.get(list.getSelectedIndex()).equals("2"));
    }
    
    @Test
    public void testLimitedMap()
    {
        LimitedMap<String, String> map = new LimitedMap<String, String>(2);
        
        map.put("1", "1");
        map.put("2", "2");
        
        assertTrue(map.size() == 2);
        
        map.put("3", "3");
        
        assertTrue(map.size() == 2);
        
        assertTrue(!map.containsKey("1"));
        
        assertTrue(map.containsKey("2"));
        
        assertTrue(map.containsKey("3"));
    }
    
    @Test
    public void testListHashMap()
    {
        ListHashMap<String, String> map = new ListHashMap<String, String>();
        
        map.put("1", "1");
        map.put("2", "2");
        map.put("3", "3");
        
        assertTrue(map.size() == 3);
        
        assertTrue(map.indexOf("1") == 0);
        assertTrue(map.indexOf("2") == 1);
        assertTrue(map.indexOf("3") == 2);
        
        map.set("2", "4", true);
        
        assertTrue(map.size() == 3);
        assertTrue(map.indexOf("4") == 1);
        assertTrue(map.get("2").equals("4"));
        
        map.add("5");
        assertTrue(map.size() == 4);
        assertTrue(map.indexOf("5") == 3);
        assertTrue(map.get("4").equals("5"));
        
        map.set("12", "7", false);
        assertTrue(map.size() == 5);
        assertTrue(map.indexOf("7") == 4);
        assertTrue(map.get("12").equals("7"));
        
        map.set("12", "8", false);
        assertTrue(map.size() == 5);
        assertTrue(map.indexOf("8") == -1);
        assertTrue(map.get("12").equals("7"));
        
        map.clear();
        assertTrue(map.size() == 0);
        assertTrue(map.getList().size() == 0);
    }
    
    @Test
    public void testTypedKeyValue()
    {
        Map<TypedKeyValue<String, String>, String> map = new HashMap<TypedKeyValue<String, String>, String>();
        
        TypedKeyValue<String, String> keyValue1 = new TypedKeyValue<String, String>(
                "1", "1");
        TypedKeyValue<String, String> keyValue2 = new TypedKeyValue<String, String>(
                "2", "2");
        TypedKeyValue<String, String> keyValue3 = new TypedKeyValue<String, String>(
                "3", "3");
        TypedKeyValue<String, String> keyValue12 = new TypedKeyValue<String, String>(
                "1", "1");
        
        assertTrue(keyValue1.equals(keyValue12));
        
        map.put(keyValue1, "1");
        map.put(keyValue2, "2");
        map.put(keyValue3, "3");
        
        assertTrue(map.get(new TypedKeyValue<String, String>("1", "1")).equals(
                "1"));
        assertTrue(map.get(new TypedKeyValue<String, String>("2", "2")).equals(
                "2"));
        assertTrue(map.get(new TypedKeyValue<String, String>("3", "3")).equals(
                "3"));
    }
    
    @Test
    public void testUpdateList()
    {
        UpdateList<String> list = new UpdateArrayList<String>();
        assertNotNull(list.getInserts());
        
        list.add("1");
        list.add("2");
        list.add("3");
        
        assertTrue(list.size() == 3);
        assertTrue(list.isDirty());
        
        assertTrue(list.getInserts().size() == 3);
        assertTrue(list.getInserts().get(2).equals("3"));
        
        list.remove("2");
        
        assertTrue(list.size() == 2);
        
        assertTrue(list.getDeletes().size() == 1);
        assertTrue(list.getDeletes().get(0).equals("2"));
        
        list.remove("3");
        
        assertTrue(list.size() == 1);
        
        assertTrue(list.getDeletes().size() == 2);
        assertTrue(list.getDeletes().get(1).equals("3"));
        
        list.baseline();
        assertTrue(list.getInserts().size() == 0);
        assertTrue(list.getDeletes().size() == 0);
        assertTrue(!list.isDirty());
    }
    
}
