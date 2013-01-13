/*
 * HistoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.history;

import static org.junit.Assert.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * HistoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class HistoryTest
{
    private static int MAX_ELEMENTS_IN_LIST = 30;
    private static int MAX_ELEMENTS_IN_HISTORY = 20;
    
    private List<String> prepareData()
    {
        List<String> data = new ArrayList<String>();
        
        for (int i = 0; i < MAX_ELEMENTS_IN_LIST; i++)
            data.add("element" + i);
        
        return data;
    }
    
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
    public void testGoBack()
    {
        History<String> history = new HistoryImpl<String>(
                MAX_ELEMENTS_IN_HISTORY);
        
        List<String> data = prepareData();
        
        for (String element : data)
            history.visit(element);
        
        String current = "element" + (MAX_ELEMENTS_IN_LIST - 1);
        
        assertTrue(history.canGoBack(current));
        
        String element = history.goBack(current);
        
        assertTrue(("element" + (MAX_ELEMENTS_IN_LIST - 2)).equals(element));
    }
    
    @Test
    public void testGoForward()
    {
        History<String> history = new HistoryImpl<String>(
                MAX_ELEMENTS_IN_HISTORY);
        
        List<String> data = prepareData();
        
        for (String element : data)
            history.visit(element);
        
        String current = "element" + (MAX_ELEMENTS_IN_LIST - 1);
        
        assertTrue(!history.canGoForward(current));
        
        String element = history.goBack(current);
        
        assertTrue(history.canGoForward(element));
        
        element = history.goForward(element);
        
        assertTrue(("element" + (MAX_ELEMENTS_IN_LIST - 1)).equals(element));
    }
    
    @Test
    public void testVisit()
    {
        History<String> history = new HistoryImpl<String>(
                MAX_ELEMENTS_IN_HISTORY);
        
        List<String> data = prepareData();
        
        for (String element : data)
            history.visit(element);
        
        assertTrue(history.elements() != null);
        
        assertTrue(history.size() == MAX_ELEMENTS_IN_HISTORY);
        
        history.clear();
        
        assertTrue(history.size() == 0);
    }
    
}
