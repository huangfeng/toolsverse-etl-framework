/*
 * ModelTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.Map;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.mvc.pojo.PojoWrapper;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * ModelTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ModelTest
{
    private static TestData _testData = null;
    
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
        
        _testData = new TestData();
    }
    
    @Test
    public void testAccessPopulate()
    {
        Level1Node node = new Level1Node();
        
        assertTrue(node.getLevel1Attr() == null);
        
        node.setLevel1Attr("abc");
        
        assertTrue("abc".equals(node.getLevel1Attr()));
        
        assertTrue("abc".equals(node.access(Level1Node.LEVEL1_ATTR)));
        
        node.populate(Level1Node.LEVEL1_ATTR, "xyz");
        
        assertTrue("xyz".equals(node.getLevel1Attr()));
        
        assertTrue("xyz".equals(node.access(Level1Node.LEVEL1_ATTR)));
        
        assertTrue(node.getBooleanAttr() == null);
        
        node.setBooleanAttr(Boolean.TRUE);
        
        assertTrue(Boolean.TRUE.equals(node.getBooleanAttr()));
        
        assertTrue(Boolean.TRUE.equals(node.access(Level1Node.BOOLEAN_ATTR)));
        
        node.populate(Level1Node.BOOLEAN_ATTR, Boolean.FALSE);
        
        assertTrue(Boolean.FALSE.equals(node.getBooleanAttr()));
        
        assertTrue(Boolean.FALSE.equals(node.access(Level1Node.BOOLEAN_ATTR)));
    }
    
    @Test
    public void testClone()
        throws Exception
    {
        Level1Node node = new Level1Node();
        
        node.setName("node1");
        node.setBooleanAttr(Boolean.TRUE);
        node.setIntAttr(1);
        node.setLevel1Attr("abc");
        
        assertTrue(node.getLevel2Model() == null);
        
        Level1Node cloneNode = (Level1Node)node.clone();
        
        assertNotNull(cloneNode);
        
        assertTrue(node != cloneNode);
        
        assertTrue("node1".equals(cloneNode.getName()));
        assertTrue(Boolean.TRUE.equals(cloneNode.getBooleanAttr()));
        assertTrue(1 == cloneNode.getIntAttr());
        assertTrue("abc".equals(cloneNode.getLevel1Attr()));
        
        assertTrue(cloneNode.getLevel2Model() == null);
    }
    
    @Test
    public void testCopyFrom()
        throws Exception
    {
        Level1Node node = new Level1Node();
        
        node.setName("node1");
        node.setBooleanAttr(Boolean.TRUE);
        node.setIntAttr(1);
        node.setLevel1Attr("abc");
        
        Level1Node copyNode = new Level1Node();
        
        copyNode.copyFrom(node);
        
        assertTrue("node1".equals(copyNode.getName()));
        assertTrue(Boolean.TRUE.equals(copyNode.getBooleanAttr()));
        assertTrue(1 == copyNode.getIntAttr());
        assertTrue("abc".equals(copyNode.getLevel1Attr()));
        
        assertTrue(copyNode.getLevel2Model() == null);
    }
    
    @Test
    public void testGetAttributes()
    {
        _testData.getRoot().setCurrentNode(_testData.getLevel1Node1());
        
        Map<String, Model.Attribute> attrs = _testData.getModel()
                .getAttributes();
        
        assertNotNull(attrs);
        
        assertTrue(attrs.size() == 9);
        
        assertTrue(attrs.containsKey(MasterModel.SEARCH_STR));
        assertTrue(attrs.containsKey(MasterModel.ROOT));
        assertTrue(attrs.containsKey(Root.CURRENT_NODE));
        assertTrue(attrs.containsKey(Level1Node.NODE_NAME));
        assertTrue(attrs.containsKey(Level1Node.BOOLEAN_ATTR));
        assertTrue(attrs.containsKey(Level1Node.INT_ATTR));
        assertTrue(attrs.containsKey(Level1Node.LEVEL2_NODE));
        assertTrue(attrs.containsKey(Level1Node.LEVEL1_ATTR));
        assertTrue(attrs.containsKey(Level2Model.LEVEL2_ATTR));
    }
    
    @Test
    public void testGetOwner()
    {
        assertNotNull(_testData.getRoot().getOwner());
        
        assertTrue(_testData.getRoot().getOwner() == _testData.getModel());
        
        Object level1Node = _testData.getLevel1Node1();
        
        assertNotNull(level1Node);
        
        assertTrue(level1Node instanceof Level1Node);
        
        assertTrue(((Level1Node)level1Node).getLevel2Model().getOwner() == level1Node);
    }
    
    @Test
    public void testManualAttributes()
    {
        Level1Node node = new Level1Node();
        
        assertTrue(node.getIntAttr() == null);
        
        node.setIntAttr(1);
        
        assertTrue(1 == node.getIntAttr());
        
        assertTrue(new Integer(1).equals(node.access(Level1Node.INT_ATTR)));
        
        node.populate(Level1Node.INT_ATTR, 2);
        
        assertTrue(2 == node.getIntAttr());
        
        assertTrue(new Integer(2).equals(node.access(Level1Node.INT_ATTR)));
    }
    
    @Test
    public void testPojoModel()
        throws Exception
    {
        PojoWrapper<PojoModel> wrapper = new PojoWrapper<PojoModel>(
                PojoModel.class);
        
        assertNotNull(wrapper);
        
        assertTrue(wrapper.getPojo() instanceof PojoModel);
        
        PojoModel model = wrapper.getPojo();
        
        assertTrue(Boolean.FALSE.equals(model.getBoolean()));
        assertTrue(-1 == model.getInt());
        assertTrue(model.getStr() == null);
        
        final TypedKeyValue<Integer, Integer> counter = new TypedKeyValue<Integer, Integer>(
                0, 0);
        wrapper.addPropertyChangeListener(new PropertyChangeListener()
        {
            public void propertyChange(PropertyChangeEvent evt)
            {
                counter.setKey(counter.getKey() + 1);
            }
        });
        
        model.setBoolean(Boolean.TRUE);
        assertTrue(counter.getKey() == 1);
        
        model.setInt(123);
        assertTrue(counter.getKey() == 2);
        
        model.setStr("abc");
        assertTrue(counter.getKey() == 3);
    }
    
    @Test
    public void testReadWrite()
    {
        Level1Node node = new Level1Node();
        
        assertTrue(node.getBooleanAttr() == null);
        
        node.read(Level1Node.BOOLEAN_ATTR, "true");
        
        assertTrue(Boolean.TRUE.equals(node.getBooleanAttr()));
        
        node.read(Level1Node.BOOLEAN_ATTR, "false");
        
        assertTrue(Boolean.FALSE.equals(node.getBooleanAttr()));
        
        Object value = node.write(Level1Node.BOOLEAN_ATTR);
        
        assertTrue("false".equals(value));
        
        assertTrue(node.getLevel1Attr() == null);
        
        node.read(Level1Node.LEVEL1_ATTR, "abc");
        
        assertTrue("abc".equals(node.getLevel1Attr()));
        
        node.setLevel1Attr("xyz");
        
        value = node.write(Level1Node.LEVEL1_ATTR);
        
        assertTrue("xyz".equals(value));
    }
}