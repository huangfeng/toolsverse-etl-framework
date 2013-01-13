/*
 * TreeNodeTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * TreeNodeTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TreeNodeTest
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
    public void testCloneTree()
        throws Exception
    {
        MockTreeNode root = new MockTreeNode();
        root.setType(TreeNode.FOLDER_TYPE);
        root.setName("All");
        
        MockTreeNode folder1 = new MockTreeNode();
        folder1.setType(TreeNode.FOLDER_TYPE);
        folder1.setName("Folder1");
        
        root.addNode(folder1);
        
        MockTreeNode folder2 = new MockTreeNode();
        folder2.setType(TreeNode.FOLDER_TYPE);
        folder2.setName("Folder2");
        
        root.addNode(folder2);
        
        assertNotNull(root.getNode(0));
        assertTrue(root.getNode(0) == folder1);
        
        assertNotNull(root.getNode(1));
        assertTrue(root.getNode(1) == folder2);
        
        MockTreeNode child1 = new MockTreeNode();
        child1.setType(TreeNode.ITEM_TYPE);
        child1.setName("Child1");
        
        folder1.addNode(child1);
        
        MockTreeNode child2 = new MockTreeNode();
        child2.setType(TreeNode.ITEM_TYPE);
        child2.setName("Child2");
        
        folder1.addNode(child2);
        
        MockTreeNode child3 = new MockTreeNode();
        child3.setType(TreeNode.ITEM_TYPE);
        child3.setName("Child3");
        
        folder1.addNode(child3);
        
        MockTreeNode newRoot = (MockTreeNode)root.clone();
        
        assertNotNull(newRoot);
        assertNotNull(newRoot.getChildren());
        assertTrue(newRoot.getChildren().size() == 2);
        
        assertTrue(newRoot.getParent() == null);
        assertNotNull(newRoot.getId());
        assertTrue(!root.getId().equals(newRoot.getId()));
        assertTrue(root.getName().equals(newRoot.getName()));
        
        assertTrue(((MockTreeNode)root.getNode(0)).getName().equals(
                ((MockTreeNode)newRoot.getNode(0)).getName()));
        
        assertTrue(((MockTreeNode)root.getNode(1)).getName().equals(
                ((MockTreeNode)newRoot.getNode(1)).getName()));
        
        assertTrue(newRoot.getNode(0).getParent() == newRoot);
        assertTrue(newRoot.getNode(1).getParent() == newRoot);
    }
    
    @Test
    public void testGetRoot()
    {
        MockTreeNode root = new MockTreeNode();
        root.setType(TreeNode.FOLDER_TYPE);
        root.setName("All");
        
        MockTreeNode folder1 = new MockTreeNode();
        folder1.setType(TreeNode.FOLDER_TYPE);
        folder1.setName("Folder1");
        
        root.addNode(folder1);
        
        MockTreeNode folder2 = new MockTreeNode();
        folder2.setType(TreeNode.FOLDER_TYPE);
        folder2.setName("Folder2");
        
        root.addNode(folder2);
        
        MockTreeNode child1 = new MockTreeNode();
        child1.setType(TreeNode.ITEM_TYPE);
        child1.setName("Child1");
        
        folder1.addNode(child1);
        
        MockTreeNode child2 = new MockTreeNode();
        child2.setType(TreeNode.ITEM_TYPE);
        child2.setName("Child2");
        
        folder1.addNode(child2);
        
        MockTreeNode child3 = new MockTreeNode();
        child3.setType(TreeNode.ITEM_TYPE);
        child3.setName("Child3");
        
        folder1.addNode(child3);
        
        assertTrue(child3.getParent() == folder1);
        assertTrue(child3.getChildren() == null);
        assertTrue(child3.getRoot() == root);
    }
    
    @Test
    public void testMaint()
    {
        MockTreeNode root = new MockTreeNode();
        root.setType(TreeNode.FOLDER_TYPE);
        root.setName("All");
        
        MockTreeNode folder1 = new MockTreeNode();
        folder1.setType(TreeNode.FOLDER_TYPE);
        folder1.setName("Folder1");
        
        root.addNode(folder1);
        
        MockTreeNode folder2 = new MockTreeNode();
        folder2.setType(TreeNode.FOLDER_TYPE);
        folder2.setName("Folder2");
        
        root.addNode(folder2);
        
        MockTreeNode child1 = new MockTreeNode();
        child1.setType(TreeNode.ITEM_TYPE);
        child1.setName("Child1");
        
        folder1.addNode(child1);
        
        MockTreeNode child2 = new MockTreeNode();
        child2.setType(TreeNode.ITEM_TYPE);
        child2.setName("Child2");
        
        folder1.addNode(child2);
        
        MockTreeNode child3 = new MockTreeNode();
        child3.setType(TreeNode.ITEM_TYPE);
        child3.setName("Child3");
        
        folder1.addNode(child3);
        
        assertTrue(child1.indexOf() == 0);
        assertTrue(child3.indexOf() == 2);
        
        MockTreeNode child4 = new MockTreeNode();
        child4.setType(TreeNode.ITEM_TYPE);
        child3.addNode(child4);
        
        assertTrue(child4.indexOf() == 2);
        assertTrue(child3.indexOf() == 3);
        
        MockTreeNode child5 = new MockTreeNode();
        child5.setType(TreeNode.ITEM_TYPE);
        child4.setNode(child5);
        
        assertTrue(child4.indexOf() == -1);
        assertTrue(child5.indexOf() == 2);
    }
    
    @Test
    public void testTree()
    {
        MockTreeNode root = new MockTreeNode();
        root.setType(TreeNode.FOLDER_TYPE);
        root.setName("All");
        
        assertTrue(root.getId() != null);
        
        MockTreeNode folder1 = new MockTreeNode();
        folder1.setType(TreeNode.FOLDER_TYPE);
        folder1.setName("Folder1");
        
        MockTreeNode folder2 = new MockTreeNode();
        folder2.setType(TreeNode.FOLDER_TYPE);
        folder2.setName("Folder2");
        
        assertTrue(root.addNode(folder1) == 0);
        
        assertTrue(root.addNode(folder2) == 1);
        
        assertTrue(root.getChildren().size() == 2);
        
        MockTreeNode child1 = new MockTreeNode();
        child1.setType(TreeNode.ITEM_TYPE);
        child1.setName("Child1");
        
        folder1.addNode(child1);
        
        MockTreeNode child2 = new MockTreeNode();
        child2.setType(TreeNode.ITEM_TYPE);
        child2.setName("Child2");
        
        folder1.addNode(child2);
        
        MockTreeNode child3 = new MockTreeNode();
        child3.setType(TreeNode.ITEM_TYPE);
        child3.setName("Child3");
        
        assertTrue(folder1.insertNode(child3, 1) == 1);
        
        assertTrue(folder1.getChildren().size() == 3);
        
        assertTrue(child3.deleteNode() == 1);
    }
    
}
