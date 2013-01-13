/*
 * MockTreeNode.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

/**
 * MockTreeNode
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockTreeNode extends TreeNode
{
    private String _name;
    private String _id;
    
    public MockTreeNode()
    {
        _id = Utils.getUUIDName();
    }
    
    @Override
    public TreeNode createNode(TreeNode node)
    {
        MockTreeNode mockTreeNode = new MockTreeNode();
        
        mockTreeNode.setType(node.getType());
        
        mockTreeNode.setName(((MockTreeNode)node).getName());
        
        return mockTreeNode;
    }
    
    @Override
    public String getDisplayValue()
    {
        return getName();
    }
    
    @Override
    public String getIconPath()
    {
        return null;
    }
    
    public String getId()
    {
        return _id;
    }
    
    public String getName()
    {
        return _name;
    }
    
    public void setId(String value)
    {
        _id = value;
    }
    
    public void setName(String value)
    {
        _name = value;
    }
    
}
