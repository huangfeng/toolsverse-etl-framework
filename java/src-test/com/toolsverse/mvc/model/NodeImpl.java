/*
 * NodeImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.util.List;

/**
 * NodeImpl
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class NodeImpl extends ModelImpl implements Node
{
    private Node _parent = null;
    private List<Node> _children = null;
    
    public List<Node> getChildren()
    {
        return _children;
    }
    
    public Node getParent()
    {
        return _parent;
    }
    
    public Node getRootNode()
    {
        if (getParent() == null)
            return this;
        else
            return (getParent()).getRootNode();
    }
    
    public void setChildren(List<Node> value)
    {
        _children = value;
        
    }
    
    public void setParent(Node value)
    {
        _parent = value;
    }
}
