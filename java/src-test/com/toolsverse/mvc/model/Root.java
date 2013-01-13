/*
 * Root.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

/**
 * Root
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Root extends NodeImpl
{
    public static final String CURRENT_NODE = "currentnode";
    
    @Getter(name = CURRENT_NODE)
    public Node getCurrentNode()
    {
        return (Node)getAttributeValue(CURRENT_NODE);
    }
    
    @Setter(name = CURRENT_NODE)
    public void setCurrentNode(Node value)
    {
        setAttributeValue(CURRENT_NODE, value);
    }
}
