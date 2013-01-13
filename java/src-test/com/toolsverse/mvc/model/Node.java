/*
 * Node.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.util.List;

/**
 * Node
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Node extends Model
{
    List<Node> getChildren();
    
    Node getParent();
    
    Node getRootNode();
    
    void setChildren(List<Node> value);
    
    void setParent(Node value);
    
}
