/*
 * Level1Node.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import com.toolsverse.util.Utils;

/**
 * Level1Node
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Level1Node extends NodeImpl
{
    // dictionary
    public static final String NODE_NAME = "nodename";
    public static final String LEVEL1_ATTR = "level1attr";
    public static final String LEVEL2_NODE = "level2node";
    public static final String BOOLEAN_ATTR = "booleanattr";
    public static final String INT_ATTR = "intattr";
    
    public Level1Node()
    {
        addAttribute(INT_ATTR, Integer.class, "getIntAttr", "setIntAttr");
    }
    
    @Getter(name = BOOLEAN_ATTR)
    public Boolean getBooleanAttr()
    {
        return (Boolean)getAttributeValue(BOOLEAN_ATTR);
    }
    
    public Integer getIntAttr()
    {
        return (Integer)getAttributeValue(INT_ATTR);
    }
    
    @Getter(name = LEVEL1_ATTR)
    public String getLevel1Attr()
    {
        return (String)getAttributeValue(LEVEL1_ATTR);
    }
    
    @Getter(name = LEVEL2_NODE)
    public Level2Model getLevel2Model()
    {
        return (Level2Model)getAttributeValue(LEVEL2_NODE);
    }
    
    @Getter(name = NODE_NAME)
    public String getName()
    {
        return (String)getAttributeValue(NODE_NAME);
    }
    
    @Reader(name = BOOLEAN_ATTR)
    public Object readBooleanAttr(Object value)
    {
        return Utils.str2Boolean(Utils.makeString(value), null);
    }
    
    @Setter(name = BOOLEAN_ATTR)
    public void setBooleanAttr(Boolean value)
    {
        setAttributeValue(BOOLEAN_ATTR, value);
    }
    
    public void setIntAttr(Integer value)
    {
        setAttributeValue(INT_ATTR, value);
    }
    
    @Setter(name = LEVEL1_ATTR)
    public void setLevel1Attr(String value)
    {
        setAttributeValue(LEVEL1_ATTR, value);
    }
    
    @Setter(name = LEVEL2_NODE)
    public void setLevel2Node(Level2Model value)
    {
        setAttributeValue(LEVEL2_NODE, value);
    }
    
    @Setter(name = NODE_NAME)
    public void setName(String value)
    {
        setAttributeValue(NODE_NAME, value);
    }
    
    @Writer(name = BOOLEAN_ATTR)
    public Object writeBooleanAttr()
    {
        Boolean value = getBooleanAttr();
        
        return value != null ? value.toString() : null;
    }
}
