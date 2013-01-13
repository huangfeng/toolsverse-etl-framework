/*
 * Level2Model.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

/**
 * Level2Model
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Level2Model extends ModelImpl
{
    public static final String LEVEL2_ATTR = "level2attr";
    
    @Getter(name = LEVEL2_ATTR)
    public String getLevel2Attr()
    {
        return (String)getAttributeValue(LEVEL2_ATTR);
    }
    
    @Setter(name = LEVEL2_ATTR)
    public void setLevel2Attr(String value)
    {
        setAttributeValue(LEVEL2_ATTR, value);
    }
}
