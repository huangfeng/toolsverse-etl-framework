/*
 * PojoModel.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

/**
 * PojoModel
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class PojoModel
{
    public static final String BOOLEAN_ATTR = "booleanattr";
    public static final String INT_ATTR = "intattr";
    public static final String STR_ATTR = "strattr";
    
    private boolean _booleanValue;
    private int _intValue;
    private String _strValue;
    
    public PojoModel()
    {
        _booleanValue = Boolean.FALSE;
        _intValue = -1;
        _strValue = null;
    }
    
    @Getter(name = BOOLEAN_ATTR)
    public boolean getBoolean()
    {
        return _booleanValue;
    }
    
    @Getter(name = INT_ATTR)
    public int getInt()
    {
        return _intValue;
    }
    
    @Getter(name = STR_ATTR)
    public String getStr()
    {
        return _strValue;
    }
    
    @Setter(name = BOOLEAN_ATTR)
    public void setBoolean(boolean value)
    {
        _booleanValue = value;
    }
    
    @Setter(name = INT_ATTR)
    public void setInt(int value)
    {
        _intValue = value;
    }
    
    @Setter(name = STR_ATTR)
    public void setStr(String value)
    {
        _strValue = value;
    }
    
}