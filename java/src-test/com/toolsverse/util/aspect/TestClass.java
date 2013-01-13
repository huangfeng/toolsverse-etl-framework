/*
 * TestClass.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.aspect;

/**
 * TestClass
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TestClass
{
    protected String _value;
    
    public TestClass()
    {
        _value = null;
    }
    
    protected String doSomething(String value)
    {
        return null;
    }
    
    public String getSomething()
    {
        return _value;
    }
    
    public void setSomething(String value)
    {
        _value = value;
    }
    
}
