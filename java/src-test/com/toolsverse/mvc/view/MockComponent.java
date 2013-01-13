/*
 * MockComponent.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

/**
 * MockComponent
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class MockComponent
{
    private String _name;
    
    public String getName()
    {
        return _name;
    }
    
    public void setName(String name)
    {
        _name = name;
    }
}
