/*
 * MockComponentAdapter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;


/**
 * MockComponentAdapter
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class MockComponentAdapter extends ComponentAdapter
{
    public MockComponentAdapter(View view, Object component)
    {
        super(view, component);
    }
    
    @Override
    public String getName()
    {
        MockComponent comp = (MockComponent)getComponent();
        
        return comp != null ? comp.getName() : "";
    }
    
    @Override
    public void invalidate()
    {
    }
}
