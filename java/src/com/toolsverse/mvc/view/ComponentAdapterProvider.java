/*
 * ComponentAdapterProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

/**
 * Defines the interface through which a component may publish its own component
 * adapters for use the in MVC framework. When a component that implements this
 * interface is registered with a component view, its
 * <code>getComponentAdapter</code> will be invoked and used in place of any
 * default adapter implementation that may exist. 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ComponentAdapterProvider
{
    
    /**
     * Gets the component adapter.
     *
     * @param view the view
     * @return the component adapter
     */
    ComponentAdapter getComponentAdapter(View view);
    
}
