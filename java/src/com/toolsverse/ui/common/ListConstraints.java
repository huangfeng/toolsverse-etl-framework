/*
 * ListConstraints.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.util.List;

import com.toolsverse.util.KeyValue;

/**
 * The control constraints used by List UI components.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ListConstraints extends ControlConstraints
{
    
    /** The list. */
    private List<KeyValue> _list;
    
    /** The provider class name. */
    private String _providerClass;
    
    /**
     * Instantiates a new ListConstraints.
     */
    public ListConstraints()
    {
        setNullAllowed(true);
        
    }
    
    /**
     * Instantiates a new ListConstraints.
     *
     * @param isNullAllowed the "is null value allowed" flag
     * @param list the list
     * @param providerClass the list provider class name
     */
    public ListConstraints(boolean isNullAllowed, List<KeyValue> list,
            String providerClass)
    {
        setNullAllowed(isNullAllowed);
        
        _list = list;
        
        _providerClass = providerClass;
    }
    
    /**
     * Gets the list.
     *
     * @return the list
     */
    public List<KeyValue> getList()
    {
        return _list;
    }
    
    /**
     * Gets the list provider class name.
     *
     * @return the list provider class name
     */
    public String getProviderClass()
    {
        return _providerClass;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ui.common.ControlConstraints#isEmpty()
     */
    @Override
    public boolean isEmpty()
    {
        return _list == null || _list.size() == 0;
    }
    
    /**
     * Sets the list.
     *
     * @param value the new list
     */
    public void setList(List<KeyValue> value)
    {
        _list = value;
    }
    
    /**
     * Sets the list provider class name.
     *
     * @param value the new list provider class name
     */
    public void setProviderClass(String value)
    {
        _providerClass = value;
    }
}