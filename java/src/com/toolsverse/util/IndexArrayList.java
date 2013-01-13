/*
 * IndexArrayList.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.ArrayList;

/**
 * The ArrayList implementation of the IndexList interface
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class IndexArrayList<E> extends ArrayList<E> implements IndexList<E>
{
    
    /** The _selected index. */
    private int _selectedIndex = -1;
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.IndexList#getSelectedIndex()
     */
    public int getSelectedIndex()
    {
        return _selectedIndex;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.IndexList#getSelectedValue()
     */
    public E getSelectedValue()
    {
        return _selectedIndex >= 0 && _selectedIndex <= size() && size() > 0 ? get(_selectedIndex)
                : null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.IndexList#setSelectedIndex(int)
     */
    public void setSelectedIndex(int value)
    {
        _selectedIndex = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.IndexList#setSelectedValue(java.lang.Object)
     */
    public void setSelectedValue(E value)
    {
        if (_selectedIndex >= 0 && _selectedIndex <= size() && size() > 0)
            set(_selectedIndex, value);
    }
}
