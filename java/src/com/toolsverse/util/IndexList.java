/*
 * IndexList.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.util.List;

/**
 * This interface extends basic List by adding selected index and selected value.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public interface IndexList<E> extends List<E>
{
    
    /**
     * Gets the selected index.
     * 
     * @return the selected index
     */
    int getSelectedIndex();
    
    /**
     * Gets the selected value.
     * 
     * @return the selected value
     */
    E getSelectedValue();
    
    /**
     * Sets the selected index.
     * 
     * @param value the new selected index
     */
    void setSelectedIndex(int value);
    
    /**
     * Sets the selected value.
     * 
     * @param value the new selected value
     */
    void setSelectedValue(E value);
}
