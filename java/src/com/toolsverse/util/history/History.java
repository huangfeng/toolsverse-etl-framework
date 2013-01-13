/*
 * History.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.history;

import java.util.List;

/**
 * The history of navigation in the data structures such as List, Tree, etc. Each time element in the data structure is visited the reference is stored in the History.
 * It is them possible to navigate back and forth by calling goBack() and goForward(). The history has a limited size. When new element increases the 
 * size to maxSize + 1 the first visited element is removed.     
 *
 * @param <V> the value type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface History<V>
{
    
    /**
     * Check if it is possible to go back starting from the current element.  
     *
     * @param current the current element
     * @return true, if successful
     */
    boolean canGoBack(V current);
    
    /**
     * Check if it is possible to go forward starting from the current element.  
     *
     * @param current the current element
     * @return true, if successful
     */
    boolean canGoForward(V current);
    
    /**
     * Clears the history 
     */
    void clear();
    
    /**
     * Checks if history contains the elements.
     *
     * @param element the element to check
     * @return true, if successful
     */
    boolean contains(V element);
    
    /**
     * Gets the list of elements.
     *
     * @return the list
     */
    List<V> elements();
    
    /**
     * Returns element of the history which was visited prior to current. 
     * 
     * @param current the current element
     * @return the element of the history which was visited prior to current
     */
    V goBack(V current);
    
    /**
     * Returns element of the history which was visited after the current. 
     * 
     * @param current the current element
     * @return the element of the history which was visited after the current
     */
    V goForward(V current);
    
    /**
     * Removes the element for the history.
     *
     * @param element the element
     */
    void remove(V element);
    
    /**
     * Sets the maximum size of the history. 
     *
     * @param value the new max size
     */
    void setMaxSize(int value);
    
    /**
     * Returns the current size of the history.
     *
     * @return the current size of the history
     */
    int size();
    
    /**
     * Visits the element. 
     *
     * @param element the element
     */
    void visit(V element);
}
