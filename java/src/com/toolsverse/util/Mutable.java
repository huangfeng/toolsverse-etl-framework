/*
 * Mutable.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

/**
 * An interface that represents and controls the state of the mutable object. The possible states are:
 * <br><br>
 * <b>dirty</b> - object has changed
 * <br>
 * <b>baselined</b> - object has not been changed or are in the most recent coherent state.      
 * <br><br>
 * An example would be a controller class which makes button enabled when model has changed 
 * (has a dirty status) and disables it when model has been saved on the hard drive and status changed on 
 * baselined.  
 *    
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public interface Mutable
{
    
    /**
     * Sets the state of the mutable object to baselined.  
     */
    void baseline();
    
    /**
     * Checks if object has changed but hasn't been baselined.   
     * 
     * @return true, if object is dirty
     */
    boolean isDirty();
    
    /**
     * Sets the state of the object to dirty or baselined
     * 
     * @param value if <code>true</code> the state of the object will be set to dirty, otherwise to baselined.
     */
    void setDirty(boolean value);
}