/*
 * ControlConstraints.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.io.Serializable;

/**
 * This is a base class for the control constraints. Such as "is null allowed", "maximum length", etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ControlConstraints implements Serializable
{
    
    /** The is null allowed flag. */
    private boolean _isNullAllowed = true;
    
    /**
     * Instantiates a new ControlConstraints.
     */
    public ControlConstraints()
    {
        _isNullAllowed = true;
    }
    
    /**
     * Checks if ControlConstraints is empty - no settings other than default.  
     *
     * @return true, if is empty
     */
    public boolean isEmpty()
    {
        return true;
    }
    
    /**
     * Checks if is null value allowed.
     *
     * @return true, if is null value allowed
     */
    public boolean isNullAllowed()
    {
        return _isNullAllowed;
    }
    
    /**
     * Sets the "is null value allowed" flag.
     *
     * @param value the new "is null value allowed" flag
     */
    public void setNullAllowed(boolean value)
    {
        _isNullAllowed = value;
    }
}