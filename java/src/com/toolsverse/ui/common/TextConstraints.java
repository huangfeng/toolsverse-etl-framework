/*
 * TextConstraints.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

/**
 * The control constraints used by Text UI components.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class TextConstraints extends ControlConstraints
{
    
    /** The minimum length. */
    private int _minLength;
    
    /** The maximum length. */
    private int _maxLength;
    
    /**
     * Instantiates a new TextConstraints.
     */
    public TextConstraints()
    {
        setNullAllowed(true);
        
        _minLength = -1;
        _maxLength = -1;
    }
    
    /**
     * Instantiates a new TextConstraints.
     *
     * @param isNullAllowed the "is null value allowed" flag.
     * @param minLength the minimum length
     * @param maxLength the maximum length
     */
    public TextConstraints(boolean isNullAllowed, int minLength, int maxLength)
    {
        setNullAllowed(isNullAllowed);
        
        _minLength = minLength;
        
        _maxLength = maxLength;
    }
    
    /**
     * Gets the maximum length.
     *
     * @return the maximum length
     */
    public int getMaxLength()
    {
        return _maxLength;
    }
    
    /**
     * Gets the minimum length.
     *
     * @return the minimum length
     */
    public int getMinLength()
    {
        return _minLength;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ui.common.ControlConstraints#isEmpty()
     */
    @Override
    public boolean isEmpty()
    {
        return _minLength < 0 && _maxLength <= 0;
    }
    
    /**
     * Sets the maximum length.
     *
     * @param value the new maximum length
     */
    public void setMaxLength(int value)
    {
        _maxLength = value;
    }
    
    /**
     * Sets the minimum length.
     *
     * @param value the new minimum length
     */
    public void setMinLength(int value)
    {
        _minLength = value;
    }
    
}