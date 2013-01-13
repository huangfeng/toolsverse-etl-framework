/*
 * NumberConstraints.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import com.toolsverse.util.Utils;

/**
 * The control constraints used by Number UI components.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class NumberConstraints extends ControlConstraints
{
    
    /** The str2Number and number2Str format. */
    private String _format;
    
    /** The maximum value. */
    private Number _maxValue;
    
    /** The minimum value. */
    private Number _minValue;
    
    /**
     * Instantiates a new NumberConstraints.
     */
    public NumberConstraints()
    {
        setNullAllowed(true);
        
        _format = null;
        _maxValue = null;
        _minValue = null;
    }
    
    /**
     * Instantiates a new NumberConstraints.
     *
     * @param isNullAllowed the "is null value allowed" flag
     * @param format the str2Number and number2Str format. Can be null
     * @param minValue the minimum value
     * @param maxValue the maximum value
     */
    public NumberConstraints(boolean isNullAllowed, String format,
            Number minValue, Number maxValue)
    {
        setNullAllowed(isNullAllowed);
        
        setFormat(format);
        
        _minValue = minValue;
        
        _maxValue = maxValue;
    }
    
    /**
     * Gets the str2Number and number2Str format. Can be null.
     *
     * @return the format
     */
    public String getFormat()
    {
        return _format;
    }
    
    /**
     * Gets the maximum value.
     *
     * @return the maximum value
     */
    public Number getMaxValue()
    {
        return _maxValue;
    }
    
    /**
     * Gets the minimum value.
     *
     * @return the minimum value
     */
    public Number getMinValue()
    {
        return _minValue;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ui.common.ControlConstraints#isEmpty()
     */
    @Override
    public boolean isEmpty()
    {
        return Utils.isNothing(_format) && _minValue == null
                && _maxValue == null;
    }
    
    /**
     * Sets the str2Number and number2Str format. Can be null.
     *
     * @param value the new format
     */
    public void setFormat(String value)
    {
        _format = value;
    }
    
    /**
     * Sets the maximum value.
     *
     * @param value the new maximum value
     */
    public void setMaxValue(Number value)
    {
        _maxValue = value;
    }
    
    /**
     * Sets the minimum value.
     *
     * @param value the new minimum value
     */
    public void setMinValue(Number value)
    {
        _minValue = value;
    }
}