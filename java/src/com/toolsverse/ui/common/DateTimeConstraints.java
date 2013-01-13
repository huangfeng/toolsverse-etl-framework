/*
 * DateTimeConstraints.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import com.toolsverse.util.Utils;

/**
 * This is a control constraints used by DateTime UI components. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class DateTimeConstraints extends ControlConstraints
{
    
    /** The str2date and date2str format. */
    private String _format;
    
    /**
     * Instantiates a new DateTimeConstraints.
     */
    public DateTimeConstraints()
    {
        setNullAllowed(true);
        
        _format = null;
    }
    
    /**
     * Instantiates a new DateTimeConstraints.
     *
     * @param isNullAllowed the "is null value allowed" flag
     * @param format the format
     */
    public DateTimeConstraints(boolean isNullAllowed, String format)
    {
        setNullAllowed(isNullAllowed);
        
        setFormat(format);
    }
    
    /**
     * Gets the str2date and date2str format.
     *
     * @return the format
     */
    public String getFormat()
    {
        return _format;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ui.common.ControlConstraints#isEmpty()
     */
    @Override
    public boolean isEmpty()
    {
        return Utils.isNothing(_format);
    }
    
    /**
     * Sets the str2date and date2str format.
     *
     * @param value the new format
     */
    public void setFormat(String value)
    {
        _format = value;
    }
}