/*
 * BaseFilterContext.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.filter;

/**
 * The abstract implementation of the FilterContext interface.  
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseFilterContext implements FilterContext
{
    
    /** The _context. */
    private String _context;
    
    /**
     * Sets the context.
     *
     * @param value the new context
     */
    public void setContext(String value)
    {
        _context = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return _context;
    }
}
