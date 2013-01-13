/*
 * Filter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.filter;

/**
 * A filter is an object than perform filtering task on object implementing FilterContext interface.
 * The most common use is filtering objects based on execution context. For example "unit testing" vs "production".
 * Some objects can be designed to work in the "unit testing" mode only and should not be loaded in production.
 *
 * @see com.toolsverse.util.filter.FilterContext
 *  
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface Filter
{
    
    /**
     * Checks if particular object implementing FilterContext is appropriate in the current execution context.    
     *
     * @param context the FilterContext
     * @return true, if successful
     */
    boolean filter(FilterContext context);
}
