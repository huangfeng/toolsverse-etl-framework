/*
 * Function.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

/**
 * Defines common set of methods for the extension functions. Functions are used by etl framework to calculate Variable value at run time, set visibility, etc. 
 * The class which implements <code>Function</code> interface usually includes multiple functions, for example: getValue(), getPrimaryKey(), so basically
 * it is a library of functions.        
 * 
 * @see com.toolsverse.etl.common.Variable
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface Function
{
    
    /**
     * Gets the array of functions which will be executed after main etl action (insert, update, delete, merge, etc).
     *
     * @return the array of "after" functions
     */
    String[] getAfterFunctions();
    
    /**
     * Gets the array of functions which will be executed before main etl action (insert, update, delete, merge, etc).
     *
     * @return the array of "before" functions
     */
    String[] getBeforeFunctions();
    
    /**
     * Gets the array of functions which will be executed to calculate visibility of the field. Can be used to exclude field from 
     * the main etl action (insert, update, delete, merge, etc).   
     *
     * @return the array of "exclude" functions
     */
    String[] getExcludeFunctions();
    
    /**
     * Gets the array of functions which will be executed at run time. Use them to calculate field values.
     *
     * @return the array of "runtime" functions
     */
    String[] getRuntimeFunctions();
}
