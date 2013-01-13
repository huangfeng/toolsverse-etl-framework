/*
 * BaseExceptionHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.exception;

import com.toolsverse.util.log.Logger;

/**
 * Abstract implementation of the ExceptionHandler. Only logException is implemented. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseExceptionHandler implements ExceptionHandler
{
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.exception.ExceptionHandler#handleException(int,
     * java.lang.Object, java.lang.String, java.lang.Throwable)
     */
    public abstract void handleException(int severity, Object object,
            String message, Throwable throwable);
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.exception.ExceptionHandler#logException(int,
     * java.lang.Object, java.lang.String, java.lang.Throwable)
     */
    public void logException(int severity, Object object, String message,
            Throwable throwable)
    {
        Logger.log(severity, object, message, throwable);
    }
}
