/*
 * DefaultExceptionHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.exception;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Container independent ExceptionHandler. Based on execution environment (client or web) automatically instantiates correct implementation of the ExceptionHandler
 * and using itself as a wrapper calls its methods. 
 * 
 * <p>
 * Example:
 * <p><pre class="brush: java">
 * DefaultExceptionHandler.instance().handleException(
 *                      Logger.SEVERE,
 *                      this,
 *                      TestConnectionResource.ERROR_TESTING_CONNECTION
 *                              .getValue(), ex);
 * </pre>
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class DefaultExceptionHandler implements ExceptionHandler
{
    
    /** The instance of the DefaultExceptionHandler. */
    private volatile static DefaultExceptionHandler _instance;
    
    /**
     * Returns an singleton instance the DefaultExceptionHandler
     *
     * @return the default exception handler
     */
    public static DefaultExceptionHandler instance()
    {
        if (_instance == null)
        {
            synchronized (DefaultExceptionHandler.class)
            {
                if (_instance == null)
                    _instance = new DefaultExceptionHandler();
            }
        }
        
        return _instance;
    }
    
    /** ExceptionHandler. */
    private ExceptionHandler _exceptionHandler;
    
    /**
     * Instantiates a new DefaultExceptionHandler.
     */
    private DefaultExceptionHandler()
    {
        try
        {
            _exceptionHandler = (ExceptionHandler)ObjectFactory.instance().get(
                    ExceptionHandler.class.getName(), ExceptionHandler.class);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            _exceptionHandler = null;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.exception.ExceptionHandler#handleException(int,
     * java.lang.Object, java.lang.String, java.lang.Throwable)
     */
    public void handleException(int severity, Object object, String message,
            Throwable throwable)
    {
        logException(severity, object, message, throwable);
        
        if (_exceptionHandler != null)
            _exceptionHandler.handleException(severity, object, message,
                    throwable);
    }
    
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
