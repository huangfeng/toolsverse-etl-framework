/*
 * ExceptionHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.exception;

/**
 * This is the interface for handlers that deal with all sort of exceptions. The particular implementations are usually container dependent. 
 * For example SwingExceptionHandler is used for the client Swing applications while WingsExceptionHandler - for the web based Wings apps. 
 * The correct implementation of the ExceptionHandler is automatically instantiated by the framework on start up but can be re configured using property files.
 *  
 * @see com.toolsverse.exception.DefaultExceptionHandler
 * @see com.toolsverse.util.log.Logger
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ExceptionHandler
{
    
    /**
     * Handles the exception. Usually logs exception and shows dialog box with the detail message and an option to show/hide stack trace.
     *
     * @param severity the severity
     * @param object the object
     * @param message the message
     * @param throwable the throwable
     */
    void handleException(int severity, Object object, String message,
            Throwable throwable);
    
    /**
     * Logs exception. Basically is a Logger.log(...)
     *
     * @param severity the severity
     * @param object the object
     * @param message the message
     * @param throwable the throwable
     */
    void logException(int severity, Object object, String message,
            Throwable throwable);
}
