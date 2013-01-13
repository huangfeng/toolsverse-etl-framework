/*
 * ServiceResponse.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

/**
 * The ServiceResponse is used if translation is required between request and response protocols (for example for Web services).
 * It is serializable across the wire and includes result of the service execution and Throwable if there was any exception. 
 * The proxy uses ServiceResponse to return result to the client.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ServiceResponse implements java.io.Serializable
{
    
    /** The result. */
    private Object _result;
    
    /** The throwable. */
    private Throwable _throwable;
    
    /**
     * Instantiates a new ServiceResponse.
     *
     * @param result the result
     * @param throwable the throwable
     */
    public ServiceResponse(Object result, Throwable throwable)
    {
        _result = result;
        _throwable = throwable;
    }
    
    /**
     * Gets the result.
     *
     * @return the result
     */
    public Object getResult()
    {
        return _result;
    }
    
    /**
     * Gets the throwable.
     *
     * @return the throwable
     */
    public Throwable getThrowable()
    {
        return _throwable;
    }
    
}
