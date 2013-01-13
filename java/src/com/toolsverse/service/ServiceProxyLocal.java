/*
 * ServiceProxyLocal.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Local implementation of the ServiceProxy class. Basically there is no translation between local and remote end points.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ServiceProxyLocal extends ServiceProxy
{
    
    /** The service. */
    private Service _service = null;
    
    /**
     * Gets the service.
     *
     * @return the service
     * @throws Exception in case of any error
     */
    private Service getService()
        throws Exception
    {
        if (_service != null)
            return _service;
        
        _service = getServiceClass().newInstance();
        
        return _service;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.service.ServiceProxy#invoke(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[])
     */
    @Override
    public Object invoke(Object proxy, Method m, Object[] args)
        throws Throwable
    {
        try
        {
            return m.invoke(getService(), args);
        }
        catch (Throwable throwable)
        {
            if (throwable instanceof InvocationTargetException
                    && ((InvocationTargetException)throwable)
                            .getTargetException() != null)
                throw ((InvocationTargetException)throwable)
                        .getTargetException();
            else
                throw throwable;
        }
    }
}
