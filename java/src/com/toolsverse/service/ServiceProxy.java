/*
 * ServiceProxy.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Method;

/**
 * The basis of each element in the service proxy stack. A service proxy is a service which translates service calls between two different client-service protocols.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class ServiceProxy implements InvocationHandler
{
    
    /** The service class */
    private Class<? extends Service> _service;
    
    /**
     * Gets the service class.
     *
     * @return the service class
     */
    public Class<? extends Service> getServiceClass()
    {
        return _service;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.reflect.InvocationHandler#invoke(java.lang.Object,
     * java.lang.reflect.Method, java.lang.Object[])
     */
    public abstract Object invoke(Object proxy, Method m, Object[] args)
        throws Throwable;
    
    /**
     * Sets the service class.
     *
     * @param service the new service class
     */
    public void setServiceClass(Class<? extends Service> service)
    {
        _service = service;
    }
    
    /**
     * Creates and returns a java.lang.reflect.Proxy instance using given Service class.
     *
     * @return the service
     * @throws Exception in case of any error
     */
    public Service toService()
        throws Exception
    {
        return (Service)java.lang.reflect.Proxy.newProxyInstance(
                getServiceClass().getClassLoader(), getServiceClass()
                        .getInterfaces(), this);
    }
}
