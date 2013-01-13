/*
 * ServiceRequest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.lang.reflect.Method;

import com.toolsverse.security.SecurityContext;

/**
 * The ServiceRequest is used if translation is required between request and response protocols (for example for Web services).
 * It is serializable across the wire and includes service, method to be executed, arguments to be passed to the method and current SercurityContext. 
 * The receiving side uses ServiceRequest to create a local ServiceProcy and execute particular method.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ServiceRequest implements Serializable
{
    
    /** The method. */
    private Method _method;
    
    /** The args. */
    private Object[] _args;
    
    /** The service class. */
    private Class<? extends Service> _service;
    
    /** The security context. */
    private SecurityContext _securityContext;
    
    /**
     * Instantiates a new ServiceRequest.
     *
     * @param method the method
     * @param args the args
     * @param service the service
     * @param securityContext the security context
     */
    public ServiceRequest(Method method, Object[] args,
            Class<? extends Service> service, SecurityContext securityContext)
    {
        _method = method;
        
        _args = args;
        
        _service = service;
        
        _securityContext = securityContext;
    }
    
    /**
     * Gets the arguments.
     *
     * @return the arguments
     */
    public Object[] getArgs()
    {
        return _args;
    }
    
    /**
     * Gets the method.
     *
     * @return the method
     */
    public Method getMethod()
    {
        return _method;
    }
    
    /**
     * Gets the security context.
     *
     * @return the security context
     */
    public SecurityContext getSecurityContext()
    {
        return _securityContext;
    }
    
    /**
     * Gets the service class.
     *
     * @return the service class
     */
    public Class<? extends Service> getServiceClass()
    {
        return _service;
    }
    
    /**
     * Reads and object.
     *
     * @param ois the ObjectInputStream
     * @throws ClassNotFoundException the class not found exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    @SuppressWarnings("unchecked")
    private void readObject(ObjectInputStream ois)
        throws ClassNotFoundException, IOException
    {
        _args = (Object[])ois.readObject();
        _service = (Class<? extends Service>)ois.readObject();
        
        try
        {
            _method = _service.getMethod((String)ois.readObject(),
                    (Class[])ois.readObject());
        }
        catch (Exception ex)
        {
            throw new IOException(ex);
            
        }
        
        _securityContext = (SecurityContext)ois.readObject();
    }
    
    /**
     * Writes an object.
     *
     * @param oos the ObjectOutputStream
     * @throws IOException Signals that an I/O exception has occurred.
     */
    private void writeObject(ObjectOutputStream oos)
        throws IOException
    {
        oos.writeObject(_args);
        oos.writeObject(_service);
        oos.writeObject(_method.getName());
        oos.writeObject(_method.getParameterTypes());
        oos.writeObject(_securityContext);
    }
}
