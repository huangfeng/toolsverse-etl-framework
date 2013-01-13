/*
 * ServiceFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

import java.lang.reflect.Modifier;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * The ServiceFactory is a static class that provides a factory for the creation of instances of the type com.toolsverse.service.Service.
 * This enables a client to create a Service instance in a portable manner without using the constructor of the Service implementation class.
 * Typically the particular implementation of the Service binded to it's corresponding interface in the configuration file. It is also possible to call service
 * methods using different ServiceProcy implementations based on the execution mode (client, web, client-server). 
 * 
 * <p>
 * In the example below SqlService binded to the SqlServiceImpl, ServiceProxyWeb is used if app.server.url is not null, otherwise ServiceProxyLocal:
 * <p><pre>
 * com.toolsverse.etl.sql.service.SqlService=com.toolsverse.etl.sql.service.SqlServiceImpl
 * com.toolsverse.etl.sql.service.SqlService.proxy.client=@decode(app.server.url,@null,@com.toolsverse.service.ServiceProxyLocal,@com.toolsverse.service.web.ServiceProxyWeb
 * </pre>
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ServiceFactory
{
    // props
    /** GLOBAL SERVICE PROXY property. */
    public static final String GLOBAL_SERVICE_PROXY_PROP = "service_proxy";
    
    // defaults
    /** The default IMPL suffix */
    protected static final String IMPL = "Impl";
    
    /** PROXY_TYPE suffix. */
    protected static final String PROXY_TYPE = ".proxy";
    
    /**
     * Gets the service implementation using given serviceClass.
     *
     * @param serviceClass the service class
     * @return the service
     * @throws Exception in case of any error
     */
    public static Service getService(Class<? extends Service> serviceClass)
        throws Exception
    {
        return getService(serviceClass, null);
    }
    
    /**
     * Gets the service implementation using given service class, default implementation and a service proxy class name.
     *
     * @param serviceClass the service class
     * @param impl the default implementation class name
     * @param proxyClassName the service proxy class name
     * @return the service
     * @throws Exception in case of any error
     */
    public static Service getService(Class<? extends Service> serviceClass,
            Class<? extends Service> impl, String proxyClassName)
        throws Exception
    {
        return getServiceProxy(proxyClassName, serviceClass, impl).toService();
    }
    
    /**
     * Gets the service implementation using given service class and service proxy class name.
     *
     * @param serviceClass the service class
     * @param proxyClassName the service proxy class name
     * @return the service
     * @throws Exception in case of any error
     */
    public static Service getService(Class<? extends Service> serviceClass,
            String proxyClassName)
        throws Exception
    {
        return getService(serviceClass, getServiceImplClass(serviceClass),
                proxyClassName);
    }
    
    /**
     * Gets the service implementation class using given service class. If there is no configured binding uses serviceClass.getName() + "Impl" to calculate a class name.
     *
     * @param serviceClass the service class
     * @return the service implementation class
     * @throws Exception in case of any error
     */
    @SuppressWarnings("unchecked")
    private static Class<? extends Service> getServiceImplClass(
            Class<? extends Service> serviceClass)
        throws Exception
    {
        int modifiers = serviceClass.getModifiers();
        
        if (!Modifier.isAbstract(modifiers) && !Modifier.isInterface(modifiers))
            return serviceClass;
        
        String implClass = null;
        
        String deployment = SystemConfig.instance().getSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY);
        
        if (!Utils.isNothing(deployment))
        {
            implClass = SystemConfig.instance().getSystemProperty(
                    serviceClass.getName() + "." + deployment);
        }
        
        if (Utils.isNothing(implClass))
        {
            implClass = SystemConfig.instance().getSystemProperty(
                    serviceClass.getName());
        }
        
        if (Utils.isNothing(implClass))
            implClass = serviceClass.getName() + IMPL;
        
        return (Class<? extends Service>)Class.forName(implClass);
    }
    
    /**
     * Gets the service proxy instance using given service proxy class.
     *
     * @param proxyClass the service proxy class
     * @return the service proxy
     * @throws Exception in case of any error
     */
    private static ServiceProxy getServiceProxy(
            Class<? extends ServiceProxy> proxyClass)
        throws Exception
    {
        int modifiers = proxyClass.getModifiers();
        
        if (Modifier.isAbstract(modifiers))
            return null;
        
        return proxyClass.newInstance();
    }
    
    /**
     * Gets the service proxy using given proxy class name, service class and default service implementation.
     *
     * @param proxyClassName the proxy class name
     * @param serviceClass the service class
     * @param impl the default implementation 
     * @return the service proxy
     * @throws Exception in case of any error
     */
    public static ServiceProxy getServiceProxy(String proxyClassName,
            Class<? extends Service> serviceClass, Class<? extends Service> impl)
        throws Exception
    {
        ServiceProxy serviceProxy = getServiceProxy(getServiceProxyClass(
                proxyClassName, serviceClass != null ? serviceClass.getName()
                        : null));
        
        if (impl == null)
            serviceProxy.setServiceClass(getServiceImplClass(serviceClass));
        else
            serviceProxy.setServiceClass(impl);
        
        return serviceProxy;
    }
    
    /**
     * Gets the service proxy class using given proxy class name and a service class name.
     * 
     * @param type the proxy class name
     * @param serviceClassName the service class name
     * @return the service proxy class
     * @throws Exception in case of any error
     */
    @SuppressWarnings("unchecked")
    private static Class<? extends ServiceProxy> getServiceProxyClass(
            String proxyClassName, String serviceClassName)
        throws Exception
    {
        if (Utils.isNothing(proxyClassName))
        {
            String deployment = SystemConfig.instance().getDeploymentType();
            
            proxyClassName = SystemConfig.instance().getSystemProperty(
                    serviceClassName + PROXY_TYPE + "." + deployment);
            
            if (Utils.isNothing(proxyClassName))
            {
                proxyClassName = SystemConfig.instance().getSystemProperty(
                        serviceClassName + PROXY_TYPE);
                
                if (Utils.isNothing(proxyClassName))
                {
                    proxyClassName = SystemConfig.instance().getSystemProperty(
                            GLOBAL_SERVICE_PROXY_PROP + "." + deployment);
                }
                
                if (Utils.isNothing(proxyClassName))
                {
                    proxyClassName = SystemConfig.instance().getSystemProperty(
                            GLOBAL_SERVICE_PROXY_PROP);
                }
            }
        }
        
        if (Utils.isNothing(proxyClassName))
            proxyClassName = ServiceProxyLocal.class.getName();
        
        return (Class<? extends ServiceProxy>)Class.forName(proxyClassName);
    }
    
}
