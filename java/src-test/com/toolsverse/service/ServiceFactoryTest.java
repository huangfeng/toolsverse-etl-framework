/*
 * ServiceFactoryTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

import com.toolsverse.config.SystemConfig;

/**
 * ServiceFactoryTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ServiceFactoryTest
{
    @Test
    public void testGetClientService()
        throws Exception
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                MockServiceClient.class.getName());
        
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                MockServiceServer.class.getName());
        
        MockService mockService = (MockService)ServiceFactory
                .getService(MockService.class);
        
        assertNotNull(mockService);
        
        assertTrue(mockService.doSomething().equals(
                MockServiceClient.class.getName()));
    }
    
    @Test
    public void testGetClientServiceProxy()
        throws Exception
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + ServiceFactory.PROXY_TYPE, null);
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                MockClientServiceProxy.class.getName());
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                MockServerServiceProxy.class.getName());
        
        ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(null,
                MockService.class, null);
        
        assertNotNull(serviceProxy);
        
        assertTrue(MockClientServiceProxy.class.isInstance(serviceProxy));
    }
    
    @Test
    public void testGetConfiguredServiceProxy()
        throws Exception
    {
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + ServiceFactory.PROXY_TYPE,
                MockServiceProxy.class.getName());
        
        ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(null,
                MockService.class, null);
        
        assertNotNull(serviceProxy);
        
        assertTrue(MockServiceProxy.class.isInstance(serviceProxy));
    }
    
    @Test
    public void testGetDefaultService()
        throws Exception
    {
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT, null);
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(MockService.class.getName(),
                MockServiceDefault.class.getName());
        
        MockService mockService = (MockService)ServiceFactory
                .getService(MockService.class);
        
        assertNotNull(mockService);
        
        assertTrue(mockService.doSomething().equals(
                MockServiceDefault.class.getName()));
    }
    
    @Test
    public void testGetGlobalServiceProxy()
        throws Exception
    {
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.CLIENT_DEPLOYMENT, null);
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.CLIENT_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + ServiceFactory.PROXY_TYPE, null);
        
        SystemConfig.instance().bind(ServiceFactory.GLOBAL_SERVICE_PROXY_PROP,
                MockGlobalServiceProxy.class.getName());
        
        ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(null,
                MockService.class, null);
        
        assertNotNull(serviceProxy);
        
        assertTrue(MockGlobalServiceProxy.class.isInstance(serviceProxy));
    }
    
    @Test
    public void testGetServerService()
        throws Exception
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.SERVER_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                MockServiceClient.class.getName());
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.SERVER_DEPLOYMENT);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                MockServiceServer.class.getName());
        
        MockService mockService = (MockService)ServiceFactory
                .getService(MockService.class);
        
        assertNotNull(mockService);
        
        assertTrue(mockService.doSomething().equals(
                MockServiceServer.class.getName()));
    }
    
    @Test
    public void testGetServerServiceProxy()
        throws Exception
    {
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.SERVER_DEPLOYMENT);
        
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + ServiceFactory.PROXY_TYPE, null);
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.CLIENT_DEPLOYMENT,
                MockClientServiceProxy.class.getName());
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.SERVER_DEPLOYMENT,
                MockServerServiceProxy.class.getName());
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY,
                SystemConfig.SERVER_DEPLOYMENT);
        
        ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(null,
                MockService.class, null);
        
        assertNotNull(serviceProxy);
        
        assertTrue(MockServerServiceProxy.class.isInstance(serviceProxy));
    }
    
    @Test
    public void testGetService()
        throws Exception
    {
        SystemConfig.instance().bind(
                MockService.class.getName() + "."
                        + SystemConfig.CLIENT_DEPLOYMENT, null);
        
        SystemConfig.instance().bind(
                ServiceFactory.GLOBAL_SERVICE_PROXY_PROP + "."
                        + SystemConfig.CLIENT_DEPLOYMENT, null);
        
        SystemConfig.instance().bind(SystemConfig.DEPLOYMENT_PROPERTY, null);
        
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        MockService mockService = (MockService)ServiceFactory
                .getService(MockService.class);
        
        assertNotNull(mockService);
        
        assertTrue(mockService.doSomething().equals(
                MockServiceImpl.class.getName()));
    }
    
    @Test
    public void testGetServiceProxy()
        throws Exception
    {
        SystemConfig.instance().bind(MockService.class.getName(), null);
        
        SystemConfig.instance().bind(
                MockService.class.getName() + ServiceFactory.PROXY_TYPE, null);
        
        ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(null,
                MockService.class, null);
        
        assertNotNull(serviceProxy);
        
        assertTrue(ServiceProxyLocal.class.isInstance(serviceProxy));
    }
    
}
