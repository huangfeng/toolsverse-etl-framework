/*
 * DispatchServlet.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service.web;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.InvocationTargetException;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import javax.servlet.ServletException;
import javax.servlet.ServletInputStream;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import com.toolsverse.exception.DefaultExceptionHandler;
import com.toolsverse.resource.Resource;
import com.toolsverse.security.SecurityContext;
import com.toolsverse.service.Service;
import com.toolsverse.service.ServiceFactory;
import com.toolsverse.service.ServiceProxy;
import com.toolsverse.service.ServiceProxyLocal;
import com.toolsverse.service.ServiceRequest;
import com.toolsverse.service.ServiceResponse;
import com.toolsverse.util.log.Logger;

/**
 * In the client-server mode the client executes service method using ServiceProxyWeb. The ServiceProxyWeb calls DispatchServlet using HTTP protocol 
 * which in return executes service method using ServiceProxyLocal and sends response back to the ServiceProxyWeb. At the end ServiceProxyWeb sends result back
 * to the client. The DispatchServlet is an abstract class and it is expected that particular implementation will add security an pre-caching on init. 
 * 
 * @see com.toolsverse.service.web.ServiceProxyWeb
 * @see com.toolsverse.service.ServiceProxyLocal
 * @see com.toolsverse.service.ServiceRequest
 * @see com.toolsverse.service.ServiceResponse
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class DispatchServlet extends HttpServlet
{
    
    /**
     * Calls service method using ServiceProxyLocal.
     *
     * @param serviceRequest the service request
     * @return the service response
     */
    public ServiceResponse callService(ServiceRequest serviceRequest)
    {
        ServiceResponse serviceResponse = null;
        
        try
        {
            ServiceProxy serviceProxy = ServiceFactory.getServiceProxy(
                    ServiceProxyLocal.class.getName(),
                    getServiceClass(serviceRequest.getServiceClass()), null);
            
            Object result = serviceProxy.invoke(serviceProxy,
                    serviceRequest.getMethod(), serviceRequest.getArgs());
            
            serviceResponse = new ServiceResponse(result, null);
            
        }
        catch (Throwable th)
        {
            if (th instanceof InvocationTargetException
                    && ((InvocationTargetException)th).getTargetException() != null)
                th = ((InvocationTargetException)th).getTargetException();
            
            serviceResponse = getErrorServiceResponse(th);
        }
        
        return serviceResponse;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest
     * , javax.servlet.http.HttpServletResponse)
     */
    @Override
    public void doGet(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        response.setContentType("text/html");
        response.setStatus(HttpServletResponse.SC_OK);
        response.getWriter().print(
                "<html><head></head><body><p>"
                        + Resource.WEB_SERVICES_READY_TO_USE.getValue()
                        + "</p></body></html>");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * javax.servlet.http.HttpServlet#doPost(javax.servlet.http.HttpServletRequest
     * , javax.servlet.http.HttpServletResponse)
     */
    @Override
    public void doPost(HttpServletRequest request, HttpServletResponse response)
        throws ServletException, IOException
    {
        ServiceResponse serviceResponse = null;
        
        try
        {
            WebSessionManager.setSession(request.getSession(true));
            
            ServiceRequest serviceRequest = getServiceRequest(request);
            
            SecurityContext.setCurrentSecurityContext(serviceRequest
                    .getSecurityContext());
            
            serviceResponse = callService(serviceRequest);
        }
        catch (Exception ex)
        {
            serviceResponse = getErrorServiceResponse(ex);
        }
        
        writeServiceResponse(response, serviceResponse);
    }
    
    /**
     * Gets the service response in case of exception during service invocation.
     *
     * @param throwable the throwable
     * @return the error service response
     */
    private ServiceResponse getErrorServiceResponse(Throwable throwable)
    {
        return new ServiceResponse(null, throwable);
    }
    
    /**
     * Gets the service class.
     *
     * @return the service class
     */
    public Class<? extends Service> getServiceClass(
            Class<? extends Service> service)
    {
        return service;
    }
    
    /**
     * Gets the service request from the given HttpServletRequest. It is expected that request if packed using GZIP compression algorithm.
     *
     * @param request the request
     * @return the service request
     * @throws Exception in case of any error
     */
    public ServiceRequest getServiceRequest(HttpServletRequest request)
        throws Exception
    {
        ServletInputStream servletStream = null;
        ObjectInputStream objectStream = null;
        GZIPInputStream zipStream = null;
        
        try
        {
            servletStream = request.getInputStream();
            
            zipStream = new GZIPInputStream(servletStream);
            
            objectStream = new ObjectInputStream(zipStream);
            
            return (ServiceRequest)objectStream.readObject();
        }
        finally
        {
            if (objectStream != null)
                try
                {
                    objectStream.close();
                }
                catch (Exception ex)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), ex);
                }
            
            if (zipStream != null)
                try
                {
                    zipStream.close();
                }
                catch (Exception ex)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), ex);
                }
            
            if (servletStream != null)
                try
                {
                    servletStream.close();
                }
                catch (Exception ex)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), ex);
                }
        }
    }
    
    /**
     * Writes service response. Pack it using GZIP compression algorithm.
     *
     * @param response the response
     * @param serviceResponse the service response
     * @throws ServletException the servlet exception
     * @throws IOException Signals that an I/O exception has occurred.
     */
    public void writeServiceResponse(HttpServletResponse response,
            ServiceResponse serviceResponse)
        throws ServletException, IOException
    {
        ServletOutputStream outputStream = null;
        ObjectOutputStream objectStream = null;
        GZIPOutputStream zipStream = null;
        
        response.setContentType("application/octet-stream");
        
        try
        {
            outputStream = response.getOutputStream();
            
            zipStream = new GZIPOutputStream(outputStream);
            
            objectStream = new ObjectOutputStream(zipStream);
            
            objectStream.writeObject(serviceResponse);
            
            objectStream.flush();
        }
        finally
        {
            if (objectStream != null)
                try
                {
                    objectStream.close();
                }
                catch (Exception e)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), e);
                }
            
            if (zipStream != null)
                try
                {
                    objectStream.close();
                }
                catch (Exception e)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), e);
                }
            
            if (outputStream != null)
                try
                {
                    outputStream.close();
                }
                catch (Exception e)
                {
                    DefaultExceptionHandler.instance().logException(
                            Logger.SEVERE, getClass(),
                            Resource.ERROR_CLOSING_RESOURCE.getValue(), e);
                }
        }
    }
}
