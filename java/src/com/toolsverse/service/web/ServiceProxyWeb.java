/*
 * ServiceProxyWeb.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service.web;

import java.io.IOException;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.OutputStream;
import java.lang.reflect.Method;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

import org.apache.http.HttpEntity;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.ContentProducer;
import org.apache.http.entity.EntityTemplate;
import org.apache.http.util.EntityUtils;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.Resource;
import com.toolsverse.security.SecurityContext;
import com.toolsverse.service.ServiceProxy;
import com.toolsverse.service.ServiceRequest;
import com.toolsverse.service.ServiceResponse;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * HTTP implementation of the abstract ServiceProxy class. Uses apache HttpClient to send requests over HTTP. The requests and responses are packed using
 * GZIP compression method.
 * 
 * In the client-server mode the client executes service method using ServiceProxyWeb. The ServiceProxyWeb calls DispatchServlet using HTTP protocol 
 * which in return executes service method using ServiceProxyLocal and sends response back to the ServiceProxyWeb. At the end ServiceProxyWeb sends result back
 * to the client.
 * 
 * @see com.toolsverse.service.web.DispatchServlet
 * @see com.toolsverse.service.ServiceProxyLocal
 * @see com.toolsverse.service.ServiceRequest
 * @see com.toolsverse.service.ServiceResponse
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ServiceProxyWeb extends ServiceProxy
{
    
    /**
     * The Class GZipContentProducer. Used to pack requests using GZIP compression method.
     */
    private class GZipContentProducer implements ContentProducer
    {
        
        /** The request. */
        ServiceRequest _request;
        
        /**
         * Instantiates a new GZipContentProducer.
         *
         * @param request the request
         */
        GZipContentProducer(ServiceRequest request)
        {
            _request = request;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.http.entity.ContentProducer#writeTo(java.io.OutputStream)
         */
        public void writeTo(OutputStream out)
            throws IOException
        {
            GZIPOutputStream zipStream = null;
            ObjectOutputStream sendStream = null;
            
            try
            {
                zipStream = new GZIPOutputStream(out);
                
                sendStream = new ObjectOutputStream(zipStream);
                
                // send request
                sendStream.writeObject(_request);
                sendStream.flush();
            }
            finally
            {
                if (sendStream != null)
                    try
                    {
                        sendStream.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, this,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                if (zipStream != null)
                    try
                    {
                        zipStream.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, this,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
            }
            
        }
    }
    
    /**
     * Gets the response from the input stream.
     *
     * @param is the InputStream
     * @return the response
     * @throws Throwable in case of any error
     */
    public Object getResponse(InputStream is)
        throws Throwable
    {
        ObjectInputStream receiveStream = null;
        GZIPInputStream zipStream = null;
        try
        {
            zipStream = new GZIPInputStream(is);
            
            receiveStream = new ObjectInputStream(zipStream);
            
            Object response = receiveStream.readObject();
            
            if (response != null && response instanceof ServiceResponse)
            {
                if (((ServiceResponse)response).getThrowable() != null)
                    throw ((ServiceResponse)response).getThrowable();
                
                return ((ServiceResponse)response).getResult();
            }
            else
                return response;
        }
        finally
        {
            if (receiveStream != null)
                try
                {
                    receiveStream.close();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), ex);
                    
                }
            if (zipStream != null)
                try
                {
                    zipStream.close();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.SEVERE, this,
                            Resource.ERROR_GENERAL.getValue(), ex);
                    
                }
        }
    }
    
    /**
     * Gets the server url. Throws exception it is not configured. 
     *
     * @return the url
     * @throws Exception the exception if server url is not configured. 
     */
    public String getUrl()
        throws Exception
    {
        String url = SystemConfig.instance().getSystemProperty(
                SystemConfig.SERVER_URL);
        
        if (Utils.isNothing(url))
            throw new Exception(
                    Resource.ERROR_LOCATING_WEB_SERVICE_URL.getValue());
        
        return url;
        
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
        HttpPost post = new HttpPost(getUrl());
        
        HttpEntity entity = new EntityTemplate(new GZipContentProducer(
                new ServiceRequest(m, args, getServiceClass(),
                        SecurityContext.getCurrentSecurityContext())));
        
        post.setEntity(entity);
        
        HttpResponse response = null;
        entity = null;
        
        try
        {
            response = HttpClient.instance().execute(post);
            
            entity = response.getEntity();
            
            Object ret = getResponse(entity.getContent());
            
            return ret;
        }
        catch (Throwable ex)
        {
            post.abort();
            
            throw ex;
        }
        finally
        {
            EntityUtils.consume(entity);
        }
    };
    
}
