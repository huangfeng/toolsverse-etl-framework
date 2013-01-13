/*
 * HttpClient.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service.web;

import org.apache.http.HttpHost;
import org.apache.http.conn.params.ConnRoutePNames;
import org.apache.http.impl.client.DefaultHttpClient;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * Singleton wrapper for the apache DefaultHttpClient. Additionally configures proxy if any exist.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class HttpClient extends DefaultHttpClient
{
    
    /** The instance. */
    private volatile static HttpClient _instance;
    
    /**
     * Returns an instance of the HttpClient.
     *
     * @return the HttpClient
     */
    public static HttpClient instance()
    {
        if (_instance == null)
        {
            synchronized (HttpClient.class)
            {
                if (_instance == null)
                    _instance = new HttpClient();
            }
        }
        
        return _instance;
    }
    
    /**
     * Instantiates a new HttpClient. Configures proxy if any.
     */
    private HttpClient()
    {
        String proxyHost = SystemConfig.instance().getSystemProperty(
                SystemConfig.SERVER_PROXY_HOST);
        
        int proxyPort = Utils.str2Int(SystemConfig.instance()
                .getSystemProperty(SystemConfig.SERVER_PROXY_PORT, "8080"),
                8080);
        
        if (!Utils.isNothing(proxyHost))
        {
            HttpHost proxy = new HttpHost(proxyHost, proxyPort);
            getParams().setParameter(ConnRoutePNames.DEFAULT_PROXY, proxy);
        }
    }
    
}
