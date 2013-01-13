/*
 * ProxySocketFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.IOException;
import java.net.Socket;
import java.net.UnknownHostException;

import org.apache.commons.net.DefaultSocketFactory;

import socks.Proxy;
import socks.SocksSocket;

import com.toolsverse.util.Utils;

/**
 * The instance of this class creates a socket connection using socket proxy. 
 *
 * @see org.apache.commons.net.DefaultSocketFactory
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class ProxySocketFactory extends DefaultSocketFactory
{
    
    /** host. */
    private String _host;
    
    /** port. */
    private int _port;
    
    /**
     * Instantiates a new ProxySocketFactory.
     *
     * @param host the host
     * @param port the port
     */
    public ProxySocketFactory(String host, int port)
    {
        super();
        _host = host;
        _port = port > 0 ? port : 1080;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * org.apache.commons.net.DefaultSocketFactory#createSocket(java.lang.String
     * , int)
     */
    @Override
    public Socket createSocket(String host, int port)
        throws UnknownHostException, IOException
    {
        if (Utils.isNothing(_host))
            return super.createSocket(host, port);
        
        Proxy.setDefaultProxy(_host, _port);
        Socket socket = new SocksSocket(host, port);
        
        return socket;
    }
}
