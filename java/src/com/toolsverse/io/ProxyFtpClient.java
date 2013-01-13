/*
 * ProxyFtpClient.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import org.apache.commons.net.ftp.FTPClient;

/**
 * The proxy ftp client.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class ProxyFtpClient extends FTPClient
{
    
    /**
     * Instantiates a new proxy ftp client.
     *
     * @param host the host
     * @param port the port
     */
    public ProxyFtpClient(String host, int port)
    {
        super();
        
        if (port > 0)
            setSocketFactory(new ProxySocketFactory(host, port));
    }
    
}
