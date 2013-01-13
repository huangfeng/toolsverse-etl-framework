/*
 * IoProcessorFactory.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * Instantiates the particular implementation of the IoProcessor by name. Possible names: file, ftp, sftp. IoProcessorFactory is a singleton. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class IoProcessorFactory
{
    
    /** FILE. */
    public static final String FILE = "file";
    
    /** FTP. */
    public static final String FTP = "ftp";
    
    /** SFTP. */
    public static final String SFTP = "sftp";
    
    /** instance of the IoProcessorFactory. */
    private volatile static IoProcessorFactory _instance;
    
    /**
     * Returns an instance of the IoProcessorFactory
     *
     * @return the IoProcessorFactory
     */
    public static IoProcessorFactory instance()
    {
        if (_instance == null)
        {
            synchronized (ObjectFactory.class)
            {
                if (_instance == null)
                    _instance = new IoProcessorFactory();
            }
        }
        
        return _instance;
    }
    
    /**
     * Instantiates a new IoProcessorFactory.
     */
    private IoProcessorFactory()
    {
    }
    
    /**
     * Returns FileProcessor.
     *
     * @see com.toolsverse.io.FileProcessor
     * @return the IoProcessor
     */
    public IoProcessor getIoProcessor()
    {
        return getIoProcessor(null, null, -1);
    }
    
    /**
     * Gets the IoProcessor by name.
     *
     * @param resource the name of the IoProcessor. Possible values: file, ftp, sftp
     * @return the IoProcessor
     */
    public IoProcessor getIoProcessor(String resource)
    {
        return getIoProcessor(resource, null, -1);
    }
    
    /**
     * Gets the IoProcessor by name. If proxyUrl != null and proxyPort != - 1 returns a proxy version (if exists) for the particular IoProcessor.
     *
     * @param resource the name of the IoProcessor. Possible values: file, ftp, sftp
     * @param proxyUrl the proxy url
     * @param proxyPort the proxy port
     * @return the resource processor
     */
    public IoProcessor getIoProcessor(String resource, String proxyUrl,
            int proxyPort)
    {
        if (Utils.isNothing(resource))
            return new FileProcessor();
        
        if (resource.equalsIgnoreCase(FILE))
            return new FileProcessor();
        else if (resource.equalsIgnoreCase(FTP))
            return new FtpProcessor(proxyUrl, proxyPort);
        else if (resource.equalsIgnoreCase(SFTP))
            return new SftpProcessor();
        else
            return null;
    }
    
}
