/*
 * UpdaterRequest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater.service;

import java.io.Serializable;

import com.toolsverse.config.SystemConfig;

/**
 * The instance of this class is used to request a product's update or information about new product's version.
 * The request includes information about all loaded extension modules. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class UpdaterRequest implements Serializable
{
    
    /** The UPDATER KEY property. */
    public static final String UPDATER_KEY_PROP = "app.update.key";
    
    /** The key for updater. Can be app name */
    private String _key;
    
    /** The app version. */
    private String _appVersion;
    
    /**
     * Instantiates a new updater request.
     */
    public UpdaterRequest()
    {
        _appVersion = SystemConfig.instance().getVersion();
        
        _key = SystemConfig.instance().getSystemProperty(UPDATER_KEY_PROP,
                SystemConfig.instance().getAppName());
    }
    
    /**
     * Gets the app version.
     *
     * @return the app version
     */
    public String getAppVersion()
    {
        return _appVersion;
    }
    
    /**
     * Gets the app name.
     *
     * @return the app name
     */
    public String getKey()
    {
        return _key;
    }
    
}