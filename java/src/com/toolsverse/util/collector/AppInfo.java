/*
 * AppInfo.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.collector;

import java.io.Serializable;

import com.toolsverse.util.Utils;
import com.toolsverse.util.encryption.Base64;

/**
 * This class contains basic information about execution environment, such as host name, ip address, "home" folder, etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class AppInfo implements Serializable
{
    
    /** default LOCAL IP4. */
    public static final String LOCAL_IP4 = "127.0.0.1";
    
    /** default LOCAL IP6. */
    public static final String LOCAL_IP6 = "0:0:0:0:0:0:0:1";
    
    /** default LOCAL HOST. */
    public static final String LOCAL_HOST = "localhost";
    
    /** The Constant MAGIC_PREFIX. */
    private static final String MAGIC_PREFIX = "rO0ABXQA";
    
    /**
     * Gets the encoded name.
     *
     * @param name the name
     * @return the encoded name
     */
    public static final String getEncodedName(String name)
    {
        return MAGIC_PREFIX
                + Base64.encodeObject(name).replaceAll("\\=", "")
                        .replaceAll("\\.", "");
    }
    
    /**
     * Checks if given name is encoded.
     *
     * @param value the value
     * @return true, if given name is encoded
     */
    public static final boolean isEncodedName(String value)
    {
        return value != null && value.indexOf(MAGIC_PREFIX) == 0;
    }
    
    /** The client's address. */
    private String _clientAddr;
    
    /** The client's host. */
    private String _clientHost;
    
    /** The app root folder. */
    private String _appRoot;
    
    /** The app home folder. */
    private String _appHome;
    
    /** The unique id. */
    private String _uniqueId;
    
    /**
     * Instantiates a new AppInfo.
     */
    public AppInfo()
    {
        _clientAddr = "";
        _clientAddr = "";
        _appRoot = "";
        _appHome = "";
        _uniqueId = null;
    }
    
    /**
     * Gets the app home folder.
     *
     * @return the app home folder
     */
    public String getAppHome()
    {
        return _appHome;
    }
    
    /**
     * Gets the app root folder.
     *
     * @return the app root folder
     */
    public String getAppRoot()
    {
        return _appRoot;
    }
    
    /**
     * Gets the client address. When app works in the client-server mode this is an ip address of the client's machine.
     * In client or web modes it is a local machine ip address.
     *
     * @return the client address
     */
    public String getClientAddr()
    {
        return _clientAddr;
    }
    
    /**
     * Gets the client host name. When app works in the client-server mode this is an host name of the client's machine.
     * In client or web modes it is a local machine host name.
     *
     * @return the client host name
     */
    public String getClientHost()
    {
        return _clientHost;
    }
    
    /**
     * Gets the unique id. In the client-server or web modes this is a user id.
     *
     * @return the unique id
     */
    public String getUniqueId()
    {
        return _uniqueId;
    }
    
    /**
     * Gets the unique name. If unique id is set returns base64 encoded unique id, otherwise base64 encoded client's host name.
     * The unique name is used to create user's specific home folders. For example if user 'abc' logged in into the app it's home folder
     * will be <code>getAppHome() + "/" + getUniqueName()</code>.    
     *
     * @return the unique name
     */
    public String getUniqueName()
    {
        String name;
        
        if (!Utils.isNothing(_uniqueId))
        {
            return getEncodedName(_uniqueId);
        }
        
        if (LOCAL_IP4.equals(_clientHost) || LOCAL_IP6.equals(_clientHost)
                || Utils.isNothing(_clientHost))
            name = LOCAL_HOST;
        else
        {
            name = _clientHost.replaceAll("\\.", "");
        }
        
        return getEncodedName(name);
    }
    
    /**
     * Sets the app home folder.
     *
     * @param value the new app home folder
     */
    public void setAppHome(String value)
    {
        _appHome = value;
    }
    
    /**
     * Sets the app root folder.
     *
     * @param value the new app root
     */
    public void setAppRoot(String value)
    {
        _appRoot = value;
    }
    
    /**
     * Sets the client address.
     *
     * @param value the new client address
     */
    public void setClientAddr(String value)
    {
        _clientAddr = value;
    }
    
    /**
     * Sets the client host name.
     *
     * @param value the new client host name
     */
    public void setClientHost(String value)
    {
        _clientHost = value;
    }
    
    /**
     * Sets the unique id. In the client-server or web modes this is a user id.
     *
     * @param value the new unique id
     */
    public void setUniqueId(String value)
    {
        _uniqueId = value;
    }
}
