/*
 * AppInfoCollector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.collector;

/**
 * The class which collects information about application's execution environment must implement this interface. 
 * The particular implementations of this interface are different for client, client-server and web modes.
 *  
 * @see com.toolsverse.util.collector.AppInfoCollectorDefault
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface AppInfoCollector
{
    /**
     * Collects information about application's execution environment, such as host name, ip address, "home" folder, etc.
     *
     * @return the AppInfo object
     */
    AppInfo getAppInfo();
}
