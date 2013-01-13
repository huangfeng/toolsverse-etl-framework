/*
 * AppInfoCollectorDefault.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.collector;

import java.net.InetAddress;
import java.net.UnknownHostException;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;

/**
 * The default implementation of the AppInfoCollector interface. Used in the client mode. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class AppInfoCollectorDefault implements AppInfoCollector
{
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.collector.AppInfoCollector#getAppInfo()
     */
    public AppInfo getAppInfo()
    {
        AppInfo appInfo = new AppInfo();
        
        try
        {
            String home = SystemConfig.instance().getHome();
            
            appInfo.setAppHome(FileUtils.getUnixFolderName(home));
            appInfo.setAppRoot(FilenameUtils.separatorsToUnix(FilenameUtils
                    .normalizeNoEndSeparator(appInfo.getAppHome())));
            
            InetAddress addr = InetAddress.getLocalHost();
            
            if (addr == null)
                return appInfo;
            
            appInfo.setClientHost(addr.getHostName());
            appInfo.setClientAddr(addr.getHostAddress());
            
            return appInfo;
            
        }
        catch (UnknownHostException e)
        {
            return appInfo;
        }
    }
}
