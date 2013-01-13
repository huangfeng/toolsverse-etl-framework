/*
 * UpdaterProxy.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater.service;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.Resource;
import com.toolsverse.service.web.ServiceProxyWeb;
import com.toolsverse.util.Utils;

/**
 * And extension of the ServiceProxyWeb used to communicate with UpdaterService.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class UpdaterProxy extends ServiceProxyWeb
{
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.service.web.ServiceProxyWeb#getUrl()
     */
    @Override
    public String getUrl()
        throws Exception
    {
        String url = SystemConfig.instance().getSystemProperty(
                SystemConfig.UPDATE_URL);
        
        if (Utils.isNothing(url))
            throw new Exception(
                    Resource.ERROR_LOCATING_WEB_SERVICE_URL.getValue());
        
        return url;
    }
}