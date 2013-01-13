/*
 * UpdaterService.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater.service;

import com.toolsverse.service.Service;

/**
 * The class which checks for product's updates must implement this interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface UpdaterService extends Service
{
    /**
     * Check for product updates.
     *
     * @param request the request
     * @return the updater response
     * @throws Exception in case of any error
     */
    UpdaterResponse checkForUpdates(UpdaterRequest request)
        throws Exception;
}