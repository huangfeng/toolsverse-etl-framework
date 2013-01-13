/*
 * UpdaterServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater.service;

/**
 * The default implementation of the {@link UpdaterService} interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class UpdaterServiceImpl implements UpdaterService
{
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.ide.updater.service.UpdaterService#checkForUpdates(com
     * .toolsverse.ide.updater.service.UpdaterRequest)
     */
    public UpdaterResponse checkForUpdates(UpdaterRequest request)
        throws Exception
    {
        throw new IllegalStateException("Should be never called dirrecly");
        
    }
}