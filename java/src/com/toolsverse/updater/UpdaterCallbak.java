/*
 * UpdaterCallbak.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater;

import com.toolsverse.updater.service.UpdaterResponse;

/**
 * When {@link Updater} finishes checking for updates or downloading update it calls doneChecking or doneDownloading respectfully.       
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface UpdaterCallbak
{
    
    /**
     * Called when updater finished checking for updates.
     *
     * @param response the response
     */
    void doneChecking(UpdaterResponse response);
    
    /**
     * Called when updater finished downloading update.
     *
     * @param response the response
     */
    void doneDownloading(UpdaterResponse response);
}
