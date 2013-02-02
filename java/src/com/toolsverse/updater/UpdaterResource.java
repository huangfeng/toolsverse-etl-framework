/*
 * UpdaterResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater;

import com.toolsverse.config.SystemConfig;

/**
 * Messages, errors, etc used by {@link Updater}. 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum UpdaterResource
{
    // messages
    NEW_VERSION_IS_READY("New version of the software is available. You can download it using the following url:"),
    
    NEW_VERSION_IS_DOWNLOADED("New version of the software is downloaded. The update file name: %1"),
    
    NEW_VERSION_IS_DOWNLOADED_RESTART("New version of the software is downloaded. Please restart application to install update."),
    
    // errors
    CANNOT_CREATE_FOLDER("Cannot create folder %1"),
    CANNOT_FIND_FILE("Cannot find file %1"),
    
    ERROR_DOWNLOADING_UPDATE("Error downloading update"),
    ERROR_CHECKING_FOR_UPDATES("Error checking for updates");
    
    /**
     * The value
     */
    private String _value;
    
    /**
     * Instantiates new UpdaterResource using given value.
     * 
     * @param value
     */
    UpdaterResource(String value)
    {
        _value = value;
    }
    
    /**
     * Gets the value of this enum constant.
     * 
     * @return the value
     */
    public String getValue()
    {
        return SystemConfig.instance().getObjectProperty(this);
    }
    
    @Override
    public String toString()
    {
        return _value;
    }
}
