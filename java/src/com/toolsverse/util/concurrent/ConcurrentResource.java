/*
 * ConcurrentResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import com.toolsverse.config.SystemConfig;

/**
 * The default messages used by ConcurrentExecutor.
 * 
 * @see com.toolsverse.util.concurrent.ConcurrentExecutor
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public enum ConcurrentResource
{
    // errors
    ERROR_CREATING_PROGRESS("Error creating progress indicator."),
    
    ERROR_DURING_EXECUTION("There was an error. Please see log for details."),
    
    ERROR_AQUIRING_LOCK("Error aquiring file lock."),
    
    ERROR_RELEASING_LOCK("Error releasing file lock."),
    
    ERROR_CLOSING_CHANNEL("Error closing channel."),
    
    CANNOT_AQUIRE_LOCK("Cannot aquire file lock."),
    
    // messages
    FINISHED_MESSAGE("You request is finished. You can now close dialog."),
    
    PLEASE_WAIT_MESSAGE(
            "You request is being processed. Please wait until it is finished.");
    
    /** The _value. */
    private String _value;
    
    /**
     * Instantiates a new concurrent resource.
     * 
     * @param value
     *            the value
     */
    ConcurrentResource(String value)
    {
        _value = value;
    }
    
    /**
     * Gets the value.
     * 
     * @return the value
     */
    public String getValue()
    {
        return SystemConfig.instance().getObjectProperty(this);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Enum#toString()
     */
    @Override
    public String toString()
    {
        return _value;
    }
}
