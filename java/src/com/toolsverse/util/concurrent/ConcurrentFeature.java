/*
 * ConcurrentFeature.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.util.Utils;

/**
 * This class is used to configure ConcurrentAdapter behavior such as: show progress indicator or not, enabled cancel functionality or nor, etc.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConcurrentFeature
{
    
    /** Show progress. */
    private boolean _showProgress;
    
    /** Enable cancel button */
    private boolean _enableCancel;
    
    /** The message do display */
    private String _message;
    
    /** The ProgressDelegate. */
    private ProgressDelegate _progress;
    
    /**
     * Instantiates a new default ConcurrentFeature.
     */
    public ConcurrentFeature()
    {
        this((ProgressDelegate)null);
    }
    
    /**
     * Instantiates a new ConcurrentFeature.
     *
     * @param showProgress if true the progress indicator will be displayed
     */
    public ConcurrentFeature(boolean showProgress)
    {
        this(showProgress, null, null);
    }
    
    /**
     * Instantiates a new ConcurrentFeature.
     *
     * @param showProgress if true the progress indicator will be displayed
     * @param message the message to display
     * @param progress the ProgressDelegate. If null the default for the current execution mode ProgressDelegate will be used
     */
    public ConcurrentFeature(boolean showProgress, String message,
            ProgressDelegate progress)
    {
        _message = !Utils.isNothing(message) ? message
                : ConcurrentResource.PLEASE_WAIT_MESSAGE.getValue();
        _progress = progress;
        _enableCancel = true;
        _showProgress = showProgress || SystemConfig.instance().isClient();
    }
    
    /**
     * Instantiates a new ProgressDelegate
     *
     * @param progress the ProgressDelegate. If parameter is null the default for the current execution mode ProgressDelegate will be used.
     */
    public ConcurrentFeature(ProgressDelegate progress)
    {
        this(true, null, progress);
    }
    
    /**
     * Gets the message which is displayed on the progress indicator.
     *
     * @return the message
     */
    public String getMessage()
    {
        return _message;
    }
    
    /**
     * Gets the current ProgressDelegate
     *
     * @return the progress delegate
     */
    public ProgressDelegate getProgressDelegate()
    {
        return _progress;
    }
    
    /**
     * Checks if cancel functionality is enabled. If it is enabled the ProgressDeligate displays a Cancel button. 
     * By clicking on Cancel button user requests interruption of the current service thread.   
     *
     * @return true, if cancel functionality is enabled
     */
    public boolean isEnableCancel()
    {
        return _enableCancel;
    }
    
    /**
     * Checks if progress indicator is configured to be displayed. Progress indicator is usually a modal dialog with a progress bar.
     *
     * @return true, if progress indicator is configured to be displayed. 
     */
    public boolean isShowProgress()
    {
        return _showProgress;
    }
    
    /**
     * Enables or disables cancel functionality. 
     *
     * @param value the new value
     */
    public void setEnableCancel(boolean value)
    {
        _enableCancel = value;
    }
    
    /**
     * Sets the message which will be displayed on progress indicator.
     *
     * @param value the new message
     */
    public void setMessage(String value)
    {
        _message = value;
    }
    
    /**
     * Sets the new ProgressDelegate. If parameter is null the default for the current execution mode ProgressDelegate will be used.
     *
     * @param value the new ProgressDelegate
     */
    public void setProgressDelegate(ProgressDelegate value)
    {
        _progress = value;
    }
    
    /**
     * Sets the new show progress value. If value is true the progress indicator will be displayed.
     *
     * @param value the new show progress value
     */
    public void setShowProgress(boolean value)
    {
        _showProgress = value;
    }
}
