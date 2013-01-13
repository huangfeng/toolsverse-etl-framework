/*
 * ProgressDelegate.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

/**
 * When there is a need to show execution progress use particular implementation of the ProgressDelegate. Depending on execution environment (client or web) the right
 * implementation of the ProgressDelegate is automatically instantiated.  It is however possible to provide your own ProgressDelegate by the means 
 * of the <code>ConcurrentFeature</code>.
 * 
 * <p>
 * Typically ProgressDelegate is a modal dialog box with a progress bar and a "Cancel" button. 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ProgressDelegate
{
    
    /**
     * Displays the ProgressDelegate.
     *
     * @param executor the ConcurrentExecuto
     * @param concurrentFeature the ConcurrentFeature
     * @param adapter the ConcurrentAdapter
     */
    void display(ConcurrentExecutor executor,
            ConcurrentFeature concurrentFeature, ConcurrentAdapter adapter);
    
    /**
     * Executed when service thread is finished.
     */
    void done();
    
    /**
     * Executed when service thread is finished with exception.
     *
     * @param ex the Throwable
     */
    void onException(Throwable ex);
    
    /**
     * Executed when service thread is started
     */
    void start();
}
