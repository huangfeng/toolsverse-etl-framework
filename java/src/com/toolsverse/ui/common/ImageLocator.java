/*
 * ImageLocator.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

/**
 * This interface must be implemented by the classes which load images from the resource. 
 *
 * @param <V> the image type. For example Icon.
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ImageLocator<V>
{
    
    /**
     * Gets the image from the resource using given path.
     *
     * @param path the path
     * @return the image
     */
    V get(String path);
    
    /**
     * Gets the image from the resource using given path. If image not found uses defaultPath instead. 
     *
     * @param path the path
     * @param defaultPath the default path
     * @return the image
     */
    V get(String path, String defaultPath);
    
    /**
     * Checks if image is empty.
     *
     * @param image the image
     * @return true, if image is empty
     */
    boolean isEmpty(Object image);
}