/*
 * BaseOptionsHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.awt.event.ActionListener;

/**
 * The abstract implementation of the OptionsHandler.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseOptionsHandler implements OptionsHandler
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.ui.common.OptionsHandler#handleOptions(java.lang.Object,
     * java.lang.Object, int, java.awt.event.ActionListener)
     */
    public Object handleOptions(Object parent, Object message, int messageType,
            ActionListener feedback)
    {
        return handleOptions(parent, message, messageType, null, feedback);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.ui.common.OptionsHandler#handleOptions(java.lang.Object,
     * java.lang.Object, int, java.lang.Object, java.awt.event.ActionListener)
     */
    public abstract Object handleOptions(Object parent, Object message,
            int messageType, Object input, ActionListener feedback);
}
