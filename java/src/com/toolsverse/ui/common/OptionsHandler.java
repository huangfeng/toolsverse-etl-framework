/*
 * OptionsHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.awt.event.ActionListener;

/**
 * This interface must be implemented by the classes which display informational, error, and other message dialogs.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface OptionsHandler
{
    
    /** The INPUT_MESSAGE. */
    static final int INPUT_MESSAGE = 10;
    
    /** Action Performed Value if Yes is Choosen. */
    static final String YES_ACTION = "YES";
    
    /** Action Performed Value if No is choosen. */
    static final String NO_ACTION = "NO";
    
    /** Action Performed Value if Ok is choosen. */
    static final String OK_ACTION = "OK";
    
    /** Action Performed Value if Cancel is choosen. */
    static final String CANCEL_ACTION = "CANCEL";
    
    /** Action Performed Value Unknow. */
    static final String UNKNOWN_ACTION = "UNKNOWN";
    
    /**
     * Handle options.
     *
     * @param parent the parent object
     * @param message the message
     * @param messageType the message type
     * @param feedback the feedback
     * @return the object
     */
    Object handleOptions(Object parent, Object message, int messageType,
            ActionListener feedback);
    
    /**
     * Handle options.
     *
     * @param parent the parent object
     * @param message the message
     * @param messageType the message type
     * @param input the input field
     * @param feedback the feedback
     * @return the object
     */
    Object handleOptions(Object parent, Object message, int messageType,
            Object input, ActionListener feedback);
}
