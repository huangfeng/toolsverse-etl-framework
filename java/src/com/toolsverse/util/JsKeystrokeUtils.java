/*
 * JsKeystrokeUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.awt.event.InputEvent;

import javax.swing.KeyStroke;

/**
 * The collection of static methods for mapping between JavaScipt keyboard events and Java KeyStroke.  
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public final class JsKeystrokeUtils
{
    /**
     * Converts Java KeyStroke to the form which JavaScript can understand
     * 
     * @param key the KeyStroke to convert 
     * @param event the event
     * 
     * @return the string which can be used in the JavaScript to map keyboard events   
     */
    public static String keyStroke2String(KeyStroke key, String event)
    {
        int m = key.getModifiers();
        
        String res = "";
        
        if ((m & (InputEvent.CTRL_DOWN_MASK | InputEvent.CTRL_MASK)) != 0)
        {
            if (Utils.isNothing(res))
                res = event + ".ctrlKey";
            else
                res = res + " && " + event + ".ctrlKey";
        }
        if ((m & (InputEvent.META_DOWN_MASK | InputEvent.META_MASK)) != 0)
        {
            if (Utils.isNothing(res))
                res = event + ".metaKey";
            else
                res = res + " && " + event + ".metaKey";
        }
        if ((m & (InputEvent.ALT_DOWN_MASK | InputEvent.ALT_MASK)) != 0)
        {
            if (Utils.isNothing(res))
                res = event + ".altKey";
            else
                res = res + " && " + event + ".altKey";
        }
        if ((m & (InputEvent.SHIFT_DOWN_MASK | InputEvent.SHIFT_MASK)) != 0)
        {
            if (Utils.isNothing(res))
                res = event + ".shiftKey";
            else
                res = res + " && " + event + ".shiftKey";
        }
        
        if (Utils.isNothing(res))
            res = event + ".keyCode==" + key.getKeyCode();
        else
            res = res + " && " + event + ".keyCode==" + key.getKeyCode();
        
        return res;
        
    }
}
