/*
 * KeyMapping.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.util.HashMap;

import javax.swing.Action;
import javax.swing.ActionMap;
import javax.swing.ComponentInputMap;
import javax.swing.InputMap;
import javax.swing.JComponent;
import javax.swing.KeyStroke;

import com.toolsverse.util.JsKeystrokeUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;

/**
 * The wrapper class for the KeyStroke and Action. Creates an ActionMap and InputMap. Used to define keyboard shortcuts.   
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class KeyMapping extends
        HashMap<String, TypedKeyValue<KeyStroke, Action>>
{
    
    /**
     * Instantiates a new KeyMapping.
     */
    public KeyMapping()
    {
        super();
    }
    
    /**
     * Gets the action map.
     *
     * @return the action map
     */
    public ActionMap getActionMap()
    {
        if (size() == 0)
            return null;
        
        ActionMap actionMap = new ActionMap();
        
        for (String command : keySet())
        {
            Action action = get(command).getValue();
            
            actionMap.put(command, action);
        }
        
        return actionMap;
    }
    
    /**
     * Gets the input map.
     *
     * @param owner the owner
     * @return the input map
     */
    public InputMap getInputMap(JComponent owner)
    {
        if (size() == 0)
            return null;
        
        InputMap inputMap = owner != null ? new ComponentInputMap(owner)
                : new InputMap();
        
        for (String command : keySet())
        {
            KeyStroke key = get(command).getKey();
            
            inputMap.put(key, command);
        }
        
        return inputMap;
    }
    
    /**
     * Gets the Wings keyboard event.
     *
     * @param id the id
     * @param events the events
     * @return the wings events
     */
    public String getWingsEvents(String id, String events)
    {
        String event = "";
        
        if (size() == 0)
            return event;
        
        for (String command : keySet())
        {
            KeyStroke key = get(command).getKey();
            
            event = event + "if ("
                    + JsKeystrokeUtils.keyStroke2String(key, "event") + ") "
                    + "{" + Utils.makeString(events)
                    + "wingS.request.sendEvent(event, true, true, '" + id
                    + "_keystroke', '" + command + "');}\n";
        }
        
        return event;
    }
    
}