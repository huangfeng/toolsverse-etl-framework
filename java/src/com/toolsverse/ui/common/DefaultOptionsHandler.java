/*
 * DefaultOptionsHandler.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.awt.event.ActionListener;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * The default implementation of the OptionsHandler interface and cross-container, singleton entry point to the OptionsHandler functionality.
 * It automatically instantiates appropriate OptionsHandler for the current execution mode (client, web).
 * 
 * <p>
 * Example (will work the same way in the Swing app and in the Web browser):
 * <p><pre>
 * DefaultOptionsHandler.instance().handleOptions(null, Utils.format(IdeResource.CANNOT_DELETE_NODE_MSG.getValue(),
 *                                                new String[] {node.getName()}), JOptionPane.ERROR_MESSAGE, null);
 *
 * </pre>
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class DefaultOptionsHandler implements OptionsHandler
{
    
    /** The instance of the DefaultOptionsHandler. */
    private volatile static DefaultOptionsHandler _instance;
    
    /**
     * Returns and instance of the DefaultOptionsHandler.
     *
     * @return the default options handler
     */
    public static DefaultOptionsHandler instance()
    {
        if (_instance == null)
        {
            synchronized (DefaultOptionsHandler.class)
            {
                if (_instance == null)
                    _instance = new DefaultOptionsHandler();
            }
        }
        
        return _instance;
    }
    
    /** The options handler implementation for the current execution mode (client, web). */
    private OptionsHandler _optionsHandler;
    
    /**
     * Instantiates a new DefaultOptionsHandler.
     */
    private DefaultOptionsHandler()
    {
        try
        {
            _optionsHandler = (OptionsHandler)ObjectFactory.instance().get(
                    OptionsHandler.class.getName(), OptionsHandler.class);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            _optionsHandler = null;
        }
    }
    
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
        if (_optionsHandler != null)
            return _optionsHandler.handleOptions(parent, message, messageType,
                    feedback);
        else
            return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.ui.common.OptionsHandler#handleOptions(java.lang.Object,
     * java.lang.Object, int, java.lang.Object, java.awt.event.ActionListener)
     */
    public Object handleOptions(Object parent, Object message, int messageType,
            Object input, ActionListener feedback)
    {
        if (_optionsHandler != null)
            return _optionsHandler.handleOptions(parent, message, messageType,
                    input, feedback);
        else
            return null;
        
    }
    
}
