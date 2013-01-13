/*
 * MockBooleanComponent.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.event.EventListenerList;

import com.toolsverse.util.Utils;

/**
 * MockBooleanComponent
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockBooleanComponent extends MockComponent
{
    private Boolean _value;
    
    private EventListenerList _listenerList = new EventListenerList();
    
    /**
     * Adds an <code>ActionListener</code> to the button.
     * 
     * @param l
     *            the <code>ActionListener</code> to be added
     */
    public void addActionListener(ActionListener l)
    {
        _listenerList.add(ActionListener.class, l);
    }
    
    public boolean isSelected()
    {
        return _value;
    }
    
    /**
     * Removes an <code>ActionListener</code> from the button. If the listener
     * is the currently set <code>Action</code> for the button, then the
     * <code>Action</code> is set to <code>null</code>.
     * 
     * @param l
     *            the listener to be removed
     */
    public void removeActionListener(ActionListener l)
    {
        _listenerList.remove(ActionListener.class, l);
    }
    
    public void setSelected(boolean value)
    {
        if (Utils.equals(value, _value))
            return;
        ;
        
        _value = value;
        
        if (_listenerList != null)
        {
            
            Object[] listeners = _listenerList.getListenerList();
            for (int i = 1; i < listeners.length; i++)
            {
                // Lazily create the event:
                ((ActionListener)listeners[i]).actionPerformed(new ActionEvent(
                        this, ActionEvent.ACTION_PERFORMED, getName()));
            }
        }
        
    }
    
}
