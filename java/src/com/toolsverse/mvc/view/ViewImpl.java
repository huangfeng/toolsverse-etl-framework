/*
 * ViewImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.mvc.controller.Controller;
import com.toolsverse.mvc.controller.ControllerAdapter;
import com.toolsverse.util.Null;

/**
 * ViewImpl is an abstract implementation of the View interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class ViewImpl implements View
{
    
    /** The controller. */
    private Controller _controller;
    
    /** The property change support. */
    private PropertyChangeSupport _propertyChangeSupport;
    
    /** The action listeners. */
    private List<ActionListener> _actionListeners;
    
    /** The components. */
    private Map<String, ComponentAdapter> _components;
    
    /** The controller adapters. */
    private List<ControllerAdapter> _controllerAdapters;
    
    /**
     * Instantiates a new ViewImpl.
     */
    public ViewImpl()
    {
        _controller = null;
        _propertyChangeSupport = new PropertyChangeSupport(this);
        _actionListeners = new ArrayList<ActionListener>();
        _components = new HashMap<String, ComponentAdapter>();
        _controllerAdapters = new ArrayList<ControllerAdapter>();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#access(java.lang.String)
     */
    public Object access(String attributeName)
    {
        ComponentAdapter adapter = _components.get(attributeName);
        
        if (adapter != null)
            return adapter.access();
        else
            return Null.NULL;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#actionPerformed(java.awt.event.ActionEvent)
     */
    public void actionPerformed(ActionEvent e)
    {
        for (int i = 0; i < _actionListeners.size(); i++)
            (_actionListeners.get(i)).actionPerformed(e);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#addActionListener(java.awt.event.ActionListener
     * )
     */
    public void addActionListener(ActionListener listener)
    {
        if (listener != null && !_actionListeners.contains(listener))
            _actionListeners.add(listener);
    }
    
    /**
     * Adds the adapter.
     *
     * @param adapter the adapter
     */
    public void addAdapter(ComponentAdapter adapter)
    {
        addAdapter(adapter, -1);
    }
    
    /**
     * Adds the adapter.
     *
     * @param adapter the adapter
     * @param behavior the behavior
     */
    public void addAdapter(ComponentAdapter adapter, long behavior)
    {
        if (adapter.getName() == null)
        {
            adapter.dettach();
            
            return;
        }
        
        if (behavior != -1)
            adapter.setBehaviorMask(behavior);
        
        _components.put(adapter.getName(), adapter);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#addComponent(java.lang.Object)
     */
    public String addComponent(Object component)
    {
        return addComponent(component, ComponentAdapter.NONE);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#addPropertyChangeListener(java.beans.
     * PropertyChangeListener)
     */
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        _propertyChangeSupport.addPropertyChangeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#clearComponents()
     */
    public void clearComponents()
    {
        if (_components != null && _components.size() > 0)
        {
            Object[] names = _components.keySet().toArray();
            
            for (Object name : names)
            {
                ComponentAdapter componentAdapter = _components.get(name
                        .toString());
                
                removeComponent(componentAdapter.getComponent());
            }
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#clearControllerAdapters()
     */
    public void clearControllerAdapters()
    {
        _controllerAdapters.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#getComponent(java.lang.String)
     */
    public ComponentAdapter getComponent(String name)
    {
        return _components.get(name);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#getComponents()
     */
    public Map<String, ComponentAdapter> getComponents()
    {
        return _components;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#getController()
     */
    public Controller getController()
    {
        return _controller;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#getControllerAdapters()
     */
    public List<ControllerAdapter> getControllerAdapters()
    {
        return _controllerAdapters;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#populate(java.lang.String,
     * java.lang.Object)
     */
    public void populate(String attributeName, Object newValue)
    {
        if (Null.NULL.equals(newValue))
            return;
        
        ComponentAdapter adapter = _components.get(attributeName);
        
        if (adapter != null)
            adapter.populate(newValue);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#propertyChange(java.beans.PropertyChangeEvent
     * )
     */
    public void propertyChange(PropertyChangeEvent event)
    {
        PropertyChangeListener[] listeners = _propertyChangeSupport
                .getPropertyChangeListeners();
        
        if (listeners == null)
            return;
        
        for (int i = 0; i < listeners.length; i++)
            listeners[i].propertyChange(event);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#registerControllerAdapter(com.toolsverse
     * .mvc.controller.ControllerAdapter)
     */
    public void registerControllerAdapter(ControllerAdapter controllerAdapter)
    {
        if (_controllerAdapters.contains(controllerAdapter))
            return;
        
        _controllerAdapters.add(controllerAdapter);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.view.View#removeActionListener(java.awt.event.
     * ActionListener)
     */
    public void removeActionListener(ActionListener listener)
    {
        if (listener != null && _actionListeners.contains(listener))
            _actionListeners.remove(listener);
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#removePropertyChangeListener(java.beans.
     * PropertyChangeListener)
     */
    public void removePropertyChangeListener(PropertyChangeListener listener)
    {
        _propertyChangeSupport.removePropertyChangeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#setController(com.toolsverse.mvc.controller
     * .Controller)
     */
    public void setController(Controller controller)
    {
        _controller = controller;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.view.View#unRegisterControllerAdapter(com.toolsverse
     * .mvc.controller.ControllerAdapter)
     */
    public void unRegisterControllerAdapter(ControllerAdapter controllerAdapter)
    {
        _controllerAdapters.remove(controllerAdapter);
    }
}
