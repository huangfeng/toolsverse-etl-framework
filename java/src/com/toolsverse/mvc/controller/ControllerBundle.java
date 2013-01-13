/*
 * ControllerBundle.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import java.util.Map;

import com.toolsverse.mvc.model.Model;
import com.toolsverse.mvc.view.ComponentAdapter;
import com.toolsverse.mvc.view.View;

/**
 * Creates a new Controller and binds it with a View and Model using given ControllerAdapter. Starts MVC.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class ControllerBundle
{
    
    /** The controller. */
    private Controller _controller;
    
    /** The view. */
    private View _view;
    
    /** The adapter. */
    private ControllerAdapter _adapter;
    
    /**
     * Instantiates a new ControllerBundle.
     *
     * @param adapter the adapter
     * @param view the view
     * @param model the model
     * @throws Exception in case of any error
     */
    public ControllerBundle(ControllerAdapter adapter, View view, Model model)
            throws Exception
    {
        _adapter = adapter;
        
        _view = view;
        
        if (_adapter != null)
            _view.registerControllerAdapter(_adapter);
        
        _controller = new ControllerImpl();
        _controller.setMasterModel(model);
        _controller.setMasterView(_view);
    }
    
    /**
     * Instantiates a new ControllerBundle.
     *
     * @param view the view
     * @param model the model
     * @throws Exception in case of any error
     */
    public ControllerBundle(View view, Model model) throws Exception
    {
        this(null, view, model);
    }
    
    /**
     * Adds the component to the view.
     *
     * @param component the component
     */
    public void addComponent(Object component)
    {
        String name = _view.addComponent(component);
        
        if (name != null)
            _adapter.getAttributes().put(name, null);
    }
    
    /**
     * Adds the component to the view using behavior modifier.
     *
     * @param component the component
     * @param behavior the behavior
     */
    public void addComponent(Object component, long behavior)
    {
        String name = _view.addComponent(component, behavior);
        
        if (name != null)
            _adapter.getAttributes().put(name, null);
    }
    
    /**
     * Frees the memory. Unregisters components.
     */
    public void free()
    {
        Map<String, ComponentAdapter> comps = _view.getComponents();
        
        if (comps != null)
        {
            _view.clearComponents();
            
            _adapter.getAttributes().clear();
        }
        
        _view.unRegisterControllerAdapter(_adapter);
    }
    
    /**
     * Gets the controller.
     *
     * @return the controller
     */
    public Controller getController()
    {
        return _controller;
    }
    
    /**
     * Gets the view.
     *
     * @return the view
     */
    public View getView()
    {
        return _view;
    }
    
    /**
     * Starts MVC.
     */
    public void init()
    {
        _controller.init();
    }
    
    /**
     * Sets the controller adapter.
     *
     * @param value the new controller adapter
     */
    public void setControllerAdapter(ControllerAdapter value)
    {
        _adapter = value;
    }
}
