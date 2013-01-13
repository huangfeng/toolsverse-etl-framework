/*
 * ControllerImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;

import com.toolsverse.mvc.model.Model;
import com.toolsverse.mvc.view.View;

/**
 * The default implementation of the Controller interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ControllerImpl implements Controller
{
    
    /**
     * The listener interface for receiving controllerAction events.
     * The class that is interested in processing a Action
     * event implements this interface, and the object created
     * with that class is registered with a component.
     */
    private class ControllerActionListener implements ActionListener
    {
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent
         * )
         */
        public void actionPerformed(ActionEvent event)
        {
            if (!isEnabled() || _view == null
                    || _view.getControllerAdapters() == null)
                return;
            
            List<ControllerAdapter> controllerAdapters = _view
                    .getControllerAdapters();
            
            for (ControllerAdapter controllerAdapter : controllerAdapters)
            {
                boolean stop = false;
                
                if (!controllerAdapter.isEnabled())
                    continue;
                
                stop = controllerAdapter.viewAction(ControllerImpl.this,
                        event.getSource(), (event).getActionCommand());
                
                if (stop)
                    return;
                
                if (controllerAdapter.supportsActionAnnotation((event)
                        .getActionCommand()))
                    stop = controllerAdapter.viewAnnotationAction(
                            ControllerImpl.this, event.getSource(),
                            (event).getActionCommand());
                
                if (stop)
                    return;
                
            }
        }
    }
    
    /**
     * The listener interface for receiving Model Change events.
     * The class that is interested in processing a Model Change
     * event implements this interface, and the object created
     * with that class is registered with a component.
     */
    private class ModelChangeListener implements PropertyChangeListener
    {
        
        /*
         * (non-Javadoc)
         * 
         * @see java.beans.PropertyChangeListener#propertyChange(java.beans.
         * PropertyChangeEvent)
         */
        public void propertyChange(PropertyChangeEvent event)
        {
            modelChanged(event);
        }
    }
    
    /**
     * The listener interface for receiving Ciew Change events.
     * The class that is interested in processing a View Change
     * event implements this interface, and the object created
     * with that class is registered with a component.
     */
    private class ViewChangeListener implements PropertyChangeListener
    {
        
        /*
         * (non-Javadoc)
         * 
         * @see java.beans.PropertyChangeListener#propertyChange(java.beans.
         * PropertyChangeEvent)
         */
        public void propertyChange(PropertyChangeEvent event)
        {
            viewChanged(event);
        }
    }
    
    /** The model change listener. */
    private PropertyChangeListener _modelChangeListener;
    
    /** The view change listener. */
    private PropertyChangeListener _viewChangeListener;
    
    /** The controller action listener. */
    private ActionListener _controllerActionListener;
    
    /** The is initializing flag. */
    private boolean _isInitializing;
    
    /** The model. */
    private Model _model;
    
    /** The view. */
    private View _view;
    
    /** The enabled flag. */
    private boolean _enabled;
    
    /**
     * Instantiates a new ControllerImpl.
     */
    public ControllerImpl()
    {
        _enabled = true;
        _modelChangeListener = new ModelChangeListener();
        _viewChangeListener = new ViewChangeListener();
        _controllerActionListener = new ControllerActionListener();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#convertForDisplay(java.lang.
     * String, java.lang.Object)
     */
    public Object convertForDisplay(String attributeName, Object input)
    {
        if (!isEnabled() || _view == null
                || _view.getControllerAdapters() == null)
            return input;
        
        List<ControllerAdapter> controllerAdapters = _view
                .getControllerAdapters();
        
        for (ControllerAdapter controllerAdapter : controllerAdapters)
        {
            if (!controllerAdapter.isEnabled())
                continue;
            
            input = controllerAdapter.convertForDisplay(this, attributeName,
                    input);
            
            if (controllerAdapter
                    .supportsConvertForDisplayAnnotation(attributeName))
            {
                input = controllerAdapter.convertForDisplayAnnotation(this,
                        attributeName, input);
            }
        }
        
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#convertForStorage(java.lang.
     * String, java.lang.Object)
     */
    public Object convertForStorage(String attributeName, Object input)
    {
        if (!isEnabled() || _view == null
                || _view.getControllerAdapters() == null)
            return input;
        
        List<ControllerAdapter> controllerAdapters = _view
                .getControllerAdapters();
        
        for (ControllerAdapter controllerAdapter : controllerAdapters)
        {
            if (!controllerAdapter.isEnabled())
                continue;
            
            input = controllerAdapter.convertForStorage(this, attributeName,
                    input);
            
            if (controllerAdapter
                    .supportsConvertForStorageAnnotation(attributeName))
            {
                input = controllerAdapter.convertForStorageAnnotation(this,
                        attributeName, input);
            }
        }
        
        return input;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#getMasterModel()
     */
    public Model getMasterModel()
    {
        return _model;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#getMasterView()
     */
    public View getMasterView()
    {
        return _view;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#init()
     */
    public void init()
    {
        init(null, null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#init(com.toolsverse.mvc.model
     * .Model, com.toolsverse.mvc.controller.ControllerAdapter)
     */
    public void init(Model model, ControllerAdapter controllerAdapter)
    {
        if (!isEnabled())
            return;
        
        try
        {
            _isInitializing = true;
            
            if (_view == null)
                return;
            
            if (model == null)
                model = _model;
            
            if (model == null)
                return;
            
            Map<String, Model.Attribute> attrs = model.getAllAttributes();
            
            if (attrs == null)
                return;
            
            Map<String, ? extends Object> attributes = null;
            
            try
            {
                if (controllerAdapter != null)
                {
                    attributes = controllerAdapter.getAttributes();
                    controllerAdapter.setIsInitializing(true);
                }
                
                for (String attributeName : attrs.keySet())
                {
                    if (attributes != null
                            && !attributes.containsKey(attributeName))
                        continue;
                    
                    _view.populate(attributeName, model.access(attributeName));
                }
            }
            finally
            {
                if (controllerAdapter != null)
                    controllerAdapter.setIsInitializing(false);
            }
        }
        finally
        {
            _isInitializing = false;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#isEnabled()
     */
    public boolean isEnabled()
    {
        return _enabled;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#isInitializing()
     */
    public boolean isInitializing()
    {
        return _isInitializing;
    }
    
    /**
     * Executed when model has changed. Notifies view about the change.
     *
     * @param event the event
     */
    private void modelChanged(PropertyChangeEvent event)
    {
        if (!isEnabled())
            return;
        
        if (_model != null)
            _view.populate(event.getPropertyName(), event.getNewValue());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#registerModel(com.toolsverse
     * .mvc.model.Model)
     */
    public void registerModel(Model model)
    {
        if (model != null)
        {
            model.removePropertyChangeListener(_modelChangeListener);
            
            model.addPropertyChangeListener(_modelChangeListener);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#registerView(com.toolsverse.
     * mvc.view.View)
     */
    public void registerView(View view)
    {
        if (view != null)
        {
            _view.addActionListener(_controllerActionListener);
            _view.addPropertyChangeListener(_viewChangeListener);
            
            _view.setController(this);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.controller.Controller#setEnabled(boolean)
     */
    public void setEnabled(boolean enabled)
    {
        _enabled = enabled;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#setMasterModel(com.toolsverse
     * .mvc.model.Model)
     */
    public void setMasterModel(Model model)
    {
        unRegisterModel(_model);
        
        _model = model;
        
        registerModel(_model);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#setMasterView(com.toolsverse
     * .mvc.view.View)
     */
    public void setMasterView(View view)
    {
        unRegisterView(_view);
        
        _view = view;
        
        registerView(_view);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#unRegisterModel(com.toolsverse
     * .mvc.model.Model)
     */
    public void unRegisterModel(Model model)
    {
        if (model != null)
            model.removePropertyChangeListener(_modelChangeListener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.controller.Controller#unRegisterView(com.toolsverse
     * .mvc.view.View)
     */
    public void unRegisterView(View view)
    {
        if (view != null)
        {
            _view.setController(null);
            
            view.removeActionListener(_controllerActionListener);
            view.removePropertyChangeListener(_viewChangeListener);
        }
    }
    
    /**
     * Executed when view has changed. Notifies model about the change.
     *
     * @param event the event
     */
    private void viewChanged(PropertyChangeEvent event)
    {
        if (!isEnabled())
            return;
        
        if (_view != null)
            _model.populate(event.getPropertyName(), event.getNewValue());
    }
    
}
