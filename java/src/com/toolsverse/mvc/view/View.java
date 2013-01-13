/*
 * View.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.util.List;
import java.util.Map;

import com.toolsverse.mvc.controller.Controller;
import com.toolsverse.mvc.controller.ControllerAdapter;

/**
 * This interface defines the methods required of objects filling the role of View in the Toolsverse Model/View/Controller implementation.
 * 
 * @see com.toolsverse.mvc.model.Model
 * @see com.toolsverse.mvc.controller.Controller
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface View extends PropertyChangeListener, ActionListener
{
    
    /**
     * Returns the value of the component by attribute name. For example for the text field its a field.getText(), etc.
     *
     * @param attributeName the attribute name
     * @return the object
     */
    Object access(String attributeName);
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * java.awt.event.ActionListener#actionPerformed(java.awt.event.ActionEvent)
     */
    void actionPerformed(ActionEvent e);
    
    /**
     * Adds the action listener.
     *
     * @param listener the listener
     */
    void addActionListener(ActionListener listener);
    
    /**
     * Adds the component to the view
     *
     * @param component the component
     * @return the string
     */
    String addComponent(Object component);
    
    /**
     * Adds the component to the view using behavior modifier.
     *
     * @param component the component
     * @param behavior the behavior modifier
     * @return the name of the component
     */
    String addComponent(Object component, long behavior);
    
    /**
     * Adds the property change listener.
     *
     * @param listener the listener
     */
    void addPropertyChangeListener(PropertyChangeListener listener);
    
    /**
     * Removes all components from the view.
     */
    void clearComponents();
    
    /**
     * Removes all controller adapters rom the view.
     */
    void clearControllerAdapters();
    
    /**
     * Gets the ComponentAdapter by name.
     *
     * @param name the name
     * @return the ComponentAdapter
     */
    ComponentAdapter getComponent(String name);
    
    /**
     * Gets the map where name is a name of the attribute and value is a ComponentAdapter.
     *
     * @return the the map where name is a name of the attribute and value is a ComponentAdapter
     */
    Map<String, ComponentAdapter> getComponents();
    
    /**
     * Gets the controller.
     *
     * @return the controller
     */
    Controller getController();
    
    /**
     * Gets the list of the registered controller adapters.
     *
     * @return the list controller adapters
     */
    List<ControllerAdapter> getControllerAdapters();
    
    /**
     * Sets the value of the component by attribute name. For example for the text field its a field.setText(newValue), etc.
     *
     * @param attributeName the attribute name
     * @param newValue the new value
     */
    void populate(String attributeName, Object newValue);
    
    /*
     * (non-Javadoc)
     * 
     * @see java.beans.PropertyChangeListener#propertyChange(java.beans.
     * PropertyChangeEvent)
     */
    void propertyChange(PropertyChangeEvent event);
    
    /**
     * Registers controller adapter.
     *
     * @param controllerAdapter the controller adapter
     */
    void registerControllerAdapter(ControllerAdapter controllerAdapter);
    
    /**
     * Removes the action listener.
     *
     * @param listener the listener
     */
    void removeActionListener(ActionListener listener);
    
    /**
     * Removes the component from the view.
     *
     * @param component the component
     * @return the name of the component
     */
    String removeComponent(Object component);
    
    /**
     * Removes the property change listener.
     *
     * @param listener the listener
     */
    void removePropertyChangeListener(PropertyChangeListener listener);
    
    /**
     * Sets the controller for the view
     *
     * @param controller the new controller
     */
    void setController(Controller controller);
    
    /**
     * Unregisters controller adapter.
     *
     * @param controllerAdapter the controller adapter
     */
    void unRegisterControllerAdapter(ControllerAdapter controllerAdapter);
}
