/*
 * Controller.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import com.toolsverse.mvc.model.Model;
import com.toolsverse.mvc.view.View;

/**
 * This interface defines the methods required of objects filling the role of Controller in the Toolseverse Model/View/Controller implementation.
 * 
 * Controller supports multiple views and models at the same time. They must be registered. 
 * 
 * @see com.toolsverse.mvc.model.Model
 * @see com.toolsverse.mvc.view.View
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface Controller
{
    /**
     * Converts value before sending it to the view. Regularly no conversion require so value just passing through. 
     *
     * @param attributeName the attribute name
     * @param input the value to convert
     * @return the object
     */
    Object convertForDisplay(String attributeName, Object input);
    
    /**
     * Converts value before sending it to the model. Regularly no conversion require so value just passing through. 
     *
     * @param attributeName the attribute name
     * @param input the value to convert
     * @return the object
     */
    Object convertForStorage(String attributeName, Object input);
    
    /**
     * Gets the master model. Model supports sub-models so this one returns the top level model.
     *
     * @return the master model
     */
    Model getMasterModel();
    
    /**
     * Gets the master view.
     *
     * @return the master view
     */
    View getMasterView();
    
    /**
     * Binds model and view. Starts MVC (populates all controls from the model).
     */
    void init();
    
    /**
     * Inits the given model using given controllerAdapter. Populates all controls from the model.
     *
     * @param model the model
     * @param controllerAdapter the controller adapter
     */
    void init(Model model, ControllerAdapter controllerAdapter);
    
    /**
     * Checks if controller is enabled. It is not the events and values from the models and views will not be crossing. 
     *
     * @return true, if is enabled
     */
    boolean isEnabled();
    
    /**
     * Checks if controller is in process of initialization. At this time there should be no interaction between models and views.
     *
     * @return true, if is initializing
     */
    boolean isInitializing();
    
    /**
     * Registers model. Used to register sub-models.
     *
     * @param model the model
     */
    void registerModel(Model model);
    
    /**
     * Registers view. Used to register sub-views.
     *
     * @param view the view
     */
    void registerView(View view);
    
    /**
     * Sets the enabled flag. It controller is not enabled the events and values from the models and views will not be crossing.
     *
     * @param enabled the new enabled flag
     */
    void setEnabled(boolean enabled);
    
    /**
     * Sets the master model.
     *
     * @param model the new master model
     */
    void setMasterModel(Model model);
    
    /**
     * Sets the master view.
     *
     * @param view the new master view
     */
    void setMasterView(View view);
    
    /**
     * Un registers model.
     *
     * @param model the model
     */
    void unRegisterModel(Model model);
    
    /**
     * Un registers view.
     *
     * @param view the view
     */
    void unRegisterView(View view);
}
