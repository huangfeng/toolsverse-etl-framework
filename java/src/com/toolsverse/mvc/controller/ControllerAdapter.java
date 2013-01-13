/*
 * ControllerAdapter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import java.util.Map;

/**
 * In the Toolseverse implementation of the MVC the ControllerAdapter is where you put all event handlers and model to view and view to model data conversion.
 * 
 * The way you do this is by adding annotations ViewAction, ConvertForDisplay and ConvertForStorage to the methods of the particular ControllerAdapter implementations.
 * 
 * The ControllerAdapter implementation must be registered in the instance of the Controller interface.
 * 
 * @see com.toolsverse.mvc.controller.ViewAction
 * @see com.toolsverse.mvc.controller.ConvertForDisplay
 * @see com.toolsverse.mvc.controller.ConvertForStorage
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ControllerAdapter
{
    
    /**
     * Converts value before sending it to the view. Regularly no conversion require so value is just passing through.  You can implement this method 
     * and check attribute name but better way is to use ConvertForDisplay annotation. 
     *
     * @param controller the controller
     * @param attributeName the attribute name
     * @param input the input
     * @return the object
     */
    Object convertForDisplay(Controller controller, String attributeName,
            Object input);
    
    /**
     * Converts value before sending it to the view. The method is called by MVC framework and mark as final in the ControllerAdapterImpl.
     *
     * @param controller the controller
     * @param attributeName the attribute name
     * @param input the input
     * @return the object
     */
    Object convertForDisplayAnnotation(Controller controller,
            String attributeName, Object input);
    
    /**
     * Converts value before sending it to the model. Regularly no conversion require so value is just passing through.  You can implement this method 
     * and check attribute name but better way is to use ConvertForStorage annotation. 
     *
     * @param controller the controller
     * @param attributeName the attribute name
     * @param input the input
     * @return the object
     */
    Object convertForStorage(Controller controller, String attributeName,
            Object input);
    
    /**
     * Converts value before sending it to the model. The method is called by MVC framework and mark as final in the ControllerAdapterImpl.
     *
     * @param controller the controller
     * @param attributeName the attribute name
     * @param input the input
     * @return the object
     */
    Object convertForStorageAnnotation(Controller controller,
            String attributeName, Object input);
    
    /**
     * Gets the attributes. The key is an attribute name, the value is usually null.
     *
     * @return the attributes
     */
    Map<String, Object> getAttributes();
    
    /**
     * Checks if ControllerAdapter is enabled. It is not the events and values from the models and views will not be crossing. 
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
     * Sets the enabled flag. It it is not enabled the events and values from the models and views will not be crossing.
     *
     * @param enabled the new enabled
     */
    void setEnabled(boolean enabled);
    
    /**
     * Sets the checks if is initializing.
     *
     * @param value the new checks if is initializing
     */
    void setIsInitializing(boolean value);
    
    /**
     * Returns true if ViewAction annotation is supported for the attribute.
     *
     * @param attributeName the attribute name
     * @return true, if successful
     */
    boolean supportsActionAnnotation(String attributeName);
    
    /**
     * Returns true if ConvertForDisplay annotation is supported for the attribute.
     *
     * @param attributeName the attribute name
     * @return true, if successful
     */
    boolean supportsConvertForDisplayAnnotation(String attributeName);
    
    /**
     * Returns true if ConvertForStorage annotation is supported for the attribute.
     *
     * @param attributeName the attribute name
     * @return true, if successful
     */
    boolean supportsConvertForStorageAnnotation(String attributeName);
    
    /**
     * This is an event handler for all action events. The better way is to use ViewAction annotation.
     *
     * @param controller the controller
     * @param source the source
     * @param attributeName the attribute name
     * @return true, if action is handled for the particular event so it stops right there and not pushed further. 
     */
    boolean viewAction(Controller controller, Object source,
            String attributeName);
    
    /**
     * Event handler for the particular action event. The method is final in the ControllerAdapterImpl.
     *
     * @param controller the controller
     * @param source the source
     * @param attributeName the attribute name
     * @return true, if action is handled for the particular event so it stops right there and not pushed further. 
     */
    boolean viewAnnotationAction(Controller controller, Object source,
            String attributeName);
}
