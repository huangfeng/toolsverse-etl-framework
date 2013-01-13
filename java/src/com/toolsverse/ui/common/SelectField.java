/*
 * SelectField.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

/**
 * This interface must be implemented by the UI components which usually contain two parts: text field and a button. 
 * When user clicks on a button the UI component for selecting value pops up. Clicking OK returns value back to the text field. 
 * There could be other applications but this is main usage pattern.     
 *
 * @param <R> the generic type of the value
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface SelectField<R>
{
    
    /**
     * Gets the value.
     *
     * @return the value
     */
    R getValue();
    
    /**
     * Checks if is enabled.
     *
     * @return true, if is enabled
     */
    boolean isEnabled();
    
    /**
     * Selects the value. Executed when value is selected in the pop up UI component.  
     */
    void selectValue();
    
    /**
     * Sets the enabled.
     *
     * @param value the new enabled
     */
    void setEnabled(boolean value);
    
    /**
     * Sets the value.
     *
     * @param value the new value
     */
    void setValue(R value);
    
    /**
     * Converts value to the string. Used to display value in the text field.
     *
     * @param value the value
     * @return the string
     */
    String value2Str(R value);
}
