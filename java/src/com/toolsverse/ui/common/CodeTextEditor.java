/*
 * CodeTextEditor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

/**
 * This interface must be implemented by the class which is a code text editor and requires certain plug-ins. For example Code Formatter plug-in will be
 * automatically attached to the class implementing CodeTextEditor.  
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface CodeTextEditor extends TextEditor, ICode
{
    
    /**
     * Gets the dialect.
     *
     * @return the dialect
     */
    String getDialect();
    
    /**
     * Gets the language name.
     *
     * @return the language name
     */
    String getLang();
}
