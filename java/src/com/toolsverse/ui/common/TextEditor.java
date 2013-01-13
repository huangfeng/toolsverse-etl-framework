/*
 * TextEditor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.awt.Point;
import java.util.Set;

/**
 * Generic TextEditor interface. If some class implements TextEditor it is indication for the plug-in that it can attach itself to it. 
 * For example Search and Replace plug-in can be attached to the class implementing TextEditor.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface TextEditor extends IText
{
    
    /**
     * Set the focus to the text editor.
     *
     * @param lastpos the "lastpos" flag. If true maintain last cursor position. 
     */
    void doRequestFocus(boolean lastpos);
    
    /**
     * Gets the cursor position.
     *
     * @return the cursor position
     */
    Point getCursorPosition();
    
    /**
     * Gets the editor id.
     *
     * @return the editor id
     */
    String getEditorId();
    
    /**
     * Gets the selected text.
     *
     * @return the selected text
     */
    String getSelectedText();
    
    /**
     * Gets the text.
     *
     * @return the text
     */
    String getText();
    
    /**
     * Gets the text editor.
     *
     * @return the text editor
     */
    TextEditor getTextEditor();
    
    /**
     * Gets the word at current cursor position.
     *
     * @param chars the allowed characters
     * @return the word at current cursor position
     */
    String getWordAtCursor(Set<Character> chars);
    
    /**
     * Checks if component is editable.
     *
     * @return true, if is editable
     */
    boolean isEditable();
    
    /**
     * Refresh editor.
     */
    void refreshEditor();
    
    /**
     * Replace string using given filter.
     *
     * @param filter the filter
     */
    void replace(TextEditorSearch filter);
    
    /**
     * Replace all using given filter.
     *
     * @param filter the filter
     */
    void replaceAll(TextEditorSearch filter);
    
    /**
     * Search using given filter.
     *
     * @param filter the filter
     */
    void search(TextEditorSearch filter);
    
    /**
     * Sets the cursor position.
     *
     * @param point the point
     * @param schedule the schedule
     */
    void setCursorPosition(Point point, boolean schedule);
    
    /**
     * Sets the text.
     *
     * @param text the new text
     */
    void setText(String text);
}
