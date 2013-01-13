/*
 * TextEditorSearch.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ui.common;

import java.io.Serializable;

/**
 * This interface must be implemented by the class which defines "text search and replace" parameters.   
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface TextEditorSearch extends Serializable
{
    
    /** search "forward". */
    static int DIRECTION_FORWARD = 0;
    
    /** search "back". */
    static int DIRECTION_BACK = 1;
    
    /** search entire text. */
    static int SCOPE_ALL = 0;
    
    /** search from the current possition . */
    static int SCOPE_CURRENT = 1;
    
    /**
     * Gets the "case sensitive" flag.
     *
     * @return the case "case sensitive" flag
     */
    Boolean getCaseSens();
    
    /**
     * Gets the direction.
     *
     * @return the direction
     */
    Integer getDirection();
    
    /**
     * Gets the regular expression flag.
     *
     * @return the regular expression flag
     */
    Boolean getRegExp();
    
    /**
     * Gets the replace string.
     *
     * @return the replace string
     */
    String getReplaceStr();
    
    /**
     * Gets the scope.
     *
     * @return the scope
     */
    Integer getScope();
    
    /**
     * Gets the search starring.
     *
     * @return the search string
     */
    String getSearchStr();
    
    /**
     * Gets the "whole word" flag.
     *
     * @return the "whole word" flag
     */
    Boolean getWholeWord();
    
    /**
     * Checks if "is first" .
     *
     * @return true, if search must move cursor to the first position in the line.   
     */
    boolean isFirst();
}
