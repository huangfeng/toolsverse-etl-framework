/*
 * ConditionalExecution.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

/**
 * This is a data structure to store conditions and related information.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class ConditionalExecution
{
    
    /** The condition code. */
    private String _conditionCode;
    
    /** The condition connection name. */
    private String _conditionConnectionName;
    
    /** The language. */
    private String _lang;
    
    /**
     * Instantiates a new conditional execution.
     */
    public ConditionalExecution()
    {
        _conditionCode = null;
        _conditionConnectionName = null;
        _lang = Variable.DEFAULT_LANG;
    }
    
    /**
     * Gets the condition code.
     * 
     * @return the condition code
     */
    public String getConditionCode()
    {
        return _conditionCode;
    }
    
    /**
     * Gets the condition connection name.
     * 
     * @return the condition connection name
     */
    public String getConditionConnectionName()
    {
        return _conditionConnectionName;
    }
    
    /**
     * Gets the condition language. The default is SQL.
     *
     * @return the condition language
     */
    public String getConditionLang()
    {
        return _lang;
    }
    
    /**
     * Gets the default connection name.
     * 
     * @return the default connection name
     */
    public abstract String getDefaultConnectionName();
    
    /**
     * Sets the condition code.
     * 
     * @param value
     *            the new condition code
     */
    public void setConditionCode(String value)
    {
        _conditionCode = value;
    }
    
    /**
     * Sets the condition connection name.
     * 
     * @param value
     *            the new condition connection name
     */
    public void setConditionConnectionName(String value)
    {
        _conditionConnectionName = value;
    }
    
    /**
     * Sets the condition language.
     *
     * @param value the new condition language
     */
    public void setConditionLang(String value)
    {
        _lang = value;
    }
    
}
