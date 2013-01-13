/*
 * ConnectorResult.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.io.Serializable;

/**
 * When <code>DataSetConnector</code> finishes populating or persisting data set it returns an object of the type <code>ConnectorResult</code>.   
 *
 * @see com.toolsverse.etl.connector.DataSetConnector
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConnectorResult implements Serializable
{
    
    /** The Ok return code. */
    public static int OK_CODE = 0;
    
    /** The VALIDATION FAILED return code. */
    public static int VALIDATION_FAILED_CODE = 1;
    
    /** The ACTION FAILED return code. */
    public static int ACTION_FAILED_CODE = 2;
    
    /** The return code. */
    private int _retCode;
    
    /** The result. */
    private String _result;
    
    /**
     * Instantiates a new ConnectorResult.
     */
    public ConnectorResult()
    {
        _retCode = OK_CODE;
        _result = null;
    }
    
    /**
     * Adds the value to the result string.
     *
     * @param value the value to add
     */
    public void addResult(String value)
    {
        if (_result == null)
            _result = value;
        else
            _result = _result + "\n" + value;
    }
    
    /**
     * Gets the result string.
     *
     * @return the result string
     */
    public String getResult()
    {
        return _result;
    }
    
    /**
     * Gets the return code.
     *
     * @return the return code
     */
    public int getRetCode()
    {
        return _retCode;
    }
    
    /**
     * Sets the result string.
     *
     * @param value the new result string
     */
    public void setResult(String value)
    {
        _result = value;
    }
    
    /**
     * Sets the return code.
     *
     * @param value the new return code
     */
    public void setRetCode(int value)
    {
        _retCode = value;
    }
    
}
