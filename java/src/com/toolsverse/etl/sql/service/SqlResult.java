/*
 * SqlResult.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.sql.service;

import java.io.Serializable;
import java.util.Map;

import com.toolsverse.etl.common.DataSet;

/**
 * SqlResult is a data structure returned by various methods of SqlService. Contains objects retrieved from the database.
 * 
 * @see com.toolsverse.etl.sql.service.SqlService
 * @see com.toolsverse.etl.common.DataSet
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlResult implements Serializable
{
    
    /** The result. */
    private String _result;
    
    /** The map of named data sets. */
    private Map<String, DataSet> _dataSets;
    
    /** The exception. */
    private Exception _exception;
    
    /** The "can commit" flag. */
    private boolean _canCommit;
    
    /** The "can rollback" flag. */
    private boolean _canRollback;
    
    /**
     * Instantiates a new sql result.
     *
     * @param result the result
     * @param dataSets the map of named data sets
     */
    public SqlResult(String result, Map<String, DataSet> dataSets)
    {
        this(result, dataSets, false, false);
    }
    
    /**
     * Instantiates a new sql result.
     *
     * @param result the result
     * @param dataSets the map of named data sets
     * @param canCommit the "can commit" flag
     * @param canRollback the "can rollback" flag
     */
    public SqlResult(String result, Map<String, DataSet> dataSets,
            boolean canCommit, boolean canRollback)
    {
        _result = result;
        
        _dataSets = dataSets;
        
        _exception = null;
        
        _canCommit = canCommit;
        
        _canRollback = canRollback;
    }
    
    /**
     * "Can commit" flag.
     *
     * @return true, if calling method can commit transaction
     */
    public boolean canCommit()
    {
        return _canCommit;
    }
    
    /**
     * "Can rollback" flag.
     *
     * @return true, if calling method can rollback transaction
     */
    public boolean canRollback()
    {
        return _canRollback;
    }
    
    /**
     * Gets the map of named data sets.
     * 
     * @return the map named data sets
     */
    public Map<String, DataSet> getDataSets()
    {
        return _dataSets;
    }
    
    /**
     * Gets the exception.
     * 
     * @return the exception
     */
    public Exception getException()
    {
        return _exception;
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
     * Sets the "can commit" flag.
     *
     * @param value the new value for "can commit" flag
     */
    public void setCanCommit(boolean value)
    {
        _canCommit = value;
    }
    
    /**
     * Sets the "can rollback" rollback.
     *
     * @param value the new value for the "can rollback" flag
     */
    public void setCanRollback(boolean value)
    {
        _canRollback = value;
    }
    
    /**
     * Sets the exception.
     * 
     * @param ex
     *            the new exception
     */
    public void setException(Exception ex)
    {
        _exception = ex;
    }
    
}
