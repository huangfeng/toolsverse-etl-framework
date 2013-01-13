/*
 * TaskResult.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.io.Serializable;

import com.toolsverse.etl.common.DataSet;

/**
 * All "execute" methods of the class implementing <code>OnTask</code> interface must return this data structure.      
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class TaskResult implements Serializable
{
    
    /**
     * The result codes
     */
    public static enum TaskResultCode
    {
        CONTINUE, REJECT, STOP, HALT
    }
    
    /** CONTINUE to the next task regardless of the exception. */
    public static TaskResult CONTINUE = new TaskResult();
    
    /** REJECT any changes made by the task. */
    public static TaskResult REJECT = new TaskResult(TaskResultCode.REJECT);
    
    /** The STOP current task. Do not continue to the next task. */
    public static TaskResult STOP = new TaskResult(TaskResultCode.STOP);
    
    /** HALT etl process. */
    public static TaskResult HALT = new TaskResult(TaskResultCode.HALT);
    
    /** The result. */
    private TaskResultCode _result;
    
    /** The data set. */
    private DataSet _dataSet;
    
    /** The error. */
    private String _error;
    
    /**
     * Instantiates a new task result.
     */
    public TaskResult()
    {
        _result = TaskResultCode.CONTINUE;
        _dataSet = null;
        _error = null;
    }
    
    /**
     * Instantiates a new task result.
     *
     * @param dataSet the data set
     */
    public TaskResult(DataSet dataSet)
    {
        _result = TaskResultCode.CONTINUE;
        _dataSet = dataSet;
        _error = null;
    }
    
    /**
     * Instantiates a new task result.
     *
     * @param result the result
     */
    public TaskResult(TaskResultCode result)
    {
        _result = result;
        _dataSet = null;
    }
    
    /**
     * Instantiates a new task result.
     *
     * @param result the result
     * @param dataSet the data set
     */
    public TaskResult(TaskResultCode result, DataSet dataSet)
    {
        _result = result;
        _dataSet = dataSet;
        _error = null;
    }
    
    /**
     * Gets the data set.
     *
     * @return the data set
     */
    public DataSet getDataSet()
    {
        return _dataSet;
    }
    
    /**
     * Gets the error.
     *
     * @return the error
     */
    public String getError()
    {
        return _result == TaskResultCode.HALT ? _error : null;
    }
    
    /**
     * Gets the result.
     *
     * @return the result
     */
    public TaskResultCode getResult()
    {
        return _result;
    }
    
    /**
     * Sets the data set.
     *
     * @param value the new data set
     */
    public void setDataSet(DataSet value)
    {
        _dataSet = value;
    }
    
    /**
     * Sets the error.
     *
     * @param value the new error
     */
    public void setError(String value)
    {
        _error = value;
    }
    
    /**
     * Sets the result.
     *
     * @param value the new result
     */
    public void setResult(TaskResultCode value)
    {
        _result = value;
    }
    
}
