/*
 * EtlResponse.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.io.Serializable;
import java.util.Date;

import com.toolsverse.etl.core.config.EtlConfig;

/**
 * The data structure returned by etl process when it finished.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlResponse implements Serializable
{
    
    /** The last executed code. */
    private String _code;
    
    /** The file name. */
    private String _fileName;
    
    /** The error line number. */
    private int _line;
    
    /** The exception. */
    private Exception _exception;
    
    /** The start time. */
    private Date _startTime;
    
    /** The end time. */
    private Date _endTime;
    
    /** The return code. */
    private int _retCode;
    
    /**
     * Instantiates a new etl response.
     * 
     */
    public EtlResponse()
    {
        _fileName = null;
        _line = -1;
        _exception = null;
        _startTime = new Date();
        _endTime = null;
        _code = null;
        _retCode = EtlConfig.RETURN_OK;
    }
    
    /**
     * Instantiates a new etl response using given return code. 
     *
     * @param retCode the return code. Possible values: EtlConfig.RETURN_OK, EtlConfig.RETURN_ERROR, EtlConfig.RETURN_NO_CONFIG, EtlConfig.RETURN_CONFIG_NOT_INITIALIZED
     */
    public EtlResponse(int retCode)
    {
        super();
        
        _retCode = retCode;
    }
    
    /**
     * Gets the last executed code (sql).
     *
     * @return the last executed code (sql)
     */
    public String getCode()
    {
        return _code;
    }
    
    /**
     * Gets the end time.
     *
     * @return the end time
     */
    public Date getEndTime()
    {
        return _endTime;
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
     * Gets the file name.
     * 
     * @return the file name
     */
    public String getFileName()
    {
        return _fileName;
    }
    
    /**
     * Gets the error line number.
     * 
     * @return the error line number
     */
    public int getLine()
    {
        return _line;
    }
    
    /**
     * Gets the return code. Possible values: EtlConfig.RETURN_OK, EtlConfig.RETURN_ERROR, EtlConfig.RETURN_NO_CONFIG, EtlConfig.RETURN_CONFIG_NOT_INITIALIZED
     *
     * @return the return code
     */
    public int getRetCode()
    {
        return _retCode;
    }
    
    /**
     * Gets the start time.
     * 
     * @return the start time
     */
    public Date getStartTime()
    {
        return _startTime;
    }
    
    /**
     * Sets the last executed code(sql).
     *
     * @param value the new code
     */
    public void setCode(String value)
    {
        _code = value;
    }
    
    /**
     * Sets the end time.
     *
     * @param value the new end time
     */
    public void setEndTime(Date value)
    {
        _endTime = value;
    }
    
    /**
     * Sets the exception.
     *
     * @param value the new exception
     */
    public void setException(Exception value)
    {
        _exception = value;
    }
    
    /**
     * Sets the file name.
     * 
     * @param value
     *            the new file name
     */
    public void setFileName(String value)
    {
        _fileName = value;
    }
    
    /**
     * Sets the error line number.
     * 
     * @param value
     *            the new error line number
     */
    public void setLine(int value)
    {
        _line = value;
    }
    
    /**
     * Sets the return code. Possible values: EtlConfig.RETURN_OK, EtlConfig.RETURN_ERROR, EtlConfig.RETURN_NO_CONFIG, EtlConfig.RETURN_CONFIG_NOT_INITIALIZED
     *
     * @param value the new return code
     */
    public void setRetCode(int value)
    {
        _retCode = value;
    }
    
    /**
     * Sets the start time.
     *
     * @param value the new start time
     */
    public void setStartTime(Date value)
    {
        _startTime = value;
    }
}
