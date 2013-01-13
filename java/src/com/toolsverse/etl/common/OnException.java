/*
 * OnException.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.io.Serializable;
import java.sql.Connection;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.util.Utils;

/**
 * The default exception handler for the most etl related tasks. 
 *
 * @see com.toolsverse.etl.common.MergeHandler
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public abstract class OnException extends ConditionalExecution implements
        Serializable
{
    
    /** The ON_EXCEPTION_RAISE action. */
    public static final int ON_EXCEPTION_RAISE = 1;
    
    /** The ON_EXCEPTION_IGNORE action. The handler will ignore exception. */
    public static final int ON_EXCEPTION_IGNORE = 2;
    
    /** The ON_EXCEPTION_CONTINUE action. The handler will ignore current exception and move to the next task.*/
    public static final int ON_EXCEPTION_CONTINUE = 3;
    
    /** The ON_EXCEPTION_MERGE action. The handler will execute MergeHandler#onMerge if there are any configured. */
    public static final int ON_EXCEPTION_MERGE = 4;
    
    /** The ON_PARSE_EXCEPTION type. */
    public static final int ON_PARSE_EXCEPTION = 5;
    
    /** The  ON_EXCEPTION_RAISE action. The handler will raise exception. */
    public static final Integer ON_EXCEPTION_RAISE_KEY = ON_EXCEPTION_RAISE;
    
    /** The ON_EXCEPTION_RAISE type. Can be used instead of ON_EXCEPTION_RAISE action. */
    public static final String ON_EXCEPTION_RAISE_STR = "raise";
    
    /** The ON_EXCEPTION_IGNORE type. Can be used instead of ON_EXCEPTION_IGNORE action.  */
    public static final String ON_EXCEPTION_IGNORE_STR = "ignore";
    
    /** The ON_EXCEPTION_CONTINUE type. Can be used instead of ON_EXCEPTION_CONTINUE action. */
    public static final String ON_EXCEPTION_CONTINUE_STR = "continue";
    
    /** The ON_EXCEPTION_MERGE type. Can be used instead of ON_EXCEPTION_MERGE action.  */
    public static final String ON_EXCEPTION_MERGE_STR = "merge";
    
    /** The ON_PARSE_EXCEPTION type. */
    public static final String ON_PARSE_EXCEPTION_STR = "ignoreparseerror";
    
    /** The maps on exception types to actions. */
    public static final Map<String, Integer> ON_EXCEPTION = new HashMap<String, Integer>();
    static
    {
        ON_EXCEPTION.put(ON_EXCEPTION_RAISE_STR, ON_EXCEPTION_RAISE_KEY);
        ON_EXCEPTION.put(ON_EXCEPTION_IGNORE_STR, ON_EXCEPTION_IGNORE);
        ON_EXCEPTION.put(ON_EXCEPTION_CONTINUE_STR, ON_EXCEPTION_CONTINUE);
        ON_EXCEPTION.put(ON_EXCEPTION_MERGE_STR, ON_EXCEPTION_MERGE);
        ON_EXCEPTION.put(ON_PARSE_EXCEPTION_STR, ON_PARSE_EXCEPTION);
    }
    
    /** The name. */
    private String _name;
    
    /** The on exception action. */
    private int _onExceptionAction;
    
    /** The exception mask. */
    private String _exceptionMask;
    
    /** The key field. */
    private String _keyFields;
    
    /** The _save point. */
    private boolean _savePoint;
    
    /** The on exception actions. */
    private final List<Integer> _exceptionActions;
    
    /**
     * Instantiates a new OnException.
     */
    public OnException()
    {
        _name = null;
        _onExceptionAction = ON_EXCEPTION_RAISE;
        _exceptionMask = null;
        _keyFields = null;
        _savePoint = false;
        _exceptionActions = new ArrayList<Integer>();
    }
    
    /**
     * Finds the index of the exception handler. The instance of the OnException class can be configured to have multiple exception handlers based on the
     * exception string and message. For example: do ON_EXCEPTION_RAISE if message is "abc" and do ON_EXCEPTION_IGNORE is message is "xyz".
     *      
     * @param exception the exception string. It is possible to use ";" to separate exceptions
     * @param message the message to find
     * @return the index of the exception handler
     */
    private int findException(String exception, String message)
    {
        String exStr;
        
        if (exception == null || message == null)
            return -1;
        
        String[] exceptions = exception.split(";", -1);
        
        if (exceptions == null || exceptions.length == 0)
            return -1;
        
        message = message.toUpperCase();
        
        for (int i = 0; i < exceptions.length; i++)
        {
            exStr = exceptions[i];
            
            if (!Utils.isNothing(exStr) && !Utils.isNothing(message)
                    && message.indexOf(exStr.toUpperCase()) >= 0)
                return i;
        }
        
        return -1;
        
    }
    
    /**
     * Gets the list "on exception" actions.
     *
     * @return the list "on exception" actions
     */
    public List<Integer> getExceptionActions()
    {
        return _exceptionActions;
    }
    
    /**
     * Gets the exception mask. The OnException will be looking for this mask in the exception's message and will either execute appropriate handler or raise exception.
     *
     * @return the exception mask
     */
    public String getExceptionMask()
    {
        return _exceptionMask;
    }
    
    /**
     * Gets the name of the key fields. Used by MergeHandler to generate where clause for the update statement. 
     *
     * @return the name of the key fields
     */
    public String getKeyFields()
    {
        return _keyFields;
    }
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the on exception action. For example ON_EXCEPTION_IGNORE
     *
     * @return the on exception action
     */
    public int getOnExceptionAction()
    {
        return _onExceptionAction;
    }
    
    /**
     * Parsers given type and creates a list of "on exception" actions.
     *
     * @param type the type
     * @return the list of "on exception" actions
     */
    public int getOnExceptionActions(String type)
    {
        if (type == null)
        {
            _exceptionActions.add(ON_EXCEPTION_RAISE_KEY);
            return ON_EXCEPTION_RAISE;
        }
        
        Integer value = null;
        int ret = 0;
        
        String[] actions = type.split(";");
        _exceptionActions.clear();
        
        for (int i = 0; i < actions.length; i++)
        {
            type = actions[i];
            
            value = ON_EXCEPTION.get(type.toLowerCase());
            
            if (value == null)
                value = ON_EXCEPTION_RAISE_KEY;
            
            _exceptionActions.add(value);
            
            ret = ret | value.intValue();
        }
        
        return ret;
    }
    
    /**
     * Handles the exception.
     *
     * @param conn the connection
     * @param sql the currently executed sql statement
     * @param ex the exception
     * @param row the current data set row
     * @param mergeHandler the merge handler
     * @return the "on exception" action
     * @throws Exception in case of any error
     */
    public int handleException(Connection conn, String sql, Exception ex,
            long row, MergeHandler mergeHandler)
        throws Exception
    {
        String message = ex.getMessage();
        
        if (Utils.isNothing(message))
            message = ex.toString();
        
        if (Utils.isNothing(message))
            message = Utils.getStackTraceAsString(ex);
        
        int action = getOnExceptionAction();
        String exception = getExceptionMask();
        String keyField = getKeyFields();
        List<Integer> configExceptions = getExceptionActions();
        
        if ((action & OnException.ON_EXCEPTION_RAISE) == action)
            throw ex;
        
        int index = findException(exception, message);
        
        if (index >= 0 && index < configExceptions.size())
        {
            action = (configExceptions.get(index)).intValue();
            
            if (action == OnException.ON_EXCEPTION_MERGE)
            {
                if (mergeHandler == null)
                    throw ex;
                
                mergeHandler.onMerge(this, conn, sql, keyField, row);
            }
            return action;
        }
        
        if (index < 0 && Utils.isNothing(exception))
            for (int i = 0; i < configExceptions.size(); i++)
            {
                action = (configExceptions.get(i)).intValue();
                
                if (action == OnException.ON_EXCEPTION_MERGE)
                {
                    if (mergeHandler == null)
                        throw ex;
                    
                    mergeHandler.onMerge(this, conn, sql, keyField, row);
                }
            }
        else if (index < 0 && !Utils.isNothing(exception))
            throw ex;
        
        return action;
    }
    
    /**
     * Checks if save point on exception enabled.
     *
     * @return true, if save point enabled
     */
    public boolean isSavePoint()
    {
        return _savePoint;
    }
    
    /**
     * Sets the exception mask. The OnException will be looking for this mask in the exception's message and will either execute appropriate handler or raise exception. 
     * 
     * @param value the new exception mask
     */
    public void setExceptionMask(String value)
    {
        _exceptionMask = value;
    }
    
    /**
     * Sets the name of the key fields. Used by MergeHandler to generate where clause for the update statement.
     *
     * @param value the new names of the key fields
     */
    public void setKeyFields(String value)
    {
        _keyFields = value;
    }
    
    /**
     * Sets the name.
     *
     * @param value the new name
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the on exception action.
     *
     * @param value the new on exception action
     */
    public void setOnExceptionAction(int value)
    {
        _onExceptionAction = value;
    }
    
    /**
     * Sets the on exception action using given on exception type.
     *
     * @param type the on exception type
     */
    public void setOnExceptionAction(String type)
    {
        _onExceptionAction = getOnExceptionActions(type);
    }
    
    /**
     * Sets the save point on exception.
     *
     * @param value the new save point on exception
     */
    public void setSavePoint(boolean value)
    {
        _savePoint = value;
    }
    
}
