/*
 * FunctionContext.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

import java.lang.reflect.Method;

import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * The is a parameter passed to the function. Includes information such as current record, current value, etc.  
 * 
 * @see com.toolsverse.etl.common.Function
 * @see com.toolsverse.etl.common.Variable
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class FunctionContext
{
    
    /** The current row. */
    private DataSetRecord _currentRecord;
    
    /** The current value. */
    private Object _currentValue;
    
    /** The variable. */
    private Variable _variable;
    
    /** The scope. */
    private int _scope;
    
    /** The "is from cursor" flag. */
    private boolean _isFromCursor;
    
    /** The row. */
    private long _row;
    
    /**
     * Instantiates a new FunctionContext.
     */
    public FunctionContext()
    {
        _currentRecord = null;
        _currentValue = null;
        _variable = null;
        _scope = Variable.EXECUTE_AS_CONFIGURED;
        _isFromCursor = false;
        _row = -1;
    }
    
    /**
     * Executes function associated with the variable for the current execution scope.
     *
     * @param var the variable
     * @return the string the result of the function, converted to the String
     * @throws Exception in case of any error
     */
    public String execute(Variable var)
        throws Exception
    {
        _variable = var;
        
        if (var == null)
            return "";
        
        if (Utils.isNothing(var.getFunction())
                && Utils.isNothing(var.getFunctionClassName()))
            return "";
        
        Function funct = (Function)ObjectFactory.instance().get(
                var.getFunctionClassName(), true);
        
        boolean asConfigured = var.getScope() == Variable.EXECUTE_AS_CONFIGURED;
        boolean doExec = true;
        
        switch (_scope)
        {
            case Variable.EXECUTE_BEFORE:
                doExec = var.isScopeSet(_scope)
                        || (asConfigured && Utils.belongsTo(
                                funct.getBeforeFunctions(), var.getFunction()));
                break;
            case Variable.EXECUTE_AFTER:
                doExec = var.isScopeSet(_scope)
                        || (asConfigured && Utils.belongsTo(
                                funct.getAfterFunctions(), var.getFunction()));
                break;
            case Variable.EXECUTE_RUNTIME:
                doExec = var.isScopeSet(_scope)
                        || (asConfigured && Utils.belongsTo(
                                funct.getRuntimeFunctions(), var.getFunction()));
                break;
        }
        
        if (doExec)
            return (String)invoke(funct, var.getFunction());
        else
            return "";
    }
    
    /**
     * Gets the current record of the data set.
     * 
     * @return the current record
     */
    public DataSetRecord getCurrentRecord()
    {
        return _currentRecord;
    }
    
    /**
     * Gets the value of the current field of the current record of the data set.
     * 
     * @return the current value
     */
    public Object getCurrentValue()
    {
        return _currentValue;
    }
    
    /**
     * Gets the current row.
     *
     * @return the current row
     */
    public long getRow()
    {
        return _row;
    }
    
    /**
     * Gets the execution scope. The function can be executed BEFORE, AFTER and
     * at RUNTIME (any combination of these). 
     * 
     * @return the execution scope
     */
    public int getScope()
    {
        return _scope;
    }
    
    /**
     * Gets the variable associated with the execution context.
     * 
     * @return the variable
     */
    public Variable getVariable()
    {
        return _variable;
    }
    
    /**
     * Invokes the function by name using reflection and returns result of the
     * function.
     * 
     * @param funct
     *            the function
     * @param name
     *            the name of the function
     * 
     * @return the object Result of the function
     * 
     * @throws Exception in case of any error
     *             
     */
    private Object invoke(Function funct, String name)
        throws Exception
    {
        Method invokeMethod = null;
        
        invokeMethod = funct.getClass().getMethod(name,
                new Class[] {this.getClass()});
        
        return invokeMethod.invoke(funct, new Object[] {this});
    }
    
    /**
     * Checks if "from cursor" flag is set. There are two ways to iterate
     * through the data set row by row: a)store everything in the memory and
     * just have a loop or b)use a cursor if database supports it. 
     * 
     * @return true, if "from cursor" property is set
     */
    public boolean isFromCursor()
    {
        return _isFromCursor;
    }
    
    /**
     * Sets the current record of the data set.
     * 
     * @param value
     *            the new current record
     */
    public void setCurrentRecord(DataSetRecord value)
    {
        _currentRecord = value;
    }
    
    /**
     * Sets the value of the current field of the current record of the data set.
     * 
     * @param value
     *            The new current value
     */
    public void setCurrentValue(Object value)
    {
        _currentValue = value;
    }
    
    /**
     * Sets the flag "from cursor". There are two ways to iterate
     * through the data set row by row: a)store everything in the memory and
     * just have a loop or b)use a cursor if database supports it. 
     * 
     * @param value
     *            the new checks if is from table
     */
    public void setFromCursor(boolean value)
    {
        _isFromCursor = value;
    }
    
    /**
     * Sets the row.
     *
     * @param value the new row
     */
    public void setRow(long value)
    {
        _row = value;
    }
    
    /**
     * Sets the execution scope. The function can be executed
     * <code>before, after and at runtime</code> (any combination of these).
     * 
     * @param value
     *            the new scope
     */
    public void setScope(int value)
    {
        _scope = value;
    }
    
}
