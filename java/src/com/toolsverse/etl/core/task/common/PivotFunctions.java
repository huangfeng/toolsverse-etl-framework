/*
 * PivotFunctions.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.HashMap;
import java.util.Map;

import com.toolsverse.util.AvgNumber;
import com.toolsverse.util.Utils;

/**
 * This class is a library of the pivot functions, such as SUM(), AVG(), etc. This class is an singleton.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class PivotFunctions
{
    /** The instance. */
    private static PivotFunctions _instance = new PivotFunctions();
    
    /**
     * Singleton instance of the PivotFunctions.
     *
     * @return the pivot functions
     */
    public static PivotFunctions instance()
    {
        return _instance;
    }
    
    /** The call patterns. */
    private Map<String, String> _patterns;
    
    /**
     * Instantiates a new pivot functions.
     */
    private PivotFunctions()
    {
        _patterns = new HashMap<String, String>();
        
        Method[] methods = getClass().getMethods();
        
        for (Method method : methods)
        {
            Annotation[] annotations = method.getDeclaredAnnotations();
            if (annotations != null)
                for (Annotation annotation : annotations)
                {
                    if (annotation instanceof PivotFunction)
                    {
                        String name = ((PivotFunction)annotation).name();
                        String pattern = ((PivotFunction)annotation).pattern();
                        
                        _patterns.put(name, pattern);
                    }
                }
        }
        
    }
    
    /**
     * Calculates average.
     *
     * @param value the field value
     * @param currentValue the current value
     * @param groupCount the group count
     * @return the average
     */
    @PivotFunction(name = "AVG", pattern = "com.toolsverse.etl.core.task.common.PivotFunctions.instance().avg(?,?current,groupCount)")
    public Number avg(Object value, AvgNumber currentValue, int groupCount)
    {
        if (currentValue == null)
            return new AvgNumber(object2Number(value));
        
        currentValue.setCount(groupCount);
        currentValue.setSum(currentValue.getSum() + object2Number(value));
        
        return currentValue;
    }
    
    /**
     * Calculates number of rows.
     *
     * @param value the field value
     * @param groupCount the group count
     * @return the number of rows
     */
    @PivotFunction(name = "COUNT", pattern = "com.toolsverse.etl.core.task.common.PivotFunctions.instance().count(?,groupCount)")
    public int count(Object value, Integer groupCount)
    {
        return groupCount;
    }
    
    /**
     * Gets the function call patterns.
     *
     * @return the patterns
     */
    public Map<String, String> getPatterns()
    {
        return _patterns;
    }
    
    /**
     * Calculates maximum.
     *
     * @param value the field value
     * @param currentValue the current value
     * @return the minimum
     */
    @PivotFunction(name = "MAX", pattern = "com.toolsverse.etl.core.task.common.PivotFunctions.instance().max(?,?current)")
    public Object max(Object value, Object currentValue)
    {
        if (currentValue == null)
            return value;
        
        if (value == null)
            return currentValue;
        
        int comp = 0;
        
        if (value instanceof Number || currentValue instanceof Number
                || Utils.isNumber(Utils.makeString(currentValue)))
        {
            comp = Utils.compareTo(object2Number(value),
                    object2Number(currentValue));
        }
        else
            comp = Utils.compareTo(value, currentValue);
        
        if (comp >= 0)
            return value;
        else
            return currentValue;
    }
    
    /**
     * Calculates minimum.
     *
     * @param value the field value
     * @param currentValue the current value
     * @return the minimum
     */
    @PivotFunction(name = "MIN", pattern = "com.toolsverse.etl.core.task.common.PivotFunctions.instance().min(?,?current)")
    public Object min(Object value, Object currentValue)
    {
        if (currentValue == null)
            return value;
        
        if (value == null)
            return currentValue;
        
        int comp = 0;
        
        if (value instanceof Number || currentValue instanceof Number
                || Utils.isNumber(Utils.makeString(currentValue)))
        {
            comp = Utils.compareTo(object2Number(value),
                    object2Number(currentValue));
        }
        else
            comp = Utils.compareTo(value, currentValue);
        
        if (comp >= 0)
            return currentValue;
        else
            return value;
    }
    
    /**
     * Object2 number.
     *
     * @param value the value
     * @return the double
     */
    private double object2Number(Object value)
    {
        if (value == null)
            return 0;
        
        if (value instanceof Number)
            return ((Number)value).doubleValue();
        
        return Utils.str2Number(value.toString(), 0).doubleValue();
    }
    
    /**
     * Calculates sum.
     *
     * @param value the field value
     * @param currentValue the current value
     * @return the sum
     */
    @PivotFunction(name = "SUM", pattern = "com.toolsverse.etl.core.task.common.PivotFunctions.instance().sum(?,?current)")
    public Number sum(Object value, Object currentValue)
    {
        double val = object2Number(value) + object2Number(currentValue);
        
        if (Utils.isInteger(val))
            return (int)val;
        else
            return val;
    }
    
}
