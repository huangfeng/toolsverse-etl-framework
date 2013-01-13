/*
 * CalcNumber.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.math.BigDecimal;

/**
 * This is a abstract class which is base for calculated numbers, such as average, etc.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class CalcNumber extends Number implements Comparable<Number>
{
    
    /** The _value. */
    private BigDecimal _value;
    
    /**
     * Instantiates a new calc number.
     *
     * @param value the value
     */
    public CalcNumber(double value)
    {
        _value = new BigDecimal(value);
    }
    
    /**
     * Instantiates a new calc number.
     *
     * @param value the value
     */
    public CalcNumber(int value)
    {
        _value = new BigDecimal(value);
    }
    
    /**
     * Instantiates a new calc number.
     *
     * @param value the value
     */
    public CalcNumber(String value)
    {
        _value = new BigDecimal(value);
    }
    
    /**
     * Calculate.
     *
     * @return the number
     */
    public abstract Number calculate();
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    @Override
    public int compareTo(Number val)
    {
        if (val == null)
            return 1;
        
        return _value.compareTo(new BigDecimal(val.doubleValue()));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.math.BigDecimal#doubleValue()
     */
    @Override
    public double doubleValue()
    {
        Number val = calculate();
        
        if (val == null)
            return _value.doubleValue();
        else
            return val.doubleValue();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.math.BigDecimal#floatValue()
     */
    @Override
    public float floatValue()
    {
        Number val = calculate();
        
        if (val == null)
            return _value.floatValue();
        else
            return val.floatValue();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.math.BigDecimal#intValue()
     */
    @Override
    public int intValue()
    {
        Number val = calculate();
        
        if (val == null)
            return _value.intValue();
        else
            return val.intValue();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.math.BigDecimal#longValue()
     */
    @Override
    public long longValue()
    {
        Number val = calculate();
        
        if (val == null)
            return _value.longValue();
        else
            return val.longValue();
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.math.BigDecimal#toString()
     */
    @Override
    public String toString()
    {
        Number val = calculate();
        
        if (val == null)
            return super.toString();
        else
            return val.toString();
    }
    
    /**
     * Update.
     */
    public void update()
    {
        _value = new BigDecimal(calculate().doubleValue());
    }
    
}
