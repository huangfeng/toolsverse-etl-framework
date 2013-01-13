/*
 * AvgNumber.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

/**
 * This is an extension of the CalcNumber which calculates average. The sum and count stored internally. 
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public class AvgNumber extends CalcNumber
{
    
    /** The _sum. */
    private double _sum;
    
    /** The _count. */
    private int _count;
    
    /**
     * Instantiates a new avg number.
     *
     * @param value the value
     */
    public AvgNumber(double value)
    {
        super(value);
        
        _sum = value;
        
        _count = 1;
    }
    
    /**
     * Instantiates a new avg number.
     *
     * @param value the value
     */
    public AvgNumber(int value)
    {
        super(value);
        
        _sum = value;
        
        _count = 1;
    }
    
    /**
     * Instantiates a new avg number.
     *
     * @param value the value
     */
    public AvgNumber(String value)
    {
        super(value);
        
        _sum = Utils.str2Number(value, 0).doubleValue();
        
        _count = 1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.CalcNumber#calculate()
     */
    @Override
    public Number calculate()
    {
        Number val = _sum / _count;
        
        if (Utils.isInteger(val.doubleValue()))
            return val.longValue();
        else
            return val;
    }
    
    /**
     * Gets the count.
     *
     * @return the count
     */
    public int getCount()
    {
        return _count;
    }
    
    /**
     * Gets the sum.
     *
     * @return the sum
     */
    public double getSum()
    {
        return _sum;
    }
    
    /**
     * Sets the count.
     *
     * @param count the new count
     */
    public void setCount(int count)
    {
        _count = count <= 0 ? 1 : count;
        
        update();
    }
    
    /**
     * Sets the sum.
     *
     * @param sum the new sum
     */
    public void setSum(double sum)
    {
        _sum = sum;
        
        update();
    }
}
