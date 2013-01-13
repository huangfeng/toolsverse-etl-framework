/*
 * AvgNumberTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

/**
 * AvgNumberTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class AvgNumberTest
{
    
    @Test
    public void testCalculate()
    {
        AvgNumber number = new AvgNumber(123);
        
        number.setSum(100);
        number.setCount(5);
        
        assertTrue(20 == number.intValue());
    }
    
    @Test
    public void testCompareTo()
    {
        AvgNumber number = new AvgNumber(123);
        
        number.setSum(100);
        number.setCount(5);
        
        assertTrue(number.compareTo(20) == 0);
    }
    
    @Test
    public void testGetNumber()
    {
        AvgNumber number = new AvgNumber(123);
        
        assertTrue(123 == number.intValue());
        
        number = new AvgNumber(123.12);
        
        assertTrue(123.12 == number.doubleValue());
    }
    
}