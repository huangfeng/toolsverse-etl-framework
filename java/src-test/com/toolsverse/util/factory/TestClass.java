/*
 * TestClass.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.factory;

/**
 * TestClass
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TestClass implements TestInterface
{
    @Override
    public String toString()
    {
        return getClass().getName();
    }
}
