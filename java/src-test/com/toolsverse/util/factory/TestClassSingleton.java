/*
 * TestClassSingleton.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.factory;

/**
 * TestClassSingleton
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TestClassSingleton implements TestInterfaceSingleton
{
    @Override
    public String toString()
    {
        return getClass().getName();
    }
}
