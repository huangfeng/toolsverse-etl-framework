/*
 * MockServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

/**
 * MockServiceImpl
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockServiceImpl implements MockService
{
    public String doSomething()
    {
        return MockServiceImpl.class.getName();
    }
}
