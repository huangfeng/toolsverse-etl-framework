/*
 * MockServiceServer.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.service;

/**
 * MockServiceServer
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockServiceServer implements MockService
{
    public String doSomething()
    {
        return MockServiceServer.class.getName();
    }
}
