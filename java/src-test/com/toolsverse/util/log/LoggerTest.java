/*
 * LoggerTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.log;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TestAppender;
import com.toolsverse.util.Utils;

/**
 * LoggerTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class LoggerTest
{
    private static TestAppender _testAppender = new TestAppender();
    
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
        
        ((Log4jWriter)Logger.getLogger()).getLogger(LoggerTest.class)
                .addAppender(_testAppender);
    }
    
    @AfterClass
    public static void tearDown()
    {
        ((Log4jWriter)Logger.getLogger()).getLogger(LoggerTest.class)
                .removeAppender(_testAppender);
    }
    
    @Test
    public void testError()
    {
        _testAppender.clear();
        
        String message = "Test Fatal Error Message";
        
        String infoMessage = "Test Info Message";
        
        Logger.getLogger().setLevel(LoggerTest.class, Logger.FATAL);
        
        Logger.log(Logger.FATAL, this, message);
        
        assertTrue(_testAppender.containsMessage(message));
        
        Logger.log(Logger.INFO, this, infoMessage);
        
        assertTrue(!_testAppender.containsMessage(infoMessage));
    }
    
    @Test
    public void testInfo()
    {
        _testAppender.clear();
        
        Logger.getLogger().setLevel(LoggerTest.class, Logger.INFO);
        
        String message = "Test Info Message";
        
        Logger.log(Logger.INFO, this, message);
        
        assertTrue(_testAppender.containsMessage(message));
    }
    
}
