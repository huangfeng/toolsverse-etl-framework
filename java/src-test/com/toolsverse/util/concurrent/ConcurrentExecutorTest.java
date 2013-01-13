/*
 * ConcurrentExecutorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * ConcurrentExecutorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ConcurrentExecutorTest
{
    class TestAdapter extends ConcurrentAdapter
    {
        boolean _happyPath;
        boolean _done;
        Throwable _exception;
        
        public TestAdapter(boolean happyPath)
        {
            _happyPath = happyPath;
            _done = false;
            _exception = null;
        }
        
        @Override
        public void canceled()
        {
        }
        
        @Override
        public void done()
        {
            _done = true;
            
        }
        
        @Override
        public void execute()
            throws Exception
        {
            if (!_happyPath)
                throw new Exception("Expected exception");
        }
        
        @Override
        public void onException(Throwable ex)
        {
            _exception = ex;
        }
        
    }
    
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
    }
    
    @Test
    public void testException()
        throws Exception
    {
        TestAdapter adapter = new TestAdapter(false);
        
        adapter.executeAndWait();
        
        assertTrue(!adapter._done);
        assertNotNull(adapter._exception);
        assertTrue(adapter._exception.getMessage()
                .indexOf("Expected exception") >= 0);
    }
    
    @Test
    public void testHappyPath()
        throws Exception
    {
        TestAdapter adapter = new TestAdapter(true);
        
        adapter.executeAndWait();
        
        assertTrue(adapter._done);
        assertNull(adapter._exception);
    }
}