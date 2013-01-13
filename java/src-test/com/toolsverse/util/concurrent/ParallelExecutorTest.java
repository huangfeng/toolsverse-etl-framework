/*
 * ParallelExecutorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import org.junit.Test;

/**
 * ParallelExecutorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class ParallelExecutorTest
{
    private class HappyTask implements Callable<String>
    {
        String _value;
        
        public HappyTask(String value)
        {
            _value = value;
        }
        
        public String call()
            throws Exception
        {
            Thread.sleep(100);
            
            return _value;
        }
    }
    
    private class UnHappyTask implements Callable<String>
    {
        String _value;
        
        public UnHappyTask(String value)
        {
            _value = value;
        }
        
        public String call()
            throws Exception
        {
            Thread.sleep(100);
            
            throw new Exception("Runtime exception. Value=" + _value);
        }
    }
    
    @Test
    public void testException()
        throws Exception
    {
        int nTask = 10;
        int nHappyTask = 5;
        
        ParallelExecutor executor = new ParallelExecutor(nTask);
        Exception ex = null;
        
        Callable<String> task;
        
        for (int i = 0; i < nTask; i++)
        {
            if (i < nHappyTask)
                task = new HappyTask(String.valueOf(i));
            else
                task = new UnHappyTask(String.valueOf(i));
            
            executor.addTask(task);
        }
        
        try
        {
            executor.waitUntilDone();
        }
        catch (Exception e)
        {
            ex = e;
        }
        finally
        {
            executor.terminate();
        }
        
        assertNotNull(ex);
        
        assertTrue(ex.getMessage().indexOf(
                ("Runtime exception. Value=" + nHappyTask)) > 0);
    }
    
    private void testHappyPath(int nThreads, int nTask)
        throws Exception
    {
        ParallelExecutor executor = new ParallelExecutor(nThreads);
        Exception ex = null;
        
        for (int i = 0; i < nTask; i++)
        {
            HappyTask task = new HappyTask(String.valueOf(i));
            
            executor.addTask(task);
        }
        
        try
        {
            executor.waitUntilDone();
        }
        catch (Exception e)
        {
            ex = e;
        }
        finally
        {
            executor.terminate();
        }
        
        assertNull(ex);
        
        assertNotNull(executor.getResults());
        
        assertTrue(executor.getResults().size() == nTask);
        
        for (int i = 0; i < executor.getResults().size(); i++)
        {
            Future<?> future = executor.getResults().get(i);
            
            assertTrue(String.valueOf(i).equals(future.get()));
        }
    }
    
    @Test
    public void testHappyPathNumberOfThsEqualsNumberOfTasks()
        throws Exception
    {
        testHappyPath(5, 5);
    }
    
    @Test
    public void testHappyPathNumberOfThsLessNumberOfTasks()
        throws Exception
    {
        testHappyPath(5, 10);
    }
    
    @Test
    public void testIgnoreException()
        throws Exception
    {
        int nTask = 10;
        int nHappyTask = 5;
        
        ParallelExecutor executor = new ParallelExecutor(nTask);
        Exception ex = null;
        
        Callable<String> task;
        
        for (int i = 0; i < nTask; i++)
        {
            if (i < nHappyTask)
                task = new HappyTask(String.valueOf(i));
            else
                task = new UnHappyTask(String.valueOf(i));
            
            executor.addTask(task, true);
        }
        
        try
        {
            executor.waitUntilDone();
        }
        catch (Exception e)
        {
            ex = e;
        }
        finally
        {
            executor.terminate();
        }
        
        assertNull(ex);
        
        assertNotNull(executor.getResults());
        
        assertTrue(executor.getResults().size() == nTask);
        
        for (int i = 0; i < nHappyTask; i++)
        {
            Future<?> future = executor.getResults().get(i);
            
            assertTrue(String.valueOf(i).equals(future.get()));
        }
    }
    
}
