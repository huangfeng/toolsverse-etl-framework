/*
 * ProgressDelegate.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import java.util.ArrayList;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;

import com.toolsverse.util.TypedKeyValue;

/**
 * The purpose of this class is to coordinate execution of the multiple tasks in the separate threads. 
 * Threads are automatically created, added to the pool and started. Class provides methods to add new tasks, 
 * wait until all tasks finished and terminate execution if needed. By default it uses fixed size thread pool from 
 * the java.util.concurrent package. 
 * 
 * <p>
 * Typical usage example:
 * <p><pre class="brush: java">
 * ParallelExecutor executor = new ParallelExecutor(maxNumberOfThreads);
 * 
 * for (Callable task : tasks)
 * {
 *      executor.addTask(task);
 * }
 * 
 * try
 * {
 *     executor.waitUntilDone();
 * }
 * catch (Exception ex)
 * {
 *     Logger.log(Logger.SEVERE, executor,
 *                          Resource.ERROR_GENERAL.getValue(), ex);
 *                  
 * }
 *              
 * executor.terminate();
 * </pre> 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ParallelExecutor
{
    
    /** The thread pool. */
    private ExecutorService _threadPool;
    
    /** If true execution was terminated. */
    private volatile boolean _isTerminated;
    
    /** Results for each task. */
    private List<TypedKeyValue<Future<?>, Boolean>> _futures;
    
    /**
     * Instantiates a new parallel executor.
     *
     * @param threadPool the thread pool
     */
    public ParallelExecutor(ExecutorService threadPool)
    {
        _futures = new Vector<TypedKeyValue<Future<?>, Boolean>>();
        
        _isTerminated = false;
        
        _threadPool = threadPool;
    }
    
    /**
     * Instantiates a new ParallelExecutor using nThreads as a max possible number of threads.
     *
     * @param nThreads the max possible number of threads
     */
    public ParallelExecutor(int nThreads)
    {
        _futures = new Vector<TypedKeyValue<Future<?>, Boolean>>();
        
        _isTerminated = false;
        
        _threadPool = Executors.newFixedThreadPool(nThreads);
    }
    
    /**
     * Adds the task to the pool and schedule it execution. Task is a class which implements Callable interface.
     *
     * @param task the task
     * @return the future or null if thread pool was terminated
     */
    public Future<?> addTask(Callable<?> task)
    {
        return addTask(task, false);
    }
    
    /**
     * Adds the task to the pool and schedule it execution. Task is a class which implements Callable interface.
     * If <code>ignoreException == true</code> any exception during execution of the task
     * will be ignored.
     *
     * @param task the task
     * @param ignoreException the ignore exception flag
     * @return the future or null if thread pool was terminated
     */
    public Future<?> addTask(Callable<?> task, boolean ignoreException)
    {
        if (!_isTerminated)
        {
            Future<?> future = null;
            
            try
            {
                future = _threadPool.submit(task);
            }
            catch (RejectedExecutionException ex)
            {
                _isTerminated = true;
                
                return null;
            }
            
            _futures.add(new TypedKeyValue<Future<?>, Boolean>(future,
                    ignoreException));
            
            return future;
        }
        
        return null;
    }
    
    /**
     * Gets the results for each task.
     *
     * @return the results
     */
    public List<Future<?>> getResults()
    {
        List<Future<?>> list = new ArrayList<Future<?>>(_futures.size());
        
        for (TypedKeyValue<Future<?>, Boolean> keyValue : _futures)
            list.add(keyValue.getKey());
        
        return list;
    }
    
    /**
     * Checks if any of the tasks are still running.
     *
     * @return true, if any of the tasks are still running
     * @throws Exception in case of any error
     */
    private boolean isRunning()
        throws Exception
    {
        for (TypedKeyValue<Future<?>, Boolean> keyValue : _futures)
        {
            Future<?> future = keyValue.getKey();
            boolean ignoreException = keyValue.getValue();
            boolean isCancelled = future.isCancelled();
            boolean isDone = future.isDone();
            
            if ((isDone || isCancelled) && !ignoreException)
                future.get();
            
            if (!isDone && !isCancelled)
            {
                return true;
            }
        }
        
        return false;
    }
    
    /**
     * Checks if execution was terminated.
     *
     * @return true, if it is terminated
     */
    public boolean isTerminated()
    {
        return _isTerminated;
    }
    
    /**
     * Terminates execution of all active tasks. When execution is terminated all tasks must gracefully shutdown itself. 
     */
    public void terminate()
    {
        _isTerminated = true;
        
        _threadPool.shutdownNow();
    }
    
    /**
     * Waits until all tasks are finished or execution is terminated.
     *
     * @throws Exception in case of any error
     */
    public void waitUntilDone()
        throws Exception
    {
        try
        {
            while (!_isTerminated && isRunning())
                Thread.sleep(100);
        }
        catch (Exception ex)
        {
            _isTerminated = true;
            throw ex;
        }
    }
}
