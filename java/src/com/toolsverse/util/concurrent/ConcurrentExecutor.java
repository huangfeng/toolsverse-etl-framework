/*
 * ConcurrentExecutor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import com.toolsverse.exception.DefaultExceptionHandler;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * ConcurrentExecutor is a service class which coordinates all work related to the service thread creation, execution and termination. 
 * It also creates and configures ProgressDelegate using ConcurrentFeature. All methods of this class usually called from the ConcurrentAdapter.
 * 
 * @see com.toolsverse.util.concurrent.ProgressDelegate
 * @see java.util.concurrent.Future
 * @see com.toolsverse.util.concurrent.ConcurrentAdapter
 *  
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConcurrentExecutor implements ProgressDelegate
{
    
    /** The current ConcurrentAdapter. */
    private ConcurrentAdapter _adapter;
    
    /** The thread pool. */
    private ExecutorService _threadPool;
    
    /** The current Future object. */
    private Future<?> _future;
    
    /**
     * Instantiates a new ConcurrentExecutor using specific ConcurrentAdapter
     *
     * @param adapter the ConcurrentAdapter
     */
    public ConcurrentExecutor(ConcurrentAdapter adapter)
    {
        _adapter = adapter;
        _threadPool = null;
        _future = null;
    }
    
    /**
     * Interrupts the current service thread.
     */
    public void cancel()
    {
        if (_future != null && !_future.isDone() && !_future.isCancelled())
        {
            _future.cancel(true);
            
            _adapter.canceled();
        }
        
        shutdown(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.concurrent.ProgressDelegate#display(com.toolsverse
     * .util.concurrent.ConcurrentExecutor,
     * com.toolsverse.util.concurrent.ConcurrentFeature,
     * com.toolsverse.util.concurrent.ConcurrentAdapter)
     */
    public void display(ConcurrentExecutor executor,
            ConcurrentFeature concurrentFeature, ConcurrentAdapter adapter)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.concurrent.ProgressDelegate#done()
     */
    public void done()
    {
        shutdown(false);
        _adapter.done();
    }
    
    /**
     * Creates and starts a new service thread. Creates and configures ProgressDelegate using ConcurrentFeature.
     *
     * @param concurrentFeature the ConcurrentFeature
     */
    public void execute(ConcurrentFeature concurrentFeature)
    {
        _threadPool = Executors.newSingleThreadExecutor();
        
        ProgressDelegate delegate = null;
        
        if (concurrentFeature.isShowProgress()
                && concurrentFeature.getProgressDelegate() == null)
            try
            {
                delegate = (ProgressDelegate)ObjectFactory.instance().get(
                        ProgressDelegate.class.getName());
                
                concurrentFeature.setProgressDelegate(delegate);
            }
            catch (Exception ex)
            {
                DefaultExceptionHandler.instance().logException(Logger.SEVERE,
                        this,
                        ConcurrentResource.ERROR_CREATING_PROGRESS.getValue(),
                        ex);
            }
        
        delegate = concurrentFeature.getProgressDelegate();
        
        if (delegate == null)
        {
            delegate = this;
            concurrentFeature.setProgressDelegate(delegate);
        }
        
        delegate.display(this, concurrentFeature, _adapter);
        
        if (!concurrentFeature.isShowProgress() || delegate == this)
        {
            _future = _threadPool.submit((Callable<Object>)_adapter);
            
            try
            {
                _future.get();
                
                done();
            }
            catch (Exception ex)
            {
                onException(ex);
            }
        }
        else
        {
            _future = _threadPool.submit((Runnable)_adapter);
            
            delegate.start();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.util.concurrent.ProgressDelegate#onException(java.lang
     * .Throwable)
     */
    public void onException(Throwable ex)
    {
        shutdown(false);
        _adapter.onException(ex);
    }
    
    /**
     * Shutdowns thread pool. 
     *
     * @param now the now
     */
    public void shutdown(boolean now)
    {
        if (_threadPool != null)
        {
            if (now)
                _threadPool.shutdownNow();
            else
                _threadPool.shutdown();
            
            _threadPool = null;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.concurrent.ProgressDelegate#start()
     */
    public void start()
    {
        
    }
    
}
