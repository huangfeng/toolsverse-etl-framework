/*
 * ConcurrentAdapter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import java.util.concurrent.Callable;

/**
 * In the typical client program it is a good practice to separate UI thread and service execution thread. For example when user clicks on "Save" button
 * the code which saves data into the database is executed. Until it's done the UI "freezes" unless UI thread is separated from the "save" thread. The purpose of this
 * class is to do just that. The particular implementation's of this class must implement abstract methods such as execute(), done(), etc.  
 * 
 * <p>
 * Do: separate UI update code and service execution code
 * <p>
 * Do not: put UI code in the service execution thread and vice versa
 * 
 * <p>
 * In the example below the particular implementation of the ConcurrentAdapter is executed when user clicks on Test Connection button:
 * <p><pre class="brush: java">
 * public void testConnection()
 * {
 *      ConcurrentFeature concurrentFeature = new ConcurrentFeature(true);
 *      concurrentFeature.setMessage(TestConnectionResource.TESTING_CONNECTION
 *              .getValue());
 *      
 *      ConcurrentAdapter concurrentAdapter = new ConcurrentAdapter()
 *      {
 *          String status = "";
 *          
 *          public void canceled()
 *          {
 *          }
 *          
 *          public void done()
 *          {
 *              getField().setText(status);
 *              
 *              if (TestConnectionResource.CONNECTION_IS_OK.getValue()
 *                      .equalsIgnoreCase(status))
 *                  getField().setForeground(Color.BLACK);
 *              else
 *                  getField().setForeground(Color.RED);
 *          }
 *          
 *          public void execute()
 *              throws Exception
 *          {
 *              TestService testService = (TestService)ServiceFactory
 *                      .getService(TestService.class);
 *              
 *              status = testService.testConnection(_connectionProvider,
 *                      _connectionParamsProvider.getConnectionParams());
 *          }
 *          
 *          public void onException(Throwable ex)
 *          {
 *              DefaultExceptionHandler.instance().handleException(
 *                      Logger.SEVERE,
 *                      this,
 *                      TestConnectionResource.ERROR_TESTING_CONNECTION
 *                              .getValue(), ex);
 *          }
 *          
 *      };
 *      
 *      concurrentAdapter.executeAndWait(concurrentFeature);
 * }
 * </pre>
 * 
 * @see com.toolsverse.util.concurrent.ConcurrentFeature
 * 
 * @author Maksym Sherbinin
 * @version 2.0 
 * @since 2.0
 */

public abstract class ConcurrentAdapter implements Runnable, Callable<Object>
{
    
    /** The concurrent feature. */
    private ConcurrentFeature _concurrentFeature;
    
    /*
     * (non-Javadoc)
     * 
     * @see java.util.concurrent.Callable#call()
     */
    public final Object call()
        throws Exception
    {
        execute();
        
        if (Thread.currentThread().isInterrupted())
            return null;
        
        return null;
    }
    
    /**
     * Executed when service thread is interrupted before it finished. Put clean up code here.  
     */
    public abstract void canceled();
    
    /**
     * Executed when service thread is finished. Put UI update code here.
     */
    public abstract void done();
    
    /**
     * Executes service thread. Put main execution code here. 
     *
     * @throws Exception in case of any error
     */
    public abstract void execute()
        throws Exception;
    
    /**
     * Executes and waits until service thread is finished or interrupted and UI is updated. Uses default ConcurrentFeature.
     */
    public void executeAndWait()
    {
        executeAndWait(new ConcurrentFeature());
    }
    
    /**
     * Executes and waits until service thread is finished or interrupted and UI is updated. Uses particular ConcurrentFeature.
     *
     * @param concurrentFeature the ConcurrentFeature
     */
    public void executeAndWait(ConcurrentFeature concurrentFeature)
    {
        _concurrentFeature = concurrentFeature;
        
        ConcurrentExecutor executor = new ConcurrentExecutor(this);
        
        executor.execute(concurrentFeature);
    }
    
    /**
     * Executed if service thread throws an exception. Put error handling code here.
     *
     * @param ex the ex
     */
    public abstract void onException(Throwable ex);
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Runnable#run()
     */
    public final void run()
    {
        try
        {
            execute();
            
            if (Thread.currentThread().isInterrupted())
                return;
            
            _concurrentFeature.getProgressDelegate().done();
        }
        catch (Throwable ex)
        {
            _concurrentFeature.getProgressDelegate().onException(ex);
        }
    }
    
}
