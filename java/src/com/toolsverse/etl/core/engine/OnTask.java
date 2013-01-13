/*
 * OnTask.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.core.config.EtlConfig;

/**
 * The etl building blocks, such as Source and Destination can have one or multiple tasks attached. Tasks can be executed before, after and during 
 * extract and load.
 * For example there can be a task configured to execute after extract which copies data files from the local machine to the remove ftp server. 
 * The actual tasks must implement <code>OnTask</code> interface.    
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public interface OnTask
{
    
    /**
     * Executes <code>task</code> before etl process is started.
     *
     * @param config the etl config
     * @param task the task to execute
     * @return the task result
     * @throws Exception in case of any error
     */
    TaskResult executeBeforeEtlTask(EtlConfig config, Task task)
        throws Exception;
    
    /**
     * Executes <code>task</code> for each row of the data set.
     *
     * @param config the etl config
     * @param task the task to execute
     * @param index the current row number
     * @return the task result
     * @throws Exception in case of any error
     */
    TaskResult executeInlineTask(EtlConfig config, Task task, long index)
        throws Exception;
    
    /**
     * Executes <code>task</code> after extract or load finished.
     *
     * @param config the etl config
     * @param task the task to execute
     * @param dataSet the data set
     * @return the task result
     * @throws Exception in case of any error
     */
    TaskResult executePostTask(EtlConfig config, Task task, DataSet dataSet)
        throws Exception;
    
    /**
     * Executes <code>task</code> before extract or load started. 
     *
     * @param config the etl config
     * @param task the task to execute
     * @return the task result
     * @throws Exception in case of any error
     */
    TaskResult executePreTask(EtlConfig config, Task task)
        throws Exception;
    
    /**
     * Initializes task.
     *
     * @param config the etl config
     * @param task the task to initialize
     * @throws Exception in case of any error
     */
    void init(EtlConfig config, Task task)
        throws Exception;
    
    /**
     * Checks if this is inline task. The inline task is executed for each row of the data set.
     *
     * @return true, if it is inline task
     */
    boolean isInlineTask();
    
    /**
     * Checks if it is a post task. The post task executed after extract or load finished.
     *
     * @return true, if it is a post task
     */
    boolean isPostTask();
    
    /**
     * Checks if it is a pre etl task. The pre etl task executed before etl process has started. 
     *
     * @return true, if it is a pre etl task
     */
    boolean isPreEtlTask();
    
    /**
     * Checks if it is a pre task. The pre task executed before extract or load started. 
     *
     * @return true, if it is a pre task
     */
    boolean isPreTask();
    
}
