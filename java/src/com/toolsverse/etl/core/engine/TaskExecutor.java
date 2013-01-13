/*
 * TaskExecutor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.DriverUnit;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.ext.loader.SharedUnitLoader;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * This class executes all tasks attached to the <code>Source</code> or
 * <code>Destination</code>.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TaskExecutor
{
    /**
     * Executes PRE ETL tasks for the <code>Block</code> if any exist.
     * <code>Block</code> can be either <code>Source</code> or
     * <code>Destination</code>. PRE ETL tasks usually run for the
     * <code>Source</code> after extract SQL is executed and meta data populated
     * but before actual extract starts.
     * 
     * @param config
     *            The config
     * @param block
     *            The block
     * @param scenario
     *            The scenario
     * 
     * @return the task result
     * 
     * @throws Exception
     *             in case of any error
     */
    public TaskResult executeBeforeEtlTasks(EtlConfig config, Block block,
            Scenario scenario)
        throws Exception
    {
        ListHashMap<String, Task> beforeEtlTasks = block.getBeforeEtlTasks();
        
        if (beforeEtlTasks == null)
            return null;
        
        TaskResult taskResult = new TaskResult();
        
        for (int i = 0; i < beforeEtlTasks.size(); i++)
        {
            Task task = beforeEtlTasks.get(i);
            
            OnTask onTask = task.getOnTask();
            
            if (onTask == null)
                continue;
            
            boolean wrongMode = task.getScope() != Task.AS_CONFIGURED
                    && !task.isScopeSet(Task.BEFORE_ETL);
            
            if (!wrongMode)
            {
                Logger.log(
                        Logger.INFO,
                        EtlLogger.class,
                        EtlResource.EXECUTING_BEFORE_ETL_TASK.getValue()
                                + task.getName());
                
                taskResult = onTask.executeBeforeEtlTask(config, task);
                
                if (taskResult == null
                        || taskResult.getResult() == TaskResult.TaskResultCode.CONTINUE)
                    continue;
                else if (taskResult.getResult() == TaskResult.TaskResultCode.HALT)
                {
                    throw new Exception(
                            !Utils.isNothing(taskResult.getError()) ? taskResult
                                    .getError()
                                    : EtlResource.ERROR_EXECUTING_TASK
                                            .getValue());
                }
                else
                    return taskResult;
            }
        }
        
        return taskResult;
    }
    
    /**
     * Executes INLINE tasks for the <code>Block</code> if any exist.
     * <code>Block</code> can be either <code>Source</code> or
     * <code>Destination</code>. INLINE tasks run for each record of the data
     * set.
     * 
     * @param config
     *            The config
     * @param block
     *            The block
     * @param scenario
     *            The scenario
     * @param index
     *            The index of the record
     * 
     * @return the task result
     * 
     * @throws Exception
     *             in case of any error
     */
    public TaskResult executeInlineTasks(EtlConfig config, Block block,
            Scenario scenario, long index)
        throws Exception
    {
        ListHashMap<String, Task> inlineTasks = block.getInlineTasks();
        
        if (inlineTasks == null)
            return null;
        
        TaskResult taskResult = new TaskResult();
        
        for (int i = 0; i < inlineTasks.size(); i++)
        {
            Task task = inlineTasks.get(i);
            
            OnTask onTask = task.getOnTask();
            
            if (onTask == null)
                continue;
            
            boolean wrongMode = task.getScope() != Task.AS_CONFIGURED
                    && !task.isScopeSet(Task.INLINE);
            
            if (!wrongMode)
            {
                if (config.getLogStep() > 0
                        && (index % config.getLogStep()) == 0)
                    Logger.log(
                            Logger.INFO,
                            EtlLogger.class,
                            EtlResource.EXECUTING_INLINE_TASK.getValue()
                                    + task.getName() + " for row " + index);
                
                taskResult = onTask.executeInlineTask(config, task, index);
                
                if (taskResult == null
                        || taskResult.getResult() == TaskResult.TaskResultCode.CONTINUE)
                    continue;
                else if (taskResult.getResult() == TaskResult.TaskResultCode.HALT)
                {
                    throw new Exception(
                            !Utils.isNothing(taskResult.getError()) ? taskResult
                                    .getError()
                                    : EtlResource.ERROR_EXECUTING_TASK
                                            .getValue());
                }
                else
                    return taskResult;
            }
        }
        
        return taskResult;
    }
    
    /**
     * Executes POST tasks for the <code>Block</code> if any exist.
     * <code>Block</code> can be either <code>Source</code> or
     * <code>Destination</code>. POST tasks run after everything associated with
     * the block said and done. Example: after extract or after load.
     * 
     * @param config
     *            The config
     * @param block
     *            The block
     * @param scenario
     *            The scenario
     * @param dataSet
     *            the data set
     * @return the task result
     * @throws Exception
     *             in case of any error
     */
    public TaskResult executePostTasks(EtlConfig config, Block block,
            Scenario scenario, DataSet dataSet)
        throws Exception
    {
        ListHashMap<String, Task> postTasks = block.getPostTasks();
        
        if (postTasks == null)
            return new TaskResult(dataSet);
        
        for (int i = 0; i < postTasks.size(); i++)
        {
            Task task = postTasks.get(i);
            
            OnTask onTask = task.getOnTask();
            
            if (onTask == null)
                continue;
            
            boolean wrongMode = task.getScope() != Task.AS_CONFIGURED
                    && !task.isScopeSet(Task.POST);
            
            if (!wrongMode)
            {
                Logger.log(
                        Logger.INFO,
                        EtlLogger.class,
                        EtlResource.EXECUTING_POST_TASK.getValue()
                                + task.getName());
                
                TaskResult taskResult = onTask.executePostTask(config, task,
                        dataSet);
                
                if (taskResult == null
                        || taskResult.getResult() == TaskResult.TaskResultCode.CONTINUE)
                {
                    dataSet = taskResult != null
                            && taskResult.getDataSet() != null ? taskResult
                            .getDataSet() : dataSet;
                    
                    continue;
                }
                else if (taskResult.getResult() == TaskResult.TaskResultCode.HALT)
                {
                    throw new Exception(
                            !Utils.isNothing(taskResult.getError()) ? taskResult
                                    .getError()
                                    : EtlResource.ERROR_EXECUTING_TASK
                                            .getValue());
                }
                else
                    return taskResult;
                
            }
        }
        
        return new TaskResult(dataSet);
    }
    
    /**
     * Executes PRE tasks for the <code>Block</code> if any exist.
     * <code>Block</code> can be either <code>Source</code> or
     * <code>Destination</code>. PRE tasks run just before any actions
     * associated with the block are taken. Example: before extract or before
     * load.
     * 
     * @param config
     *            The config
     * @param block
     *            The block
     * @param scenario
     *            The scenario
     * @param dataSet
     *            the data set
     * @return the task result
     * @throws Exception
     *             in case of any error
     */
    public TaskResult executePreTasks(EtlConfig config, Block block,
            Scenario scenario, DataSet dataSet)
        throws Exception
    {
        ListHashMap<String, Task> tasks = block.getTasks();
        
        if (tasks == null || tasks.size() == 0)
            return null;
        
        TaskResult taskResult = new TaskResult();
        
        EtlFactory etlFactory = new EtlFactory();
        
        Driver driver = null;
        ListHashMap<String, Variable> variables = scenario.getVariables();
        
        for (int i = 0; i < tasks.size(); i++)
        {
            Task task = tasks.get(i);
            
            OnTask onTask = getOnTask(task, config, block, scenario, dataSet,
                    etlFactory, driver, variables);
            
            if (onTask == null)
                continue;
            
            boolean wrongMode = task.getScope() != Task.AS_CONFIGURED
                    && !task.isScopeSet(Task.PRE);
            
            onTask.init(config, task);
            
            if (!wrongMode && (onTask.isPreTask() || task.isScopeSet(Task.PRE)))
            {
                Logger.log(
                        Logger.INFO,
                        EtlLogger.class,
                        EtlResource.EXECUTING_PRE_TASK.getValue()
                                + task.getName());
                
                taskResult = onTask.executePreTask(config, task);
                
                if (taskResult == null
                        || taskResult.getResult() == TaskResult.TaskResultCode.CONTINUE)
                    continue;
                else if (taskResult.getResult() == TaskResult.TaskResultCode.HALT)
                {
                    throw new Exception(
                            !Utils.isNothing(taskResult.getError()) ? taskResult
                                    .getError()
                                    : EtlResource.ERROR_EXECUTING_TASK
                                            .getValue());
                }
                else
                    return taskResult;
                
            }
        }
        
        return taskResult;
    }
    
    /**
     * Prepares task for execution.
     * 
     * @param task
     *            the task
     * @param config
     *            the etl config
     * @param block
     *            the block
     * @param scenario
     *            the scenario
     * @param dataSet
     *            the data set
     * @param etlFactory
     *            the etl factory
     * @param driver
     *            the driver
     * @param variables
     *            the variables
     * @return the instance of the the OnTask interface. Actual task
     *         implementation.
     * @throws Exception
     *             in case of any error
     */
    private OnTask getOnTask(Task task, EtlConfig config, Block block,
            Scenario scenario, DataSet dataSet, EtlFactory etlFactory,
            Driver driver, ListHashMap<String, Variable> variables)
        throws Exception
    {
        OnTask onTask = (OnTask)ObjectFactory.instance().get(
                task.getClassName(), true);
        
        if (onTask == null)
            return null;
        
        task.setConnection(config.getConnectionFactory().getConnection(
                task.getConnectionName()));
        if (task.getConnection() == null)
            task.setConnection(block.getConnection());
        if (task.getConnection() == null)
            task.setConnection(config.getConnectionFactory().getConnection(
                    block.getConnectionName()));
        if (task.getConnection() == null)
            task.setConnection(dataSet.getConnection());
        if (task.getConnection() == null && block instanceof Source)
            task.setConnection(config.getConnectionFactory().getConnection(
                    EtlConfig.SOURCE_CONNECTION_NAME));
        if (task.getConnection() == null && block instanceof Destination)
            task.setConnection(config.getConnectionFactory().getConnection(
                    EtlConfig.DEST_CONNECTION_NAME));
        
        if (Utils.isNothing(task.getDriverClassName()))
            driver = dataSet.getDriver();
        else
        {
            if (task.getConnection() != null)
            {
                ConnectionParams connectionParams = config
                        .getConnectionFactory().getConnectionParams(
                                task.getConnection());
                
                DriverUnit driverUnit = (DriverUnit)SharedUnitLoader.instance()
                        .getUnit(Driver.class);
                
                driver = etlFactory.getDriver(
                        task.getDriverClassName(),
                        driverUnit,
                        connectionParams != null ? connectionParams
                                .getUniqueProperty() : null);
            }
            else
                driver = etlFactory.getDriver(task.getDriverClassName(), null,
                        null);
            
            if (driver == null)
                driver = dataSet.getDriver();
        }
        task.setDriver(driver);
        if (task.getDriver() == null)
            task.setDriver(dataSet.getDriver());
        
        ListHashMap<String, Variable> vars = new ListHashMap<String, Variable>();
        
        ListHashMap<String, Variable> oldvars = task.getVariables();
        
        task.setVariables(vars);
        
        task.addVariables(variables);
        
        task.addVariables(oldvars);
        
        EtlUtils.substituteVars(task.getVariables());
        
        task.setDataSet(dataSet);
        
        task.setOnTask(onTask);
        
        task.setScenario(scenario);
        
        return onTask;
    }
    
}
