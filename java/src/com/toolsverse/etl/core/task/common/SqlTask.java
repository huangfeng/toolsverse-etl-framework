/*
 * SqlTask.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.util.HashMap;
import java.util.Map;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;

/**
 * This is {@link Task#PRE} and {@link Task#POST} task which executes specified sql.
 *
 * @see com.toolsverse.etl.core.engine.Task
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class SqlTask implements OnTask
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeBeforeEtlTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executeBeforeEtlTask(EtlConfig config, Task action)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeInlineTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task, long)
     */
    public TaskResult executeInlineTask(EtlConfig config, Task task, long index)
        throws Exception
    {
        DataSet dataSet = task.getDataSet();
        
        if (dataSet == null || dataSet.getRecordCount() <= 0)
            return new TaskResult();
        
        boolean clear = Utils.str2Boolean(task.getVariable(EtlConfig.CLEAR_VAR)
                .getValue(), true);
        
        DataSet newDataSet = new DataSet();
        
        newDataSet.setEncode(dataSet.isEncode());
        newDataSet.setFields(dataSet.getFields());
        newDataSet.setName(dataSet.getName());
        
        if (!clear)
        {
            DataSetData data = new DataSetData();
            DataSetRecord record = dataSet
                    .getRecord(dataSet.getRecordCount() - 1);
            data.add(record);
            newDataSet.setData(data);
        }
        else
            newDataSet.setData(dataSet.getData());
        
        if (!Utils.isNothing(task.getCode()))
        {
            String sql = null;
            Map<String, Object> bindVariables = new HashMap<String, Object>();
            
            if (task.getVariables() != null && task.getVariables().size() > 0)
                sql = EtlUtils.mergeSqlWithVars(config, task.getCode(),
                        task.getUsing(), task.getVariables(), bindVariables);
            else
                sql = task.getCode();
            
            SqlUtils.executeScript(task.getConnection(), sql, task.getDriver(),
                    newDataSet, task, bindVariables, task.getUsing(),
                    task.commitWhenDone());
            
            if (clear)
                dataSet.getData().clear();
            
        }
        
        return new TaskResult();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePostTask(com.toolsverse.
     * etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task,
     * com.toolsverse.etl.common.DataSet)
     */
    public TaskResult executePostTask(EtlConfig config, Task task,
            DataSet dataSet)
        
        throws Exception
    {
        Map<String, Object> bindVariables = new HashMap<String, Object>();
        
        String sql = null;
        
        if (!Utils.isNothing(task.getCode()))
        {
            if (task.getVariables() != null && task.getVariables().size() > 0)
                sql = EtlUtils.mergeSqlWithVars(config, task.getCode(),
                        task.getUsing(), task.getVariables(), bindVariables);
            else
                sql = task.getCode();
            
            SqlUtils.executeScript(task.getConnection(), sql, task.getDriver(),
                    dataSet, task, bindVariables, task.getUsing(),
                    task.commitWhenDone());
        }
        
        return new TaskResult(dataSet);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executePreTask(com.toolsverse.etl
     * .core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executePreTask(EtlConfig config, Task task)
        throws Exception
    {
        return executePostTask(config, task, task.getDataSet());
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#init(com.toolsverse.etl.core.config
     * .EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public void init(EtlConfig config, Task task)
        throws Exception
    {
        ListHashMap<String, Variable> vars = task.getVariables();
        if (vars == null)
        {
            vars = new ListHashMap<String, Variable>();
            task.setVariables(vars);
        }
        
        EtlUtils.addVar(task.getVariables(), EtlConfig.CLEAR_VAR, "true", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.DATA_FOLDER_VAR,
                FileUtils.getUnixFolderName(SystemConfig.instance()
                        .getDataFolderName()), false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isInlineTask()
     */
    public boolean isInlineTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPostTask()
     */
    public boolean isPostTask()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreEtlTask()
     */
    public boolean isPreEtlTask()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isPreTask()
     */
    public boolean isPreTask()
    {
        return false;
    }
}
