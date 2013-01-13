/*
 * sasasa * RemoveDuplicates.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.toolsverse.etl.common.CommonEtlUtils;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;

/**
 * This {@link Task#INLINE} task removes duplicated records using given key. Key can be compound. You can combine multiple
 * RemoveDuplicates tasks.
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class RemoveDuplicates implements OnTask
{
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.core.engine.OnTask#executeBeforeEtlTask(com.toolsverse
     * .etl.core.config.EtlConfig, com.toolsverse.etl.core.engine.Task)
     */
    public TaskResult executeBeforeEtlTask(EtlConfig config, Task task)
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
            return null;
        
        boolean thisRow = Utils.str2Boolean(
                task.getVariable(EtlConfig.THIS_ROW_VAR).getValue(), false);
        
        DataSetRecord record = dataSet.getRecord(thisRow ? (int)index
                : (dataSet.getRecordCount() - 1));
        
        Variable keyVar = task.getVariable(EtlConfig.KEYS_VAR);
        
        @SuppressWarnings("unchecked")
        Map<String, FieldDef> keys = (Map<String, FieldDef>)keyVar.getObject();
        
        if (keys == null)
        {
            keys = CommonEtlUtils.getKeyFields(
                    task.getVariable(EtlConfig.KEYS_VAR).getValue(),
                    dataSet.getFields());
            
            keyVar.setObject(keys);
        }
        
        boolean ignoreCase = Utils.str2Boolean(
                task.getVariable(EtlConfig.IGNORE_CASE_VAR).getValue(), true);
        boolean doTrim = Utils.str2Boolean(task.getVariable(EtlConfig.TRIM_VAR)
                .getValue(), true);
        
        @SuppressWarnings("unchecked")
        Set<String> storedKyes = (Set<String>)task.getValue("storedKyes");
        
        if (storedKyes == null)
        {
            storedKyes = new HashSet<String>();
            
            task.setValue("storedKyes", storedKyes);
        }
        
        String key = CommonEtlUtils.getKey(dataSet, record, keys, ignoreCase,
                doTrim);
        
        if (Utils.isNothing(key))
        {
            return TaskResult.CONTINUE;
        }
        
        if (storedKyes.contains(key))
            return TaskResult.REJECT;
        
        storedKyes.add(key);
        
        return TaskResult.CONTINUE;
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
        @SuppressWarnings("unchecked")
        Set<String> storedKyes = (Set<String>)task.getValue("storedKyes");
        
        if (storedKyes != null)
            storedKyes.clear();
        
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
        return new TaskResult();
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
        
        EtlUtils.addVar(task.getVariables(), EtlConfig.KEYS_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.IGNORE_CASE_VAR, "true",
                false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.TRIM_VAR, "true", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.THIS_ROW_VAR, "false",
                false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.core.engine.OnTask#isInlineTask()
     */
    public boolean isInlineTask()
    {
        return true;
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
