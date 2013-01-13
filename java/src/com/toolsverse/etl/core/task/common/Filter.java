/*
 * Filter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.util.LinkedHashMap;
import java.util.TreeMap;

import javax.script.Bindings;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.util.Script;
import com.toolsverse.util.Utils;

/**
 * This {@link Task#INLINE} task filters data set records using SQL like expression.
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Filter implements OnTask
{
    public static final String CONDITION_VAR = "CONDITION";
    
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
        
        @SuppressWarnings("unchecked")
        LinkedHashMap<String, String> condVars = (LinkedHashMap<String, String>)task
                .getValue(task.getName() + "condvars");
        
        if (condVars == null)
            return TaskResult.CONTINUE;
        
        Script script = (Script)task.getValue(task.getName() + "script");
        
        Bindings bindings = null;
        
        if (condVars.size() > 0)
        {
            bindings = script.getBindings(task, "JavaScript");
            
            @SuppressWarnings("unchecked")
            TreeMap<String, FieldDef> fields = (TreeMap<String, FieldDef>)task
                    .getValue(task.getName() + "fields");
            
            if (fields == null)
            {
                fields = dataSet.getNonCaseSensitiveFields();
                
                task.setValue(task.getName() + "fields", fields);
            }
            
            for (String name : condVars.keySet())
            {
                FieldDef field = fields.get(name);
                
                if (field != null)
                {
                    int col = dataSet.getFieldIndex(field.getName());
                    
                    bindings.put(name.replaceAll(" ", "_"), record.get(col));
                }
            }
        }
        
        Object ret = script.eval(task, bindings, task.getName(), "JavaScript");
        
        if (ret instanceof Boolean)
            return (Boolean)ret ? TaskResult.CONTINUE : TaskResult.REJECT;
        
        return TaskResult.REJECT;
        
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
        Variable var = task.getVariable(CONDITION_VAR);
        
        String code = var != null ? var.getValue() : null;
        
        if (!Utils.isNothing(code))
        {
            Script script = new Script();
            
            task.setValue(task.getName() + "script", script);
            
            code = Script.sql2Java(code);
            
            LinkedHashMap<String, String> condVars = Script.getVariables(code);
            
            task.setValue(task.getName() + "condvars", condVars);
            
            DataSet dataSet = task.getDataSet();
            
            if (!condVars.isEmpty())
                for (String name : condVars.keySet())
                    if (dataSet.getFields().containsKey(name))
                    {
                        String newName = name.replaceAll(" ", "_");
                        
                        code = code.replaceAll("\"" + name + "\"", newName);
                    }
            
            script.compile(task, task.getName(), Script.any2Js(code),
                    "JavaScript");
        }
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
        return false;
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
