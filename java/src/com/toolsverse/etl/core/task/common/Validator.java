/*
 * Validator.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

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
 * This is a {@link Task#INLINE} task which validates the given field or any field in the current row. If validation fails it can either halt etl process all together, 
 * reject value change, continue, or stop. The actual validation is a result of executing code in the supported script language, for example JavaScript.       
 * 
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Validator implements OnTask
{
    
    /** The CODE_VAR_NAME - the value of this variable defines validation code in the scripting language. */
    private static final String CODE_VAR_NAME = "CODE";
    
    /** The FIELD_VAR_NAME - the value of this variable defines name of the field to validate. Not require. */
    private static final String FIELD_VAR_NAME = "FIELD";
    
    /** The LANG_VAR_NAME - the value of this variable defines programming language for the code define by variable CODE. */
    private static final String LANG_VAR_NAME = "LANG";
    
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
        
        Script script = (Script)task.getValue(task.getName() + "script");
        if (script == null)
            return TaskResult.CONTINUE;
        
        Variable var = task.getVariable(LANG_VAR_NAME);
        String lang = (var == null || var.getValue() == null) ? "JavaScript"
                : var.getValue();
        
        String field = null;
        var = task.getVariable(FIELD_VAR_NAME);
        if (var != null && var.getValue() != null)
            field = var.getValue();
        
        DataSetRecord record = dataSet.getRecord(dataSet.getRecordCount() - 1);
        
        int fieldIndex = dataSet.getFieldIndex(field);
        FieldDef fieldDef = null;
        Object value = null;
        
        if (fieldIndex > 0)
        {
            fieldDef = dataSet.getFieldDef(fieldIndex);
            value = record.get(fieldIndex);
        }
        
        Bindings bindings = script.getBindings(task, lang);
        
        bindings.put("fieldValue", value);
        bindings.put("variable", var);
        bindings.put("currentRow", record);
        bindings.put("dataSet", dataSet);
        bindings.put("etlConfig", config);
        bindings.put("row", index);
        bindings.put("fieldDef", fieldDef);
        bindings.put("fieldIndex", fieldIndex);
        
        value = script.eval(task, bindings, task.getName(), lang);
        
        if (value instanceof TaskResult)
            return (TaskResult)value;
        else
            return null;
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
        Variable var = task.getVariable(CODE_VAR_NAME);
        
        String code = var != null ? var.getValue() : null;
        
        if (!Utils.isNothing(code))
        {
            var = task.getVariable(LANG_VAR_NAME);
            String lang = (var == null || var.getValue() == null) ? "JavaScript"
                    : var.getValue();
            
            Script script = new Script();
            
            task.setValue(task.getName() + "script", script);
            
            script.compile(task, task.getName(), code, lang);
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
