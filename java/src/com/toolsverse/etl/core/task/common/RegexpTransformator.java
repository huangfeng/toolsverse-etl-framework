/*
 * RegexpTransformator.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * This is a {@link Task#INLINE} task which evaluates regular expression and if it matches given field value replaces it on the given string.  
 * 
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class RegexpTransformator implements OnTask
{
    
    /** The REGEXP_VAR_NAME - the value of this variable defines regular expression. */
    private static final String REGEXP_VAR_NAME = "REGEXP";
    
    /** The FIELD_VAR_NAME - the value of this variable defines name of the field to update. */
    private static final String FIELD_VAR_NAME = "FIELD";
    
    /** The REPLACE_VAR_NAME - the value of this variable defines replacement string. */
    private static final String REPLACE_VAR_NAME = "REPLACE";
    
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
        
        Variable var = task.getVariable(FIELD_VAR_NAME);
        if (var == null || var.getValue() == null)
            return null;
        String field = var.getValue();
        
        var = task.getVariable(REGEXP_VAR_NAME);
        if (var == null || var.getValue() == null)
            return null;
        String regexp = var.getValue();
        
        var = task.getVariable(REPLACE_VAR_NAME);
        if (var == null || var.getValue() == null)
            return null;
        String replace = var.getValue();
        
        DataSetRecord record = dataSet.getRecord(dataSet.getRecordCount() - 1);
        
        int fieldIndex = dataSet.getFieldIndex(field);
        
        if (fieldIndex < 0)
            return null;
        
        Object value = record.get(fieldIndex);
        
        if (value == null)
            return null;
        
        try
        {
            String result = value.toString().replaceAll(regexp, replace);
            
            record.set(fieldIndex, result);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, EtlLogger.class, Utils.format(
                    EtlResource.ERROR_TRANSOFRMING_FIELD.getValue(),
                    new String[] {field}));
            
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
        return null;
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
