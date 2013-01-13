/*
 * Transpose.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.sql.Types;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.util.ListHashMap;

/**
 * This {@link Task#POST} task transposes data set. 
 *
 * @see com.toolsverse.etl.core.engine.Task
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Transpose implements OnTask
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
        if (dataSet == null || dataSet.getFieldCount() == 0)
            return new TaskResult(dataSet);
        
        int fieldCount = dataSet.getFieldCount();
        int rowCount = dataSet.getRecordCount();
        
        DataSet newDataSet = dataSet.copy();
        
        DataSetData data = new DataSetData();
        
        ListHashMap<String, FieldDef> fields = new ListHashMap<String, FieldDef>();
        
        DataSetRecord record = null;
        
        boolean diffFields = false;
        for (int col = 0; col < fieldCount; col++)
        {
            FieldDef field = dataSet.getFieldDef(col);
            
            if (col > 0
                    && field.getSqlDataType() != dataSet.getFieldDef(col - 1)
                            .getSqlDataType())
            {
                diffFields = true;
                
                break;
            }
        }
        
        FieldDef field = null;
        
        if (!diffFields)
            field = dataSet.getFieldDef(0).copy();
        else
        {
            field = new FieldDef();
            field.setNativeDataType("VARCHAR");
            field.setSqlDataType(Types.VARCHAR);
        }
        
        for (int row = 0; row < rowCount; row++)
        {
            for (int col = 0; col < fieldCount; col++)
            {
                Object value = dataSet.getFieldValue(row, col);
                
                if (row == 0)
                {
                    record = new DataSetRecord();
                    
                    data.add(record);
                }
                else
                    record = data.get(col);
                
                record.add(value);
            }
            
            FieldDef fieldToAdd = field.copy();
            
            String name = "column_" + row;
            fieldToAdd.setName(name);
            
            fields.put(name, fieldToAdd);
        }
        
        dataSet.setData(null);
        dataSet.setFields(null);
        
        newDataSet.setData(data);
        newDataSet.setFields(fields);
        
        return new TaskResult(newDataSet);
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
