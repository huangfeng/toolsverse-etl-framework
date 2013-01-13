/*
 * TestInlineTask.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;

/**
 * TestInlineTask
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @see
 * @since 1.0
 */

public class TestInlineTask implements OnTask
{
    public TaskResult executeBeforeEtlTask(EtlConfig config, Task task)
        throws Exception
    {
        return null;
    }
    
    public TaskResult executeInlineTask(EtlConfig config, Task task, long index)
        throws Exception
    {
        DataSet dataSet = task.getDataSet();
        
        if (dataSet == null || dataSet.getRecordCount() <= 0)
            return null;
        
        DataSetRecord record = dataSet.getRecord(dataSet.getRecordCount() - 1);
        
        record.set(0, index);
        
        return new TaskResult();
    }
    
    public TaskResult executePostTask(EtlConfig config, Task task,
            DataSet dataSet)
        throws Exception
    {
        return new TaskResult(dataSet);
    }
    
    public TaskResult executePreTask(EtlConfig config, Task task)
        throws Exception
    {
        return null;
    }
    
    public void init(EtlConfig config, Task task)
        throws Exception
    {
    }
    
    public boolean isInlineTask()
    {
        return true;
    }
    
    public boolean isPostTask()
    {
        return false;
    }
    
    public boolean isPreEtlTask()
    {
        return false;
    }
    
    public boolean isPreTask()
    {
        return false;
    }
    
}