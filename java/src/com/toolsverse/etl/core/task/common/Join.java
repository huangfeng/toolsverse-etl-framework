/*
 * Join.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import com.toolsverse.etl.common.CommonEtlUtils;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Source;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;

/**
 * This {@link Task#POST} task performs a join operation on two data sets. 
 *
 * @see com.toolsverse.etl.core.engine.Task
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Join implements OnTask
{
    /** The JOIN variable. Defines name of the source to join with. */
    public static final String JOIN_VAR = "JOIN";
    
    /** The type of join variable **/
    public static final String JOIN_TYPE_VAR = "TYPE";
    
    /** INNER (default) */
    public static final String INNER = "inner";
    
    /** OUTER */
    public static final String OUTER = "outer";
    
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
        if (dataSet == null || dataSet.getFieldCount() == 0
                || dataSet.getRecordCount() == 0)
            return new TaskResult(dataSet);
        
        String sourceName = task.getVariable(JOIN_VAR).getValue();
        
        if (Utils.isNothing(sourceName))
            return new TaskResult(dataSet);
        
        if (task.getScenario().getSources() == null)
            return new TaskResult(dataSet);
        
        Source source = task.getScenario().getSources()
                .get(sourceName.toUpperCase());
        
        if (source == null || source.getDataSet() == null)
            return new TaskResult(dataSet);
        
        String exclude = task.getVariable(EtlConfig.EXCLUDE_VAR).getValue();
        String include = task.getVariable(EtlConfig.INCLUDE_VAR).getValue();
        
        String joinType = task.getVariable(JOIN_TYPE_VAR).getValue().trim();
        
        String keys = task.getVariable(EtlConfig.KEYS_VAR).getValue();
        
        boolean isOuter = false;
        if (OUTER.equalsIgnoreCase(joinType))
        {
            isOuter = true;
        }
        
        dataSet = CommonEtlUtils.join(dataSet, source.getDataSet(), keys,
                isOuter, include, exclude);
        
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
        
        EtlUtils.addVar(task.getVariables(), JOIN_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), JOIN_TYPE_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.KEYS_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.EXCLUDE_VAR, "", false);
        EtlUtils.addVar(task.getVariables(), EtlConfig.INCLUDE_VAR, "", false);
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
