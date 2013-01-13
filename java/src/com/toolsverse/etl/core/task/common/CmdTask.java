/*
 * CmdTask.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Writer;
import java.util.HashMap;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * This is a {@link Task#POST} task which executes the specified command in a separate OS process. 
 * 
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class CmdTask implements OnTask
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
        String cmd = null;
        
        String scriptsFolder = FileUtils.getUnixFolderName(SystemConfig
                .instance().getScriptsFolder());
        
        String successMask = EtlUtils.getVarValue(task.getVariables(),
                EtlConfig.SUCCESS_MASK_VAR, null);
        String errorMask = EtlUtils.getVarValue(task.getVariables(),
                EtlConfig.ERROR_MASK_VAR, null);
        
        if (!Utils.isNothing(task.getCode()))
        {
            cmd = task.getCode();
            
            if (task.getVariables() != null && task.getVariables().size() > 0)
                cmd = EtlUtils.mergeSqlWithVars(config, cmd, task.getUsing(),
                        task.getVariables(), new HashMap<String, Object>());
        }
        
        if (!Utils.isNothing(cmd))
        {
            String logFileName = FileUtils.getFullFileName(scriptsFolder,
                    task.getName(), ".log", false);
            
            String cmdFileName = FileUtils
                    .getFullFileName(scriptsFolder, task.getName(), Utils
                            .getShellExt(EtlUtils.getVarValue(
                                    task.getVariables(),
                                    EtlConfig.SHELL_EXT_VAR, null)), false);
            
            File logFile = new File(logFileName);
            logFile.delete();
            
            File cmdFile = new File(cmdFileName);
            cmdFile.delete();
            
            if (cmd.indexOf(">") <= 0)
            {
                cmd = cmd.trim() + " > \"" + logFileName + "\"";
            }
            
            boolean parseLog = cmd.toLowerCase().indexOf(
                    logFileName.toLowerCase()) >= 0;
            
            cmd = SystemConfig.instance().getPathUsingAppFolders(cmd);
            
            Writer output = null;
            try
            {
                output = new BufferedWriter(new FileWriter(cmdFile));
                output.write(cmd);
            }
            finally
            {
                if (output != null)
                    output.close();
            }
            
            boolean isError = false;
            
            try
            {
                Process process = Utils.execScript(cmdFileName);
                
                process.waitFor();
                
                String error = parseLog ? getError(logFileName, errorMask,
                        successMask) : null;
                
                if (!Utils.isNothing(error))
                {
                    isError = true;
                    throw new Exception(error);
                }
            }
            finally
            {
                if (!isError)
                {
                    logFile.delete();
                    cmdFile.delete();
                }
            }
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
        return null;
    }
    
    /**
     * Gets the error from the out file.
     *
     * @param name the name
     * @param errorMask the error mask
     * @param successMask the success mask
     * @return the error
     * @throws Exception in case of any error
     */
    private String getError(String name, String errorMask, String successMask)
        throws Exception
    {
        File file = new File(name);
        
        if (!file.exists())
            return name + EtlResource.LOG_FILE_DOESNT_EXIST_STR.getValue();
        
        if (Utils.isNothing(errorMask) && Utils.isNothing(successMask)
                || file.length() == 0)
            return null;
        
        StringBuffer error = new StringBuffer();
        
        BufferedReader input = null;
        try
        {
            input = new BufferedReader(new FileReader(file));
            String line = null;
            while ((line = input.readLine()) != null)
            {
                if (Utils.belongsTo(successMask, line))
                    return null;
                
                if (line.indexOf("\n") < 0)
                    line = line + "\n";
                
                error.append(line);
                
                if (Utils.belongsTo(errorMask, line))
                    break;
            }
        }
        finally
        {
            try
            {
                if (input != null)
                {
                    input.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.INFO, this,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
        
        return error.toString();
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
        EtlUtils.addVar(task.getVariables(), EtlConfig.DATA_FOLDER_VAR,
                FileUtils.getUnixFolderName(SystemConfig.instance()
                        .getDataFolderName()), true);
        
        Alias alias = (Alias)config.getConnectionFactory().getConnectionParams(
                task.getConnection());
        
        if (alias == null)
            return;
        
        EtlUtils.addVar(task.getVariables(), EtlConfig.USER_VAR,
                Utils.makeString(alias.getUserId()), true);
        EtlUtils.addVar(task.getVariables(), EtlConfig.PASSWORD_VAR,
                Utils.makeString(alias.getPassword()), true);
        EtlUtils.addVar(task.getVariables(), EtlConfig.DB_URL_VAR,
                alias.getUrl(), true);
        EtlUtils.addVar(task.getVariables(), EtlConfig.DB_PARAMS_VAR,
                Utils.makeString(alias.getParams()), true);
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
