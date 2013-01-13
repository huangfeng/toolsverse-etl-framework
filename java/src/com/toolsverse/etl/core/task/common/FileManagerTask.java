/*
 * RegexpTransformator.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.task.common;

import java.util.List;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.OnTask;
import com.toolsverse.etl.core.engine.Task;
import com.toolsverse.etl.core.engine.TaskResult;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.io.FileResource;
import com.toolsverse.io.IoProcessor;
import com.toolsverse.io.IoProcessorFactory;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * This is a {@link Task#PRE} and {@link Task#POST} task for the basic file operations such as: copy, move, rename, delete, zip, unzip. The local file system, ftp and sftp are supported.   
 * 
 * @see com.toolsverse.etl.core.engine.Task
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class FileManagerTask implements OnTask
{
    // variables
    
    /** The COMMAND_VAR. */
    public static final String COMMAND_VAR = "COMMAND";
    
    /** The SOURCE_TYPE_VAR. */
    public static final String SOURCE_TYPE_VAR = "SOURCE_TYPE";
    
    /** The DESTINATION_TYPE_VAR. */
    public static final String DESTINATION_TYPE_VAR = "DESTINATION_TYPE";
    
    /** The SOURCE_URL_VAR. */
    public static final String SOURCE_URL_VAR = "SOURCE_URL";
    
    /** The DESTINATION_URL_VAR. */
    public static final String DESTINATION_URL_VAR = "DESTINATION_URL";
    
    /** The SOURCE_USER_VAR. */
    public static final String SOURCE_USER_VAR = "SOURCE_USER";
    
    /** The DESTINATION_USER_VAR. */
    public static final String DESTINATION_USER_VAR = "DESTINATION_USER";
    
    /** The SOURCE_PASSWORD_VAR. */
    public static final String SOURCE_PASSWORD_VAR = "SOURCE_PASSWORD";
    
    /** The DESTINATION_PASSWORD_VAR. */
    public static final String DESTINATION_PASSWORD_VAR = "DESTINATION_PASSWORD";
    
    /** The SOURCE_PROXY_VAR. */
    public static final String SOURCE_PROXY_VAR = "SOURCE_PROXY";
    
    /** The DESTINATION_PROXY_VAR. */
    public static final String DESTINATION_PROXY_VAR = "DESTINATION_PROXY";
    
    /** The SOURCE_PROXY_PORT_VAR. */
    public static final String SOURCE_PROXY_PORT_VAR = "SOURCE_PROXY_PORT";
    
    /** The FILE_COUNT_VAR. */
    public static final String FILE_COUNT_VAR = "COUNT";
    
    /** The LANGUAGE VAR. */
    public static final String FILE_COUNT_LANG_VAR = "LANG";
    
    public static final String FILE_COUNT_CODE_VAR = "CODE";
    
    /** The DESTINATION_PROXY_PORT_VAR. */
    public static final String DESTINATION_PROXY_PORT_VAR = "DESTINATION_PROXY_PORT";
    
    /** The FILES_VAR. */
    public static final String FILES_VAR = "FILES";
    
    /** The TO_FILES_VAR. */
    public static final String TO_FILES_VAR = "TO_FILES";
    
    /** The SOURCE_FOLDER_VAR. */
    public static final String SOURCE_FOLDER_VAR = "SOURCE_FOLDER";
    
    /** The DEST_FOLDER_VAR. */
    public static final String DEST_FOLDER_VAR = "DESTINATION_FOLDER";
    
    /** The ZIP_FILENAME_VAR. */
    public static final String ZIP_FILENAME_VAR = "ZIP_FILENAME";
    
    /** The SOURCE_PASSIVE_MODE_VAR. */
    public static final String SOURCE_PASSIVE_MODE_VAR = "SOURCE_PASSIVE_MODE";
    
    /** The DEST_PASSIVE_MODE_VAR. */
    public static final String DEST_PASSIVE_MODE_VAR = "DESTINATION_PASSIVE_MODE";
    
    /** The COMMAND_COPY. */
    public static final String COMMAND_COPY = "copy";
    
    /** The COMMAND_MOVE. */
    public static final String COMMAND_MOVE = "move";
    
    /** The COMMAND_COUNT. */
    public static final String COMMAND_COUNT = "count";
    
    /** The COMMAND_RENAME. */
    public static final String COMMAND_RENAME = "rename";
    
    /** The COMMAND_DELETE. */
    public static final String COMMAND_DELETE = "delete";
    
    /** The COMMAND_ZIP. */
    public static final String COMMAND_ZIP = "zip";
    
    /** The COMMAND_UNZIP. */
    public static final String COMMAND_UNZIP = "unzip";
    
    /** The COMMAND_ZIP_DELETE. */
    public static final String COMMAND_ZIP_DELETE = "zip_delete";
    
    /** The COMMAND_UNZIP_DELETE. */
    public static final String COMMAND_UNZIP_DELETE = "unzip_delete";
    
    /**
     * Checks file count.
     *
     * @param sourceTypeString the source type string
     * @param sourceUrl the source url
     * @param sourceUser the source user
     * @param sourcePassword the source password
     * @param sourceProxy the source proxy
     * @param sourceProxyPort the source proxy port
     * @param files the files
     * @param sourceFolder the source folder
     * @param sourcePasssiveMode the source passsive mode
     * @param count the count
     * @param lang the lang
     * @param code the code
     * @throws Exception the exception if case of any error or if file count less then expected
     */
    private void checkFileCount(String sourceType, String sourceUrl,
            String sourceUser, String sourcePassword, String sourceProxy,
            String sourceProxyPort, String files, String sourceFolder,
            boolean sourcePasssiveMode, int count, String lang, String code)
        
        throws Exception
    {
        IoProcessor source = null;
        boolean sourceConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            
            List<FileResource> list = source
                    .getList(sourceFolder, files, false);
            
            if (Utils.isNothing(code))
            {
                if ((list != null && list.size() < count)
                        || (list == null && count > 0))
                    throw new Exception(
                            EtlResource.FILE_COUNT_LESS_THAN_EXPECTED
                                    .getValue());
                
                return;
            }
            
            ScriptEngineManager mgr = new ScriptEngineManager();
            ScriptEngine engine = mgr.getEngineByName(lang);
            
            engine.put("files", list);
            engine.put("count", count);
            
            Object value = engine.eval(code);
            
            boolean ret = false;
            
            if (value instanceof String)
            {
                ret = Utils.str2Boolean((String)value, false);
            }
            else if (value instanceof Boolean)
            {
                ret = (Boolean)value;
            }
            
            if (!ret)
                throw new Exception(
                        EtlResource.FILE_COUNT_LESS_THAN_EXPECTED.getValue());
            
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
        }
        
    }
    
    /**
     * Connects to the resource, such as ftp, sftp, etc.
     *
     * @param type the type of the resource
     * @param url the url
     * @param user the user
     * @param password the password
     * @param proxy the proxy host
     * @param proxyPort the proxy port
     * @param passiveMode the passive mode
     * @return the io processor
     * @throws Exception in case of any error
     */
    private IoProcessor connect(String type, String url, String user,
            String password, String proxy, String proxyPort, boolean passiveMode)
        throws Exception
    {
        IoProcessor resourceProcessor = null;
        
        resourceProcessor = IoProcessorFactory.instance().getIoProcessor(type,
                proxy, Integer.parseInt(proxyPort));
        resourceProcessor.connect(url, user, password, passiveMode);
        
        return resourceProcessor;
    }
    
    /**
     * Connects to the local file system.
     *
     * @return the io processor
     * @throws Exception in case of any error
     */
    private IoProcessor connectLocal()
        throws Exception
    {
        return connect(IoProcessorFactory.FILE, null, null, null, null, "-1",
                false);
    }
    
    /**
     * Copies file from one resource to another.
     *
     * @param sourceType the source resource type
     * @param destType the destination resource type
     * @param sourceUrl the source url
     * @param destUrl the destination url
     * @param sourceUser the source user
     * @param destUser the destination user
     * @param sourcePassword the source password
     * @param destPassword the destination password
     * @param sourceProxy the source proxy host
     * @param destProxy the destination proxy host
     * @param sourceProxyPort the source proxy port
     * @param destProxyPort the destination proxy port
     * @param files the files
     * @param sourceFolder the source folder
     * @param destFolder the destination folder
     * @param sourcePasssiveMode the source passsive mode
     * @param destPasssiveMode the destination passive mode
     * @param move the move. If true deletes file from the source resource.
     * @throws Exception in case of any error
     */
    private void copy(String sourceType, String destType, String sourceUrl,
            String destUrl, String sourceUser, String destUser,
            String sourcePassword, String destPassword, String sourceProxy,
            String destProxy, String sourceProxyPort, String destProxyPort,
            String files, String sourceFolder, String destFolder,
            boolean sourcePasssiveMode, boolean destPasssiveMode, boolean move)
        
        throws Exception
    {
        IoProcessor source = null;
        IoProcessor destination = null;
        boolean sourceConnected = false;
        boolean destConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            
            destination = connect(destType, destUrl, destUser, destPassword,
                    destProxy, destProxyPort, destPasssiveMode);
            destConnected = true;
            
            List<FileResource> list = source
                    .getList(sourceFolder, files, false);
            if (list == null)
                return;
            
            for (FileResource file : list)
            {
                String filename = file.getName();
                
                if (!source
                        .get(destination, sourceFolder, destFolder, filename))
                    continue;
                
                if (move)
                    source.delete(sourceFolder, filename);
            }
            
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
            if (destConnected)
                disconnect(destination);
        }
    }
    
    /**
     * Deletes files.
     *
     * @param sourceType the source type
     * @param sourceUrl the source url
     * @param sourceUser the source user
     * @param sourcePassword the source password
     * @param sourceProxy the source proxy host
     * @param sourceProxyPort the source proxy port
     * @param files the file name. Masks such as *.txt", "??.*" are supported
     * @param sourceFolder the source folder
     * @param sourcePasssiveMode the source passsive mode
     * @throws Exception in case of any error
     */
    private void delete(String sourceType, String sourceUrl, String sourceUser,
            String sourcePassword, String sourceProxy, String sourceProxyPort,
            String files, String sourceFolder, boolean sourcePasssiveMode)
        throws Exception
    {
        IoProcessor source = null;
        boolean sourceConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            
            List<FileResource> list = source
                    .getList(sourceFolder, files, false);
            if (list == null)
                return;
            
            for (FileResource file : list)
            {
                String filename = file.getName();
                
                source.delete(sourceFolder, filename);
            }
            
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
        }
    }
    
    /**
     * Disconnects from the resource.
     *
     * @param resourceProcessor the resource processor
     */
    private void disconnect(IoProcessor resourceProcessor)
    {
        try
        {
            resourceProcessor.disconnect();
        }
        catch (Exception ex)
        {
            
            Logger.log(Logger.SEVERE, EtlLogger.class,
                    EtlResource.CANNOT_CLOSE_CONNECTION.getValue(), ex);
        }
        
    }
    
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
        executePreTask(config, task);
        
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
        String command = EtlUtils.getVarValue(task.getVariables(), COMMAND_VAR,
                COMMAND_COPY);
        String sourceType = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_TYPE_VAR, "");
        String destType = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_TYPE_VAR, "");
        String sourceUrl = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_URL_VAR, "");
        String destUrl = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_URL_VAR, "");
        String sourceUser = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_USER_VAR, "");
        String destUser = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_USER_VAR, "");
        String sourcePassword = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_PASSWORD_VAR, "");
        String destPassword = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_PASSWORD_VAR, "");
        String sourceProxy = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_PROXY_VAR, null);
        String destProxy = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_PROXY_VAR, "null");
        String sourceProxyPort = EtlUtils.getVarValue(task.getVariables(),
                SOURCE_PROXY_PORT_VAR, "-1");
        String destProxyPort = EtlUtils.getVarValue(task.getVariables(),
                DESTINATION_PROXY_PORT_VAR, "-1");
        
        int fileCount = Utils.str2Int(
                EtlUtils.getVarValue(task.getVariables(), FILE_COUNT_VAR, "1"),
                1);
        
        String lang = EtlUtils.getVarValue(task.getVariables(),
                FILE_COUNT_LANG_VAR, "JavaScript");
        
        String code = EtlUtils.getVarValue(task.getVariables(),
                FILE_COUNT_CODE_VAR, null);
        
        String files = EtlUtils.getVarValue(task.getVariables(), FILES_VAR,
                "*.*");
        String toFiles = EtlUtils.getVarValue(task.getVariables(),
                TO_FILES_VAR, files);
        String sourceFolder = SystemConfig.instance().getPathUsingAppFolders(
                FileUtils.getUnixFolderName(EtlUtils.getVarValue(task
                        .getVariables(), SOURCE_FOLDER_VAR, SystemConfig
                        .instance().getDataFolderName())));
        String destFolder = SystemConfig.instance().getPathUsingAppFolders(
                FileUtils.getUnixFolderName(EtlUtils.getVarValue(task
                        .getVariables(), DEST_FOLDER_VAR, SystemConfig
                        .instance().getDataFolderName())));
        String zipFilename = EtlUtils.getVarValue(task.getVariables(),
                ZIP_FILENAME_VAR, null);
        
        boolean sourcePasssiveMode = Utils.str2Boolean(EtlUtils.getVarValue(
                task.getVariables(), SOURCE_PASSIVE_MODE_VAR, "No"), false);
        boolean destPasssiveMode = Utils.str2Boolean(EtlUtils.getVarValue(
                task.getVariables(), DEST_PASSIVE_MODE_VAR, "No"), false);
        
        if (command.equalsIgnoreCase(COMMAND_COPY))
            copy(sourceType, destType, sourceUrl, destUrl, sourceUser,
                    destUser, sourcePassword, destPassword, sourceProxy,
                    destProxy, sourceProxyPort, destProxyPort, files,
                    sourceFolder, destFolder, sourcePasssiveMode,
                    destPasssiveMode, false);
        else if (command.equalsIgnoreCase(COMMAND_MOVE))
            copy(sourceType, destType, sourceUrl, destUrl, sourceUser,
                    destUser, sourcePassword, destPassword, sourceProxy,
                    destProxy, sourceProxyPort, destProxyPort, files,
                    sourceFolder, destFolder, sourcePasssiveMode,
                    destPasssiveMode, true);
        else if (command.equalsIgnoreCase(COMMAND_RENAME))
            rename(sourceType, destType, sourceUrl, destUrl, sourceUser,
                    destUser, sourcePassword, destPassword, sourceProxy,
                    destProxy, sourceProxyPort, destProxyPort, files, toFiles,
                    sourceFolder, destFolder, sourcePasssiveMode,
                    destPasssiveMode);
        else if (command.equalsIgnoreCase(COMMAND_DELETE))
            delete(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, files, sourceFolder,
                    sourcePasssiveMode);
        else if (command.equalsIgnoreCase(COMMAND_ZIP))
            zip(sourceType, sourceUrl, sourceUser, sourcePassword, sourceProxy,
                    sourceProxyPort, files, sourceFolder, zipFilename,
                    FileUtils.getUnixFolderName(SystemConfig.instance()
                            .getDataFolderName()), sourcePasssiveMode, false);
        else if (command.equalsIgnoreCase(COMMAND_UNZIP))
            unzip(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourceFolder, zipFilename,
                    FileUtils.getUnixFolderName(SystemConfig.instance()
                            .getDataFolderName()), sourcePasssiveMode, false);
        else if (command.equalsIgnoreCase(COMMAND_ZIP_DELETE))
            zip(sourceType, sourceUrl, sourceUser, sourcePassword, sourceProxy,
                    sourceProxyPort, files, sourceFolder, zipFilename,
                    FileUtils.getUnixFolderName(SystemConfig.instance()
                            .getDataFolderName()), sourcePasssiveMode, true);
        else if (command.equalsIgnoreCase(COMMAND_UNZIP_DELETE))
            unzip(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourceFolder, zipFilename,
                    FileUtils.getUnixFolderName(SystemConfig.instance()
                            .getDataFolderName()), sourcePasssiveMode, true);
        else if (command.equalsIgnoreCase(COMMAND_COUNT))
            checkFileCount(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, files, sourceFolder,
                    sourcePasssiveMode, fileCount, lang, code);
        else
            Logger.log(Logger.WARNING, EtlLogger.class,
                    EtlResource.UNKNOWN_COMMAND.getValue() + command);
        
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
        return true;
    }
    
    /**
     * Renames files.
     *
     * @param sourceType the source type
     * @param destType the destination type
     * @param sourceUrl the source url
     * @param destUrl the destination url
     * @param sourceUser the source user
     * @param destUser the destination user
     * @param sourcePassword the source password
     * @param destPassword the destination password
     * @param sourceProxy the source proxy host
     * @param destProxy the destination proxy host
     * @param sourceProxyPort the source proxy port
     * @param destProxyPort the destination proxy port
     * @param files the file name. Masks such as *.txt", "??.*" are supported 
     * @param toFiles the to files
     * @param sourceFolder the source folder
     * @param destFolder the destination folder
     * @param sourcePasssiveMode the source passive mode
     * @param destPasssiveMode the destination passsive mode
     * @throws Exception in case of any error
     */
    private void rename(String sourceType, String destType, String sourceUrl,
            String destUrl, String sourceUser, String destUser,
            String sourcePassword, String destPassword, String sourceProxy,
            String destProxy, String sourceProxyPort, String destProxyPort,
            String files, String toFiles, String sourceFolder,
            String destFolder, boolean sourcePasssiveMode,
            boolean destPasssiveMode)
        
        throws Exception
    {
        IoProcessor source = null;
        IoProcessor destination = null;
        boolean sourceConnected = false;
        boolean destConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            
            destination = connect(destType, destUrl, destUser, destPassword,
                    destProxy, destProxyPort, destPasssiveMode);
            destConnected = true;
            
            List<FileResource> list = source
                    .getList(sourceFolder, files, false);
            if (list == null)
                return;
            
            for (FileResource file : list)
            {
                String filename = file.getName();
                
                String oldName = FilenameUtils.getBaseName(filename);
                String oldExt = FilenameUtils.getExtension(filename);
                
                String newName = FilenameUtils.getBaseName(toFiles);
                String newExt = FilenameUtils.getExtension(toFiles);
                
                if (newName.equals("*"))
                    newName = oldName;
                if (newExt.equals("*"))
                    newExt = oldExt;
                
                String toFilename = newName + "." + newExt;
                
                source.move(destination, sourceFolder, destFolder, filename,
                        toFilename);
            }
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
            if (destConnected)
                disconnect(destination);
        }
    }
    
    /**
     * Unzips files.
     *
     * @param folder the folder
     * @param zipFilename the zip filename
     * @return the list of file names in the zip
     * @throws Exception in case of any error
     */
    private List<String> unzip(String folder, String zipFilename)
        throws Exception
    {
        return FileUtils.unzipFiles(folder,
                folder + FilenameUtils.getName(zipFilename));
    }
    
    /**
     * Unzips files.
     *
     * @param sourceType the source type
     * @param sourceUrl the source url
     * @param sourceUser the source user
     * @param sourcePassword the source password
     * @param sourceProxy the source proxy
     * @param sourceProxyPort the source proxy port
     * @param sourceFolder the source folder
     * @param zipFilename the zip filename
     * @param destFolder the destination folder
     * @param sourcePasssiveMode the source passive mode
     * @param delete the delete. If true deletes original zip file.
     * @throws Exception in case of any error
     */
    private void unzip(String sourceType, String sourceUrl, String sourceUser,
            String sourcePassword, String sourceProxy, String sourceProxyPort,
            String sourceFolder, String zipFilename, String destFolder,
            boolean sourcePasssiveMode, boolean delete)
        throws Exception
    {
        IoProcessor source = null;
        IoProcessor destination = null;
        boolean sourceConnected = false;
        boolean destConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            if (Utils.isNothing(zipFilename))
                throw new Exception(EtlResource.ZIPFILE_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            if (!source.isFileSystem())
            {
                destination = connectLocal();
                destConnected = true;
                
                source.get(destination, sourceFolder, destFolder, zipFilename);
                
                List<String> list = unzip(destFolder, zipFilename);
                
                for (int i = 0; i < list.size(); i++)
                {
                    String filename = list.get(i);
                    
                    if (!source.put(destination, destFolder, sourceFolder,
                            filename))
                        continue;
                    
                    destination.delete(destFolder, filename);
                }
                
                if (delete)
                    source.delete(sourceFolder, zipFilename);
            }
            else
            {
                unzip(sourceFolder, zipFilename);
                
                if (delete)
                    source.delete(sourceFolder, zipFilename);
            }
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
            if (destConnected)
                disconnect(destination);
        }
    }
    
    /**
     * Zips files.
     *
     * @param resourceProcessor the resource processor
     * @param folder the folder
     * @param files the file name. Masks such as *.txt", "??.*" are supported
     * @param zipFilename the zip file name
     * @throws Exception in case of any error
     */
    private void zip(IoProcessor resourceProcessor, String folder,
            String files, String zipFilename)
        throws Exception
    {
        List<FileResource> list = resourceProcessor.getList(folder, files,
                false);
        if (list == null || list.size() == 0)
            return;
        
        FileUtils.addFilesToZip(folder,
                folder + FilenameUtils.getName(zipFilename), files);
    }
    
    /**
     * Zips files.
     *
     * @param sourceType the source type
     * @param sourceUrl the source url
     * @param sourceUser the source user
     * @param sourcePassword the source password
     * @param sourceProxy the source proxy host
     * @param sourceProxyPort the source proxy port
     * @param files the files
     * @param sourceFolder the source folder
     * @param zipFilename the zip file name
     * @param destFolder the destination folder
     * @param sourcePasssiveMode the source passive mode
     * @param delete the delete. If true deletes original files. 
     * @throws Exception in case of any error
     */
    private void zip(String sourceType, String sourceUrl, String sourceUser,
            String sourcePassword, String sourceProxy, String sourceProxyPort,
            String files, String sourceFolder, String zipFilename,
            String destFolder, boolean sourcePasssiveMode, boolean delete)
        throws Exception
    {
        IoProcessor source = null;
        IoProcessor destination = null;
        boolean sourceConnected = false;
        boolean destConnected = false;
        
        try
        {
            if (Utils.isNothing(sourceFolder))
                throw new Exception(
                        EtlResource.SOURCE_FOLDER_NOT_DEFINED.getValue());
            
            if (Utils.isNothing(zipFilename))
                throw new Exception(EtlResource.ZIPFILE_NOT_DEFINED.getValue());
            
            source = connect(sourceType, sourceUrl, sourceUser, sourcePassword,
                    sourceProxy, sourceProxyPort, sourcePasssiveMode);
            sourceConnected = true;
            if (!source.isFileSystem())
            {
                destination = connectLocal();
                destConnected = true;
                
                List<FileResource> list = source.getList(sourceFolder, files,
                        false);
                
                if (list == null || list.size() == 0)
                    return;
                
                for (FileResource file : list)
                {
                    String filename = file.getName();
                    
                    source.get(destination, sourceFolder, destFolder, filename);
                }
                
                zip(destination, destFolder, files, zipFilename);
                
                source.put(destination, destFolder, sourceFolder, zipFilename);
                
                if (delete)
                    for (FileResource file : list)
                    {
                        String filename = file.getName();
                        
                        source.delete(sourceFolder, filename);
                    }
            }
            else
            {
                List<FileResource> list = source.getList(sourceFolder, files,
                        false);
                
                if (list == null || list.size() == 0)
                    return;
                
                zip(source, sourceFolder, files, zipFilename);
                
                if (delete)
                    for (FileResource file : list)
                    {
                        String filename = file.getName();
                        
                        source.delete(sourceFolder, filename);
                    }
            }
        }
        finally
        {
            if (sourceConnected)
                disconnect(source);
            if (destConnected)
                disconnect(destination);
        }
    }
}
