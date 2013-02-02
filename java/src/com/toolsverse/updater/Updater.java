/*
 * Updater.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.service.ServiceFactory;
import com.toolsverse.updater.service.UpdaterRequest;
import com.toolsverse.updater.service.UpdaterResponse;
import com.toolsverse.updater.service.UpdaterService;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The instance of this class handles product updates and product update requests. This class is thread safe.  
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class Updater
{
    // domain values for "on update" action
    
    /** The NOTIFY. */
    public static final String NOTIFY = "notify";
    
    /** The DOWNLOAD. */
    public static final String DOWNLOAD = "download";
    
    /** The SELENTLY DOWNLOAD. */
    public static final String SALENTLY_DOWNLOAD = "salentlydownload";
    
    // volatile is needed so that multiple thread can reconcile the instance
    
    /** The instance. */
    private volatile static Updater _instance;
    
    /**
     * Gets the singleton instance of the Updater.
     *
     * @return the Updater
     */
    public static Updater instance()
    {
        if (_instance == null)
        {
            synchronized (Updater.class)
            {
                if (_instance == null)
                    _instance = new Updater();
            }
        }
        
        return _instance;
    }
    
    /** The response. */
    private UpdaterResponse _response;
    
    /** The is running flag. */
    private AtomicBoolean _running;
    
    /** The is downloading flag. */
    private AtomicBoolean _downloading;
    
    /**
     * Instantiates a new updater.
     */
    public Updater()
    {
        _running = new AtomicBoolean(false);
        
        _downloading = new AtomicBoolean(false);
        
        _response = new UpdaterResponse(
                UpdaterResponse.ResponseCode.NO_SERVICE, null, null);
    }
    
    /**
     * Check for updates.
     *
     * @param callback the callback
     */
    public void checkForUpdates(final UpdaterCallbak callback)
    {
        checkForUpdates(callback, true);
    }
    
    /**
     * Check for updates.
     *
     * @param callback the callback
     * @param verbose the verbose
     */
    public void checkForUpdates(final UpdaterCallbak callback,
            final boolean verbose)
    {
        if (_running.get())
        {
            return;
        }
        
        if (!isEnabled())
        {
            _response = new UpdaterResponse(
                    UpdaterResponse.ResponseCode.NO_SERVICE, null, null);
            
            if (callback != null)
                callback.doneChecking(_response);
            
            return;
        }
        
        Runnable checker = new Runnable()
        {
            public void run()
            {
                try
                {
                    if (_downloading.get())
                    {
                        UpdaterResponse newResponse = new UpdaterResponse(
                                UpdaterResponse.ResponseCode.DOWNLOADING, null,
                                null);
                        
                        if (callback != null)
                            callback.doneChecking(newResponse);
                        
                        _response = newResponse;
                        
                        return;
                    }
                    
                    UpdaterService updaterService = (UpdaterService)ServiceFactory
                            .getService(UpdaterService.class);
                    
                    UpdaterResponse response = updaterService
                            .checkForUpdates(new UpdaterRequest());
                    
                    if (response.getResponseCode() == UpdaterResponse.ResponseCode.READY_TO_DOWNLOAD)
                    {
                        String fileName = SystemConfig.instance()
                                .getUpdateFolder() + response.getFileName();
                        
                        if (FileUtils.fileExists(fileName))
                        {
                            UpdaterResponse newResponse = new UpdaterResponse(
                                    UpdaterResponse.ResponseCode.DOWNLOADED,
                                    response.getDownloadUrl(),
                                    response.getFileName());
                            
                            if (callback != null)
                                callback.doneChecking(newResponse);
                            
                            _response = newResponse;
                            
                            return;
                        }
                    }
                    
                    if (callback != null)
                        callback.doneChecking(response);
                    
                    _response = response;
                }
                catch (Exception ex)
                {
                    if (verbose)
                        Logger.log(Logger.SEVERE, this,
                                UpdaterResource.ERROR_CHECKING_FOR_UPDATES
                                        .getValue(), ex);
                    
                    _response = new UpdaterResponse(
                            UpdaterResponse.ResponseCode.ERROR, null, null);
                    
                    if (callback != null)
                        callback.doneChecking(_response);
                }
                finally
                {
                    _running.set(false);
                }
                
            }
        };
        
        if (callback != null)
        {
            Thread thread = new Thread(checker);
            
            _running.set(true);
            thread.start();
        }
        else
            checker.run();
    }
    
    /**
     * Download update.
     *
     * @param response the response
     * @param callback the callback
     */
    public void downloadUpdate(final UpdaterResponse response,
            final UpdaterCallbak callback)
    {
        if (_downloading.get())
        {
            return;
        }
        
        if (!isEnabled())
        {
            _response = new UpdaterResponse(
                    UpdaterResponse.ResponseCode.NO_SERVICE, null, null);
            
            if (callback != null)
                callback.doneChecking(_response);
            
            return;
        }
        
        Runnable downloader = new Runnable()
        {
            public void run()
            {
                try
                {
                    if (!FileUtils.fileExists(SystemConfig.instance()
                            .getUpdateFolder()))
                    {
                        if (!FileUtils.mkDir(SystemConfig.instance()
                                .getUpdateFolder()))
                        {
                            Logger.log(Logger.SEVERE, this, Utils.format(
                                    UpdaterResource.CANNOT_CREATE_FOLDER
                                            .getValue(),
                                    new String[] {SystemConfig.instance()
                                            .getUpdateFolder()}), null);
                            
                            _response = new UpdaterResponse(
                                    UpdaterResponse.ResponseCode.ERROR, null,
                                    null);
                            
                            return;
                        }
                        
                    }
                    
                    FileUtils.deleteFilesInFolder(SystemConfig.instance()
                            .getUpdateFolder(), "*.*");
                    
                    String fileName = SystemConfig.instance().getUpdateFolder()
                            + response.getFileName();
                    
                    String downloadFileName = SystemConfig.instance()
                            .getUpdateFolder()
                            + FilenameUtils.getBaseName(response.getFileName())
                            + ".tmp";
                    
                    FileUtils.downloadFile(response.getDownloadUrl(),
                            downloadFileName);
                    
                    if (FileUtils.fileExists(downloadFileName))
                    {
                        File dFile = new File(downloadFileName);
                        dFile.renameTo(new File(fileName));
                    }
                    
                    _downloading.set(false);
                    
                    if (FileUtils.fileExists(fileName))
                    {
                        UpdaterResponse newResponse = new UpdaterResponse(
                                UpdaterResponse.ResponseCode.DOWNLOADED,
                                response.getDownloadUrl(),
                                response.getFileName());
                        
                        if (callback != null)
                            callback.doneDownloading(newResponse);
                        
                        _response = newResponse;
                    }
                    else
                    {
                        Logger.log(Logger.SEVERE, this, Utils.format(
                                UpdaterResource.CANNOT_FIND_FILE.getValue(),
                                new String[] {fileName}), null);
                        
                        _response = new UpdaterResponse(
                                UpdaterResponse.ResponseCode.ERROR, null, null);
                    }
                    
                }
                catch (Exception ex)
                {
                    _downloading.set(false);
                    
                    Logger.log(
                            Logger.SEVERE,
                            this,
                            UpdaterResource.ERROR_DOWNLOADING_UPDATE.getValue(),
                            ex);
                    
                    _response = new UpdaterResponse(
                            UpdaterResponse.ResponseCode.ERROR, null, null);
                    
                    if (callback != null)
                        callback.doneDownloading(_response);
                }
                finally
                {
                    _downloading.set(false);
                }
                
            }
        };
        
        if (callback != null)
        {
            Thread thread = new Thread(downloader);
            
            _downloading.set(true);
            _response = new UpdaterResponse(
                    UpdaterResponse.ResponseCode.DOWNLOADING, null, null);
            thread.start();
        }
        else
            downloader.run();
        
    }
    
    /**
     * Gets the response.
     *
     * @return the response
     */
    public UpdaterResponse getResponse()
    {
        if (_downloading.get())
        {
            return new UpdaterResponse(
                    UpdaterResponse.ResponseCode.DOWNLOADING, null, null);
        }
        
        if (_running.get())
        {
            return new UpdaterResponse(UpdaterResponse.ResponseCode.CHECKING,
                    null, null);
        }
        
        return _response;
    }
    
    /**
     * Checks if updater is enabled.
     *
     * @return true, if is enabled
     */
    public boolean isEnabled()
    {
        return !Utils.isNothing(SystemConfig.instance().getSystemProperty(
                SystemConfig.UPDATE_URL));
    }
    
}