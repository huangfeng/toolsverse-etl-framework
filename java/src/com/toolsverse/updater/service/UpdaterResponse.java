/*
 * UpdaterResponse.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.updater.service;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

/**
 * The {@link UpdaterService} returns instance of the UpdaterResponse.  
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class UpdaterResponse implements Serializable
{
    
    /**
     * updater response codes.
     */
    public static enum ResponseCode
    {
        /** error. */
        ERROR,
        /** up to date. */
        UP_TO_DATE,
        /** ready to download. */
        READY_TO_DOWNLOAD,
        /** no service. */
        NO_SERVICE,
        /** checking. */
        CHECKING,
        /** downloading. */
        DOWNLOADING,
        /** The downloaded. */
        DOWNLOADED,
    }
    
    /** The codes. */
    public static Map<ResponseCode, String> CODES = new HashMap<ResponseCode, String>();
    static
    {
        CODES.put(ResponseCode.ERROR, "");
        CODES.put(ResponseCode.UP_TO_DATE, "up to date");
        CODES.put(ResponseCode.READY_TO_DOWNLOAD, "new version available");
        CODES.put(ResponseCode.NO_SERVICE, "");
        CODES.put(ResponseCode.CHECKING, "checking for update");
        CODES.put(ResponseCode.DOWNLOADING, "downloading update");
        CODES.put(ResponseCode.DOWNLOADED,
                "update downloaded, restart app to apply");
    }
    
    /** The response code. */
    private final ResponseCode _responseCode;
    
    /** The download url. */
    private final String _downloadUrl;
    
    /** The file name. */
    private final String _fileName;
    
    /**
     * Instantiates a new updater response.
     *
     * @param responseCode the response code
     * @param downloadUrl the download url
     * @param fileName the file name
     */
    public UpdaterResponse(ResponseCode responseCode, String downloadUrl,
            String fileName)
    {
        _responseCode = responseCode;
        
        _downloadUrl = downloadUrl;
        
        _fileName = fileName;
    }
    
    /**
     * Gets the download url.
     *
     * @return the download url
     */
    public String getDownloadUrl()
    {
        return _downloadUrl;
    }
    
    public String getFileName()
    {
        return _fileName;
    }
    
    /**
     * Gets the response code.
     *
     * @return the response code
     */
    public ResponseCode getResponseCode()
    {
        return _responseCode;
    }
}