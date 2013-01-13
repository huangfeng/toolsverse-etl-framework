/*
 * FtpProcessor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.List;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.log.Logger;

/**
 * The FTP implementation of the IoProcessor. Supports socket proxy.
 *  
 * @see com.toolsverse.io.ProxyFtpClient
 * @see com.toolsverse.io.FtpUtils
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class FtpProcessor implements IoProcessor
{
    
    private FtpUtils _ftpUtils;
    
    /** The proxy url. */
    private String _proxyUrl;
    
    /** The proxy port. */
    private int _proxyPort;
    
    /** connected flag. */
    private boolean _connected;
    
    /**
     * Instantiates a new FtpProcessor.
     */
    public FtpProcessor()
    {
        this(null, -1);
    }
    
    /**
     * Instantiates a new FtpProcessor with a socket proxy. 
     *
     * @param proxyUrl the socket proxy url
     * @param proxyPort the socket proxy port
     */
    public FtpProcessor(String proxyUrl, int proxyPort)
    {
        _proxyUrl = proxyUrl;
        _proxyPort = proxyPort;
        
        _connected = false;
        
        _ftpUtils = getFtpUtils(proxyUrl, proxyPort);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#connect(java.lang.String,
     * java.lang.String, java.lang.String, boolean)
     */
    public void connect(String url, String user, String password,
            boolean passiveMode)
        throws Exception
    {
        getFtpUtils().connect(url, user, password, passiveMode);
        
        _connected = true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#copy(com.toolsverse.io.IoProcessor,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean copy(IoProcessor processor, String fromFolder,
            String toFolder, String filename, String toFilename)
        throws Exception
    {
        InputStream in = null;
        
        boolean ret = false;
        
        try
        {
            in = get(fromFolder, filename);
            
            ret = processor.put(toFolder, toFilename, in);
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
            
            if (ret)
                ret = done();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#delete(java.lang.String,
     * java.lang.String)
     */
    public boolean delete(String folder, String filename)
        throws Exception
    {
        return getFtpUtils().deleteFile(FileUtils.getUnixFolderName(folder),
                filename);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#disconnect()
     */
    public void disconnect()
    {
        if (!_connected)
            return;
        
        try
        {
            getFtpUtils().disconnect();
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
        }
        
        _connected = false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#done()
     */
    public boolean done()
        throws Exception
    {
        return _ftpUtils.done();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#get(com.toolsverse.io.IoProcessor,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean get(IoProcessor processor, String fromFolder,
            String toFolder, String filename)
        throws Exception
    {
        InputStream in = null;
        boolean ret = false;
        
        try
        {
            in = get(fromFolder, filename);
            
            ret = processor.put(toFolder, filename, in);
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
            
            if (ret)
                ret = done();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#get(java.lang.String,
     * java.lang.String)
     */
    public InputStream get(String fromFolder, String filename)
        throws Exception
    {
        return getFtpUtils().retrieveFile(
                FileUtils.getUnixFolderName(fromFolder), filename);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#get(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    public boolean get(String fromFolder, String toFolder, String filename)
        throws Exception
    {
        OutputStream out = null;
        boolean ret = false;
        
        String to = FileUtils.getUnixFolderName(toFolder) + filename;
        File toFile = new File(to);
        
        try
        {
            out = new FileOutputStream(to);
            
            ret = getFtpUtils().retrieveFile(
                    FileUtils.getUnixFolderName(fromFolder), filename, out);
            
            if (ret)
                out.flush();
            else
                toFile.delete();
        }
        finally
        {
            if (out != null)
                out.close();
        }
        
        return toFile.exists();
    }
    
    /**
     * Gets the FtpUtils.
     *
     * @return the FtpUtils
     */
    private FtpUtils getFtpUtils()
    {
        if (_ftpUtils == null)
            _ftpUtils = new FtpUtils();
        
        return _ftpUtils;
    }
    
    /**
     * Gets the FtpUtils with a socket proxy.
     *
     * @param proxy the socket proxy url
     * @param proxyPort the socket proxy port
     * @return the FtpUtils
     */
    private FtpUtils getFtpUtils(String proxy, int proxyPort)
    {
        if (_ftpUtils == null)
            _ftpUtils = new FtpUtils(proxy, proxyPort);
        
        return _ftpUtils;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#getList(java.lang.String,
     * java.lang.String, boolean)
     */
    public List<FileResource> getList(String folder, String filename,
            boolean includeFolders)
        throws Exception
    {
        return getFtpUtils().getFileList(folder, filename, includeFolders);
    }
    
    /**
     * Gets the socket proxy port.
     *
     * @return the socket proxy port
     */
    public int getProxyPort()
    {
        return _proxyPort;
    }
    
    /**
     * Gets the socket proxy url.
     *
     * @return the socket proxy url
     */
    public String getProxyUrl()
    {
        return _proxyUrl;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#isFileSystem()
     */
    public boolean isFileSystem()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#mkDir(java.lang.String)
     */
    public boolean mkDir(String folder)
        throws Exception
    {
        return getFtpUtils().mkDir(folder);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#move(com.toolsverse.io.IoProcessor,
     * java.lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean move(IoProcessor processor, String fromFolder,
            String toFolder, String filename, String toFilename)
        throws Exception
    {
        InputStream in = null;
        boolean ret = false;
        
        try
        {
            in = get(fromFolder, filename);
            
            ret = processor.put(toFolder, toFilename, in);
            
            ret = ret && delete(fromFolder, filename);
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
            
            if (ret)
                ret = done();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#put(com.toolsverse.io.IoProcessor,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean put(IoProcessor processor, String fromFolder,
            String toFolder, String filename)
        throws Exception
    {
        InputStream in = null;
        boolean ret = false;
        
        try
        {
            in = processor.get(fromFolder, filename);
            
            ret = put(toFolder, filename, in);
            
        }
        finally
        {
            if (in != null)
            {
                in.close();
            }
            
            if (ret)
                ret = processor.done();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#put(java.lang.String,
     * java.lang.String, java.io.InputStream)
     */
    public boolean put(String toFolder, String filename, InputStream in)
        throws Exception
    {
        return getFtpUtils().storeFile(FileUtils.getUnixFolderName(toFolder),
                filename, in);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#put(java.lang.String,
     * java.lang.String, java.lang.String)
     */
    public boolean put(String fromFolder, String toFolder, String filename)
        throws Exception
    {
        InputStream in = null;
        boolean ret = false;
        
        String from = FileUtils.getUnixFolderName(fromFolder) + filename;
        
        try
        {
            in = new FileInputStream(from);
            
            ret = getFtpUtils().storeFile(
                    FileUtils.getUnixFolderName(toFolder), filename, in);
        }
        finally
        {
            if (in != null)
                in.close();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#rename(java.lang.String,
     * java.lang.String, java.lang.String, java.lang.String)
     */
    public boolean rename(String fromFolder, String toFolder, String filename,
            String toFilename)
        throws Exception
    {
        return getFtpUtils().rename(fromFolder, toFolder, filename, toFilename);
    }
    
}
