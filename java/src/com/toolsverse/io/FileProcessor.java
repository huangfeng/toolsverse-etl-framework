/*
 * FileProcessor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.List;

import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * The local file system implementation of the IoProcessor.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class FileProcessor implements IoProcessor
{
    
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
        if (processor instanceof FileProcessor)
        {
            File fileFrom = new File(FileUtils.getUnixFolderName(fromFolder)
                    + filename);
            File fileTo = new File(FileUtils.getUnixFolderName(toFolder)
                    + toFilename);
            
            FileUtils.copyFile(fileFrom, fileTo);
            
            return fileTo.exists();
        }
        else
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
                    in.close();
                
                if (ret)
                    ret = done();
            }
            
            return ret;
        }
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
        return FileUtils.deleteFile(FileUtils.getUnixFolderName(folder)
                + filename);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#disconnect()
     */
    public void disconnect()
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#done()
     */
    public boolean done()
        throws Exception
    
    {
        return true;
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
                in.close();
            
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
        return new FileInputStream(FileUtils.getUnixFolderName(fromFolder)
                + filename);
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
        File in = new File(FileUtils.getUnixFolderName(fromFolder) + filename);
        File out = new File(FileUtils.getUnixFolderName(toFolder) + filename);
        
        FileUtils.copyFile(in, out);
        
        return out.exists();
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
        folder = !Utils.isNothing(folder) ? folder : "./";
        
        File[] files = (FileUtils.getFilesInFolder(folder, filename));
        
        if (files != null && files.length > 0)
        {
            List<FileResource> fileList = new ArrayList<FileResource>();
            
            for (File file : files)
            {
                boolean isDir = file.isDirectory();
                
                if (includeFolders || !isDir)
                {
                    FileResource fileResource = new FileResource(file);
                    
                    fileList.add(fileResource);
                }
            }
            
            return fileList;
        }
        else
            return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#isFileSystem()
     */
    public boolean isFileSystem()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#mkDir(java.lang.String)
     */
    public boolean mkDir(String folder)
        throws Exception
    {
        File file = new File(folder);
        
        if (file.exists())
            return false;
        
        return file.mkdirs();
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
        if (processor instanceof FileProcessor)
        {
            File fileFrom = new File(FileUtils.getUnixFolderName(fromFolder)
                    + filename);
            File fileTo = new File(FileUtils.getUnixFolderName(toFolder)
                    + toFilename);
            
            return fileFrom.renameTo(fileTo);
        }
        else
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
                    in.close();
                
                if (ret)
                    ret = done();
            }
            
            return ret;
        }
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
        try
        {
            File out = new File(FileUtils.getUnixFolderName(toFolder)
                    + filename);
            
            FileUtils.copyFile(in, out);
            
            return out.exists();
        }
        finally
        {
            if (in != null)
                in.close();
        }
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
        return get(fromFolder, toFolder, filename);
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
        File fileFrom = new File(FileUtils.getUnixFolderName(fromFolder)
                + filename);
        File fileTo = new File(FileUtils.getUnixFolderName(toFolder)
                + toFilename);
        
        return fileFrom.renameTo(fileTo);
    }
}
