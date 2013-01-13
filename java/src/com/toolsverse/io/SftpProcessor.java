/*
 * SftpProcessor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.BufferedInputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.JSch;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.UserInfo;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.IOCase;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * The SFTP implementation of the IoProcessor.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class SftpProcessor implements IoProcessor
{
    
    /**
     * The Class MyUserInfo. Used to store users's credentials. 
     */
    private static class MyUserInfo implements UserInfo
    {
        
        /** The passwd. */
        private String _passwd = null;
        
        /**
         * Instantiates a new MyUserInfo.
         *
         * @param passwd the passwd
         */
        public MyUserInfo(String passwd)
        {
            _passwd = passwd;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#getPassphrase()
         */
        public String getPassphrase()
        {
            return "";
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#getPassword()
         */
        public String getPassword()
        {
            return _passwd;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#promptPassphrase(java.lang.String)
         */
        public boolean promptPassphrase(String message)
        {
            return true;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#promptPassword(java.lang.String)
         */
        public boolean promptPassword(String message)
        {
            return true;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#promptYesNo(java.lang.String)
         */
        public boolean promptYesNo(String str)
        {
            return true;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.jcraft.jsch.UserInfo#showMessage(java.lang.String)
         */
        public void showMessage(String message)
        {
        }
    }
    
    /** SFTP prefix */
    private static final String SFTP = "sftp";
    
    /** default SSH2 PORT. */
    private static final int SSH2_PORT = 22;
    
    /** session. */
    private Session _session;
    
    /** _channel. */
    private ChannelSftp _channel;
    
    /** connected flag. */
    private boolean _connected;
    
    /**
     * Instantiates a new SftpProcessor.
     */
    public SftpProcessor()
    {
        _session = null;
        _channel = null;
        _connected = false;
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
        URL fUrl = null;
        String host = null;
        int port = SSH2_PORT;
        try
        {
            fUrl = new URL(url);
            host = fUrl.getHost();
            port = fUrl.getPort();
            if (port == -1)
                port = SSH2_PORT;
        }
        catch (Exception ex)
        {
            host = url;
        }
        
        JSch jsch = new JSch();
        
        user = !Utils.isNothing(user) ? user : "anonymous";
        password = Utils.makeString(password);
        
        _session = jsch.getSession(user.trim(), host.trim(), port);
        _session.setUserInfo(new MyUserInfo(password.trim()));
        _session.connect();
        
        _channel = (ChannelSftp)_session.openChannel(SFTP);
        _channel.connect();
        
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
        InputStream in = get(fromFolder, filename);
        
        boolean ret = processor.put(toFolder, toFilename, in);
        
        if (ret)
            ret = done();
        
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
        boolean isDir = Utils.isNothing(filename);
        
        String path = isDir ? folder : folder + filename;
        
        if (isDir)
        {
            List<FileResource> list = getList(
                    FileUtils.getUnixFolderName(folder), "*", true);
            
            if (list != null)
                for (FileResource file : list)
                {
                    if (file.isDirectory())
                    {
                        if (!delete(
                                FileUtils.getUnixFolderName(file.getPath()), ""))
                            return false;
                    }
                    else if (!delete(FileUtils.getUnixFolderName(FilenameUtils
                            .getFullPath(file.getPath())), file.getName()))
                        return false;
                }
        }
        
        if (isDir)
            _channel.rmdir(path);
        else
            _channel.rm(path);
        
        return true;
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
            _channel.disconnect();
            _session.disconnect();
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
        InputStream in = get(fromFolder, filename);
        
        boolean ret = processor.put(toFolder, filename, in);
        
        if (ret)
            ret = done();
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#get(java.lang.String,
     * java.lang.String)
     */
    @SuppressWarnings("deprecation")
    public InputStream get(String fromFolder, String filename)
        throws Exception
    {
        String from = FileUtils.getUnixFolderName(fromFolder) + filename;
        
        return new BufferedInputStream(
                _channel.get(from, ChannelSftp.OVERWRITE));
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
        
        try
        {
            out = new FileOutputStream(to);
            
            ret = retrieveFile(fromFolder, filename, out);
            
            if (ret)
                out.flush();
            
        }
        finally
        {
            if (out != null)
                out.close();
        }
        
        return ret;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.io.IoProcessor#getList(java.lang.String,
     * java.lang.String, boolean)
     */
    public List<FileResource> getList(String folder, String filename,
            boolean includeFolder)
        throws Exception
    {
        List<FileResource> remoteFileList = new ArrayList<FileResource>();
        
        folder = !Utils.isNothing(folder) ? folder : "./";
        
        Iterator<?> itr = _channel.ls(folder).iterator();
        
        // JCraft returns the list of files in their "long" (ls -l) format as
        // shown below.
        // So we need to parse it to retrieve just the file name.
        // drwxr-xr-x 4 jshim jshim 4096 May 27 11:27 ..
        // -rw-r--r-- 1 jshim jshim 251 May 27 18:05 myfile.txt
        while (itr.hasNext())
        {
            String longname = itr.next().toString();
            
            boolean isDir = longname.charAt(0) == 'd';
            
            // Ignore directories.
            if (includeFolder || !isDir)
            {
                int idx = longname.lastIndexOf(' ');
                
                if (idx != -1 && idx < longname.length())
                {
                    String shortname = longname.substring(idx + 1);
                    
                    if (Utils.isNothing(shortname)
                            || (isDir && shortname.charAt(0) == '.'))
                        continue;
                    
                    if (FilenameUtils.wildcardMatch(
                            FilenameUtils.getName(shortname), filename,
                            IOCase.INSENSITIVE))
                    {
                        FileResource fileResource = new FileResource();
                        
                        fileResource.setPath(folder + "/" + shortname);
                        fileResource.setName(shortname);
                        fileResource.setIsDirectory(isDir);
                        
                        int size = 0;
                        
                        String[] tokens = Utils.removeWhiteSpace(longname)
                                .split(" ", -1);
                        if (tokens.length > 5)
                            size = Utils.str2Int(tokens[4], 0);
                        
                        if (tokens.length > 8)
                        {
                            String dateStr = tokens[5] + " " + tokens[6] + " "
                                    + tokens[7];
                            
                            Date date = Utils.str2Date(dateStr, new String[] {
                                    "MMM d yyyy", "MMM d HH:mm"});
                            
                            if (date != null)
                            {
                                if (Utils.getDate(date, Calendar.YEAR, null) == 1970)
                                {
                                    date = Utils.setDate(date, Utils.getDate(
                                            new Date(), Calendar.YEAR, null),
                                            Calendar.YEAR, null);
                                }
                                
                                fileResource.setLastModified(date.getTime());
                            }
                        }
                        
                        fileResource.setSize(size);
                        
                        remoteFileList.add(fileResource);
                    }
                }
            }
        }
        
        if (remoteFileList.size() == 0)
            return null;
        
        return remoteFileList;
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
        try
        {
            _channel.mkdir(folder);
            
            return true;
        }
        catch (Exception ex)
        {
            
            Logger.log(Logger.SEVERE, this, Resource.ERROR_GENERAL.getValue(),
                    ex);
            
            return false;
        }
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
        InputStream in = get(fromFolder, filename);
        
        boolean ret = processor.put(toFolder, toFilename, in)
                && delete(fromFolder, filename);
        
        if (ret)
            ret = done();
        
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
            
            ret = put(toFolder, filename, in) && processor.done();
        }
        finally
        {
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
            boolean flag = false;
            String to = FileUtils.getUnixFolderName(toFolder) + filename;
            
            _channel.put(in, to, ChannelSftp.OVERWRITE);
            flag = true;
            
            return flag;
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
        InputStream in = null;
        boolean ret = false;
        String from = FileUtils.getUnixFolderName(fromFolder) + filename;
        
        try
        {
            in = new FileInputStream(from);
            
            ret = put(toFolder, filename, in);
            
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
        _channel.rename(FileUtils.getUnixFolderName(fromFolder) + filename,
                FileUtils.getUnixFolderName(toFolder) + toFilename);
        
        return true;
    }
    
    /**
     * Copies the file fromFolder + filename to the OutputStream.
     *
     * @param fromFolder the from folder
     * @param filename the filename
     * @param outStream the OutputStream
     * @return true, if successful
     * @throws Exception in case of any error
     */
    private boolean retrieveFile(String fromFolder, String filename,
            OutputStream outStream)
        throws Exception
    {
        boolean flag = false;
        BufferedInputStream bufferedInStream = null;
        try
        {
            bufferedInStream = (BufferedInputStream)get(fromFolder, filename);
            
            int aByte = 0;
            while ((aByte = bufferedInStream.read()) != -1)
                outStream.write(aByte);
            
            flag = true;
        }
        finally
        {
            if (bufferedInStream != null)
            {
                try
                {
                    bufferedInStream.close();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.INFO, this,
                            Resource.ERROR_GENERAL.getValue(), ex);
                    
                }
            }
        }
        
        return flag;
    }
}
