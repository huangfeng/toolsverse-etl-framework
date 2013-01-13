/*
 * FtpUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.net.ftp.FTP;
import org.apache.commons.net.ftp.FTPClient;
import org.apache.commons.net.ftp.FTPFile;
import org.apache.commons.net.ftp.FTPReply;

import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.IOCase;
import com.toolsverse.util.Utils;

/**
 * The collection of methods which implement FTP protocol using org.apache.commons.net.ftp.
 *
 * @see org.apache.commons.net.ftp.FTPClient
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.5
 */

public class FtpUtils
{
    
    /** default FTP PORT. */
    private static final int FTP_PORT = 21;
    
    /** FTPClient */
    private FTPClient ftpClient = null;
    
    /**
     * Instantiates a new FtpUtils.
     */
    public FtpUtils()
    {
        ftpClient = new ProxyFtpClient(null, -1);
    }
    
    /**
     * Instantiates a new FtpUtils with a socket proxy using provided proxy host and port.
     *
     * @param proxyHost the socket proxy host
     * @param proxyPort the socket proxy port
     */
    public FtpUtils(String proxyHost, int proxyPort)
    {
        ftpClient = new ProxyFtpClient(proxyHost, proxyPort);
    }
    
    /**
     * Connects to the FTP server.
     *
     * @param urlStr the url
     * @param loginId the login id
     * @param loginPswd the login ppassword
     * @param passiveMode the passive mode flag
     * @throws Exception in case of any error
     */
    public void connect(String urlStr, String loginId, String loginPswd,
            boolean passiveMode)
        throws Exception
    {
        boolean success = false;
        
        URL url = new URL(urlStr);
        String host = url.getHost();
        int port = url.getPort();
        if (port == -1)
            port = FTP_PORT;
        
        ftpClient.connect(host, port);
        
        int reply = ftpClient.getReplyCode();
        if (FTPReply.isPositiveCompletion(reply))
        {
            success = ftpClient.login(!Utils.isNothing(loginId) ? loginId
                    : "anonymous", Utils.makeString(loginPswd));
            
            if (!success)
                disconnect();
            else
            {
                ftpClient.setFileType(FTP.BINARY_FILE_TYPE);
                if (passiveMode)
                    ftpClient.enterLocalPassiveMode();
            }
        }
    }
    
    /**
     * Deletes file folder + filename.
     *
     * @param folder the folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean deleteFile(String folder, String filename)
        throws Exception
    {
        boolean isDir = Utils.isNothing(filename);
        
        String path = isDir ? folder : folder + filename;
        
        if (isDir)
        {
            List<FileResource> list = getFileList(
                    FileUtils.getUnixFolderName(folder), "*", true);
            
            if (list != null)
                for (FileResource file : list)
                {
                    if (file.isDirectory())
                    {
                        if (!deleteFile(
                                FileUtils.getUnixFolderName(file.getPath()), ""))
                            return false;
                    }
                    else if (!deleteFile(
                            FileUtils.getUnixFolderName(FilenameUtils
                                    .getFullPath(file.getPath())),
                            file.getName()))
                        return false;
                }
            
        }
        
        if (isDir)
            return ftpClient.removeDirectory(path);
        else
            return ftpClient.deleteFile(path);
    }
    
    /**
     * Disconnects from the FTP server.
     *
     * @throws Exception in case of any error
     */
    public void disconnect()
        throws Exception
    {
        ftpClient.logout();
        if (ftpClient.isConnected())
            ftpClient.disconnect();
    }
    
    /**
     * Executed when IO command is finished. Internally executes completePendingCommand.
     *
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean done()
        throws Exception
    {
        return ftpClient.completePendingCommand();
    }
    
    /**
     * Gets the list of FileResource objects from the folder + filename. filename can be a mask, for example: /usr/test/*.txt. 
     * If includeFolders == true recursively includes sub-folders. 
     *
     * @see com.toolsverse.io.FileResource
     *
     * @param folder the folder
     * @param filename the filename
     * @param includeFolders the include folders flag. If equals to true recursively includes sub-folders
     * @return the list of FileResource objects
     * @throws Exception in case of any error
     */
    public List<FileResource> getFileList(String folder, String filename,
            boolean includeFolders)
        throws Exception
    {
        List<FileResource> remoteFileList = new ArrayList<FileResource>();
        
        folder = !Utils.isNothing(folder) ? folder : ".";
        
        FTPFile[] files = ftpClient.listFiles(folder);
        
        if (files == null || files.length == 0)
            return null;
        
        for (FTPFile file : files)
        {
            boolean isDir = file.isDirectory();
            
            if ((includeFolders || !isDir)
                    && FilenameUtils.wildcardMatch(file.getName(), filename,
                            IOCase.INSENSITIVE))
            {
                FileResource fileResource = new FileResource();
                
                if (Utils.isNothing(file.getName())
                        || "..".equalsIgnoreCase(file.getName())
                        || ".".equalsIgnoreCase(file.getName()))
                    continue;
                
                fileResource.setPath(folder + "/" + file.getName());
                fileResource.setName(file.getName());
                fileResource.setIsDirectory(isDir);
                fileResource.setSize(!isDir ? file.getSize() : 0);
                fileResource.setLastModified(file.getTimestamp() != null ? file
                        .getTimestamp().getTimeInMillis() : 0);
                
                remoteFileList.add(fileResource);
            }
        }
        
        return remoteFileList;
    }
    
    /**
     * Gets the FTPClient.
     *
     * @return the FTPClient
     */
    public FTPClient getFTPClient()
    {
        return ftpClient;
    }
    
    /**
     * Creates a folder.
     *
     * @param folder the folder
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean mkDir(String folder)
        throws Exception
    {
        return ftpClient.makeDirectory(folder);
    }
    
    /**
     * Renames file fromFolder + filename to toFolder + toFilename.
     *
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @param toFilename the to filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean rename(String fromFolder, String toFolder, String filename,
            String toFilename)
        throws Exception
    {
        return ftpClient.rename(FileUtils.getUnixFolderName(fromFolder)
                + filename, FileUtils.getUnixFolderName(toFolder) + toFilename);
    }
    
    /**
     * Reads a file pickupFolder + filename and returns it as a InputStream.
     *
     * @param pickupFolder the pickup folder
     * @param filename the filename
     * @return the InputStream
     * @throws Exception in case of any error
     */
    public InputStream retrieveFile(String pickupFolder, String filename)
        throws Exception
    {
        InputStream input = ftpClient.retrieveFileStream(pickupFolder
                + filename);
        
        return input;
    }
    
    /**
     * Writes a file pickupFolder + filename into OutputStream.
     *
     * @param pickupFolder the pickup folder
     * @param filename the filename
     * @param out the OutputStream
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean retrieveFile(String pickupFolder, String filename,
            OutputStream out)
        throws Exception
    {
        String remoteFilePath = pickupFolder + filename;
        
        return ftpClient.retrieveFile(remoteFilePath, out);
    }
    
    /**
     * Stores a file on the server using name dropOffFolder + filename and taking input from the given InputStream. 
     *
     * @param dropOffFolder the drop off folder
     * @param filename the filename
     * @param in the InputStream
     * @return true, if successful
     * @throws Exception in case of any error
     */
    public boolean storeFile(String dropOffFolder, String filename,
            InputStream in)
        throws Exception
    {
        String remoteFilePath = dropOffFolder + filename;
        
        return ftpClient.storeFile(remoteFilePath, in);
    }
}
