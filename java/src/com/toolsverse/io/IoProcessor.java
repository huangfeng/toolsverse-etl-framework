/*
 * IoProcessor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.InputStream;
import java.util.List;

/**
 * The collection of basic IO methods, such as connect, copy, delete, mkdir, etc. 
 * The particular implementations of this interface are: FileProcessor, FtpProcessor and SftpProcessor.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface IoProcessor
{
    
    /**
     * Connects to the remote resource. 
     *
     * @param url the url
     * @param user the user
     * @param password the password
     * @param passiveMode the passive mode flag
     * @throws Exception in case of any error
     */
    void connect(String url, String user, String password, boolean passiveMode)
        throws Exception;
    
    /**
     * Copies fromFolder + filename file to the toFolder + toFilename using provided destination processor. For example it is possible to copy a file from
     * the local files system (FileProcessor) to the FTP (FtpProcessor).
     *
     * @param processor the destination IoProcessor
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @param toFilename the to filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean copy(IoProcessor processor, String fromFolder, String toFolder,
            String filename, String toFilename)
        throws Exception;
    
    /**
     * Deletes a file folder + filename.
     *
     * @param folder the folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean delete(String folder, String filename)
        throws Exception;
    
    /**
     * Disconnects from the remore resource.
     */
    void disconnect();
    
    /**
     * Executed when IO command is finished.
     *
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean done()
        throws Exception;
    
    /**
     * Copies a file fromFolder + filename to the toFolder + filename using client as a source IoProcessor.
     *
     * @param client the source IoProcessor
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean get(IoProcessor client, String fromFolder, String toFolder,
            String filename)
        throws Exception;
    
    /**
     * Copies file fromFolder + filename to the InputStream.
     *
     * @param fromFolder the from folder
     * @param filename the filename
     * @return the InputStream
     * @throws Exception in case of any error
     */
    InputStream get(String fromFolder, String filename)
        throws Exception;
    
    /**
     * Copies file fromFolder + filename to the toFolder + filename using itself as a source IoProcessor.
     *
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean get(String fromFolder, String toFolder, String filename)
        throws Exception;
    
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
    List<FileResource> getList(String folder, String filename,
            boolean includeFolders)
        throws Exception;
    
    /**
     * Checks if IoProcessor operates on a local file system.
     *
     * @return true, if IoProcessor operates on a local file system
     */
    boolean isFileSystem();
    
    /**
     * Creates a folder.
     *
     * @param folder the folder to create
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean mkDir(String folder)
        throws Exception;
    
    /**
     * Moves a file fromFolder + filename to the toFolder + filename using processor as a destination IoProcessor.
     *
     * @param processor the destination IoProcessor
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @param toFilename the to filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean move(IoProcessor processor, String fromFolder, String toFolder,
            String filename, String toFilename)
        throws Exception;
    
    /**
     * Copies file fromFolder + filename to the toFolder + filename using client as a designation IoProcessor.
     *
     * @param client the designation IoProcessor
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean put(IoProcessor client, String fromFolder, String toFolder,
            String filename)
        throws Exception;
    
    /**
     * Creates a file toFolder + filename from the InputStream in
     *
     * @param toFolder the to folder
     * @param filename the filename
     * @param in the InputStream
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean put(String toFolder, String filename, InputStream in)
        throws Exception;
    
    /**
     * Copies a file fromFolder + filename to the toFolder + toFolder using itself as a destination IoProcessor.
     *
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean put(String fromFolder, String toFolder, String filename)
        throws Exception;
    
    /**
     * Renames a file fromFolder + filename to toFolder + toFilename.
     *
     * @param fromFolder the from folder
     * @param toFolder the to folder
     * @param filename the filename
     * @param toFilename the to filename
     * @return true, if successful
     * @throws Exception in case of any error
     */
    boolean rename(String fromFolder, String toFolder, String filename,
            String toFilename)
        throws Exception;
}
