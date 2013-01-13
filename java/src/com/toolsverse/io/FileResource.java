/*
 * FileResource.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import java.io.File;
import java.io.Serializable;

/**
 * This class is a substitution for the {@link java.io.File}.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class FileResource implements Serializable
{
    
    /** The path. */
    private String _path;
    
    /** The name. */
    private String _name;
    
    /** is directory flag. */
    private boolean _isDirectory;
    
    /** size. */
    private long _size;
    
    /** last modification time. */
    private long _lastModified;
    
    /**
     * Instantiates a new FileResource.
     */
    public FileResource()
    {
        _path = null;
        _name = null;
        _isDirectory = false;
        _size = 0;
        _lastModified = 0;
    }
    
    /**
     * Instantiates a new FileResource from the file object.
     *
     * @param file the file
     */
    public FileResource(File file)
    {
        _path = file.getPath();
        _name = file.getName();
        _isDirectory = file.isDirectory();
        _size = file.length();
        _lastModified = file.lastModified();
    }
    
    /**
     * Gets the time when file was last modified.
     *
     * @return the time when file was last modified
     */
    public long getLastModified()
    {
        return _lastModified;
    }
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the path.
     *
     * @return the path
     */
    public String getPath()
    {
        return _path;
    }
    
    /**
     * Gets the size.
     *
     * @return the size
     */
    public long getSize()
    {
        return _size;
    }
    
    /**
     * Checks if file is a directory.
     *
     * @return true, if it is a directory
     */
    public boolean isDirectory()
    {
        return _isDirectory;
    }
    
    /**
     * Sets the flag isDirectory.
     *
     * @param value the new value for the isDirectory flag
     */
    public void setIsDirectory(boolean value)
    {
        _isDirectory = value;
    }
    
    /**
     * Sets the last modification time.
     *
     * @param value the new last modification time
     */
    public void setLastModified(long value)
    {
        _lastModified = value;
    }
    
    /**
     * Sets the name.
     *
     * @param value the new name
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the path.
     *
     * @param value the new path
     */
    public void setPath(String value)
    {
        _path = value;
    }
    
    /**
     * Sets the size.
     *
     * @param value the new size
     */
    public void setSize(long value)
    {
        _size = value;
    }
    
}
