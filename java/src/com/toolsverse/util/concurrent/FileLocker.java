/*
 * FileLocker.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.util.concurrent.Callable;

import com.toolsverse.util.FileUtils;
import com.toolsverse.util.log.Logger;

/**
 * The class for acquiring and releasing os level file locks. Use it only for one lock at the time.
 *
 * @param <R> the generic type. 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class FileLocker<R>
{
    
    /** The default number of retries. */
    private static final int DEFAULT_RETRY = 10;
    
    /** The default sleep time between retries. */
    private static final int DEFAULT_SLEEP_TIME = 1000;
    
    /**
     * Gets the default lock file name using given file name.
     * 
     * <br>
     * <br>
     * Example: c:\abc\x.txt -> c:\abc\x.lck 
     * 
     * @param fileName the file name
     * @return the default lock file name
     */
    public static String getDefaultLockFileName(String fileName)
    {
        
        return FileUtils.getFilename(fileName, null, ".lck", true);
        
    }
    
    /** The _lock. */
    private FileLock _lock;
    
    /** The file channel. */
    private FileChannel _channel;
    
    /** The _file name. */
    private String _fileName;
    
    /**
     * Instantiates a new file locker.
     */
    public FileLocker()
    {
        _lock = null;
        
        _channel = null;
        
        _fileName = null;
    }
    
    /**
     * Locks the given file input stream. Uses {@link #DEFAULT_RETRY} for the number of retries and
     *
     * @param in the the file input stream
     * @return true, if successful
     * {@link #DEFAULT_SLEEP_TIME} for the sleep time between retries.
     */
    public boolean acquireLock(FileInputStream in)
    {
        try
        {
            _channel = in.getChannel();
            
            return acquireLockForChannel(DEFAULT_RETRY, DEFAULT_SLEEP_TIME,
                    false);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
            
        }
    }
    
    /**
     * Locks the given file input stream.
     *
     * @param in the the file input stream
     * @param retry the number of retries
     * @param sleepTime the sleep time between retries
     * @return true, if successful
     */
    public boolean acquireLock(FileInputStream in, int retry, int sleepTime)
    {
        try
        {
            _channel = in.getChannel();
            
            return acquireLockForChannel(retry, sleepTime, false);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
            
        }
    }
    
    /**
     * Locks the given file output stream. Uses {@link #DEFAULT_RETRY} for the number of retries and
     *
     * @param out the the file output stream
     * @return true, if successful
     * {@link #DEFAULT_SLEEP_TIME} for the sleep time between retries.
     */
    public boolean acquireLock(FileOutputStream out)
    {
        try
        {
            _channel = out.getChannel();
            
            return acquireLockForChannel(DEFAULT_RETRY, DEFAULT_SLEEP_TIME,
                    true);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
            
        }
    }
    
    /**
     * Locks the given file output stream.
     *
     * @param out the the file output stream
     * @param retry the number of retries
     * @param sleepTime the sleep time between retries
     * @return true, if successful
     */
    public boolean acquireLock(FileOutputStream out, int retry, int sleepTime)
    {
        try
        {
            _channel = out.getChannel();
            
            return acquireLockForChannel(retry, sleepTime, true);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
            
        }
    }
    
    /**
     * Acquires lock for the file channel.
     *
     * @param retry the retry
     * @param sleepTime the sleep time
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return true, if successful
     * @throws Exception the exception
     */
    private boolean acquireLockForChannel(int retry, int sleepTime,
            boolean exclusive)
        throws Exception
    {
        _lock = lock(_channel, exclusive);
        
        if (_lock != null)
            return true;
        
        for (int i = 0; i < retry; i++)
        {
            _lock = lock(_channel, exclusive);
            
            if (_lock != null)
                return true;
            
            Thread.sleep(sleepTime);
        }
        
        if (_channel != null)
            try
            {
                _channel.close();
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, this,
                        ConcurrentResource.ERROR_CLOSING_CHANNEL.getValue(), ex);
            }
        
        return false;
    }
    
    /**
     * Creates a locks file with the given name. Uses {@link #DEFAULT_RETRY} for the number of retries and
     *
     * @param fileName the file name
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return true, if successful
     * {@link #DEFAULT_SLEEP_TIME} for the sleep time between retries.
     */
    public boolean createLock(String fileName, boolean exclusive)
    {
        return createLock(fileName, DEFAULT_RETRY, DEFAULT_SLEEP_TIME,
                exclusive);
    }
    
    /**
     * Creates a locks file with the given name.
     *
     * @param fileName the file name
     * @param retry the number of retries
     * @param sleepTime the sleep time between retries
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return true, if successful
     */
    @SuppressWarnings("resource")
    public boolean createLock(String fileName, int retry, int sleepTime,
            boolean exclusive)
    {
        RandomAccessFile randomAccessFile = null;
        
        try
        {
            randomAccessFile = new RandomAccessFile(fileName, "rw");
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
        }
        
        try
        {
            _channel = randomAccessFile.getChannel();
            
            _fileName = fileName;
            
            return acquireLockForChannel(retry, sleepTime, exclusive);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_AQUIRING_LOCK.getValue(), ex);
            
            return false;
            
        }
    }
    
    /**
     * Executes the {@link Callable#call()} method. If <code>fileName != null</code> crates a lock file
     * with the given name before and deletes it after. Uses {@link #DEFAULT_RETRY} for the number of retries and {@link #DEFAULT_SLEEP_TIME}
     * for the sleep time between retries.
     *
     * @param callable the instance of the Callable interface
     * @param fileName the lock file name
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return the instance of the generic type R
     * @throws Exception the exception in case of any error
     */
    public R execute(Callable<R> callable, String fileName, boolean exclusive)
        throws Exception
    {
        return execute(callable, fileName, DEFAULT_RETRY, DEFAULT_SLEEP_TIME,
                exclusive);
        
    }
    
    /**
     * Executes the {@link Callable#call()} method. If <code>fileName != null</code> crates a lock file
     * with the given name before and deletes it after.
     *
     * @param callable the instance of the Callable interface
     * @param fileName the lock file name
     * @param retry the number of retries
     * @param sleepTime the sleep time between retries
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return the instance of the generic type R
     * @throws Exception the exception in case of any error
     */
    public R execute(Callable<R> callable, String fileName, int retry,
            int sleepTime, boolean exclusive)
        throws Exception
    {
        if (fileName == null)
            return callable.call();
        else
        {
            boolean locked = createLock(fileName, retry, sleepTime, exclusive);
            
            if (locked)
                try
                {
                    return callable.call();
                }
                finally
                {
                    releaseLock();
                }
            else
            {
                throw new Exception(
                        ConcurrentResource.ERROR_AQUIRING_LOCK.getValue());
            }
            
        }
    }
    
    /**
     * Creates a file lock using given file channel.
     *
     * @param channel the file channel
     * @param exclusive if <code>true</code> try to acquire exclusive lock
     * @return the file lock
     */
    private FileLock lock(FileChannel channel, boolean exclusive)
    {
        try
        {
            return channel.tryLock(0L, Long.MAX_VALUE, !exclusive);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.INFO, this,
                    ConcurrentResource.CANNOT_AQUIRE_LOCK.getValue());
            
            return null;
        }
    }
    
    /**
     * Releases current lock.
     *
     * @return true, if successful
     */
    public boolean releaseLock()
    {
        try
        {
            if (_lock != null)
                _lock.release();
            
            return true;
        }
        catch (Exception ex)
        {
            Logger.log(Logger.SEVERE, this,
                    ConcurrentResource.ERROR_RELEASING_LOCK.getValue(), ex);
            
            return false;
        }
        finally
        {
            _lock = null;
            
            if (_channel != null)
                try
                {
                    _channel.close();
                }
                catch (IOException ex)
                {
                    Logger.log(
                            Logger.SEVERE,
                            this,
                            ConcurrentResource.ERROR_CLOSING_CHANNEL.getValue(),
                            ex);
                }
            
            _channel = null;
            
            if (_fileName != null)
                new File(_fileName).delete();
            
            _fileName = null;
        }
    }
}
