/*
 * FileLockerTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.concurrent;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.concurrent.Callable;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.Utils;

/**
 * FileLockerTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class FileLockerTest
{
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    @AfterClass
    public static void tearDown()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "lock*.*");
    }
    
    @Test
    public void testAcquireAndRelease()
        throws Exception
    {
        FileLocker<Object> locker1 = new FileLocker<Object>();
        
        FileLocker<Object> locker2 = new FileLocker<Object>();
        
        String fileName = SystemConfig.instance().getDataFolderName()
                + "lock.txt";
        
        FileOutputStream fout = new FileOutputStream(fileName);
        ObjectOutputStream outputStream = null;
        
        ObjectInputStream inputStream = null;
        
        try
        {
            outputStream = new ObjectOutputStream(fout);
            
            assertTrue(locker1.acquireLock(fout, 0, 0));
            
            outputStream.writeObject("test");
            
            assertTrue(!locker2
                    .acquireLock(new FileInputStream(fileName), 0, 0));
        }
        finally
        {
            assertTrue(locker1.releaseLock());
            
            if (outputStream != null)
                outputStream.close();
            
            try
            {
                FileInputStream fint = new FileInputStream(fileName);
                
                assertTrue(locker2.acquireLock(fint, 0, 0));
                
                inputStream = new ObjectInputStream(fint);
                
                assertTrue("test".equals(inputStream.readObject()));
            }
            finally
            {
                assertTrue(locker2.releaseLock());
                
                if (inputStream != null)
                    inputStream.close();
            }
        }
        
    }
    
    @Test
    public void testCreateAndRelease()
        throws Exception
    {
        FileLocker<Object> locker1 = new FileLocker<Object>();
        
        FileLocker<Object> locker2 = new FileLocker<Object>();
        
        String fileName = SystemConfig.instance().getDataFolderName()
                + "lock.loc";
        
        try
        {
            try
            {
                assertTrue(locker1.createLock(fileName, true));
                
                assertTrue(!locker2.createLock(fileName, 0, 0, false));
            }
            finally
            {
                assertTrue(locker1.releaseLock());
                
                try
                {
                    assertTrue(locker2.createLock(fileName, 0, 0, false));
                }
                finally
                {
                    assertTrue(locker2.releaseLock());
                }
            }
            
        }
        finally
        {
            assertTrue(!FileUtils.fileExists(fileName));
        }
        
    }
    
    @Test
    public void testExecute()
        throws Exception
    {
        final FileLocker<Boolean> locker1 = new FileLocker<Boolean>();
        
        final FileLocker<Object> locker2 = new FileLocker<Object>();
        
        final String fileName = SystemConfig.instance().getDataFolderName()
                + "lock.lck";
        
        Exception exception = null;
        
        try
        {
            locker1.execute(new Callable<Boolean>()
            {
                public Boolean call()
                    throws Exception
                {
                    System.out.println("Executing callable...");
                    
                    assertTrue(!locker2.createLock(fileName, 10, 1, false));
                    
                    throw new Exception("test");
                }
            }, fileName, 10, 1000, true);
        }
        catch (Exception ex)
        {
            exception = ex;
        }
        
        assertNotNull(exception);
        
        try
        {
            assertTrue(locker2.createLock(fileName, 10, 1, false));
        }
        finally
        {
            assertTrue(locker2.releaseLock());
        }
    }
    
    @Test
    public void testGetDefaultLockFileName()
    {
        String fileName = SystemConfig.instance().getDataFolderName()
                + "lock.txt";
        
        assertTrue("txt".equals(FilenameUtils.getExtension(fileName)));
        
        fileName = FileLocker.getDefaultLockFileName(fileName);
        
        assertTrue("lck".equals(FilenameUtils.getExtension(fileName)));
    }
}
