/*
 * IoProcessorTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.io;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.InputStream;
import java.util.List;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;

/**
 * IoProcessorTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class IoProcessorTest
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
                .getDataFolderName(), "test.*");
    }
    
    @Test
    public void testCopy()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = source.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.get(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
            
            destination.copy(destination, SystemConfig.instance()
                    .getDataFolderName(), SystemConfig.instance()
                    .getDataFolderName(), filename, "new_" + filename);
            
            newFile = new File(SystemConfig.instance().getDataFolderName()
                    + "new_" + filename);
            
            assertTrue(newFile.exists());
            
            newFile.delete();
        }
    }
    
    @Test
    public void testDelete()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = source.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.get(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
            
            destination.delete(SystemConfig.instance().getDataFolderName(),
                    filename);
            
            newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(!newFile.exists());
        }
    }
    
    @Test
    public void testGet()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = source.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.get(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
        }
    }
    
    @Test
    public void testGetIoProcessor()
    {
        IoProcessor processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(processor instanceof FileProcessor);
        
        assertTrue(processor.isFileSystem());
        
        processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FTP);
        
        assertTrue(processor instanceof FtpProcessor);
        
        assertTrue(!processor.isFileSystem());
        
        assertTrue(((FtpProcessor)processor).getProxyUrl() == null);
        assertTrue(((FtpProcessor)processor).getProxyPort() == -1);
        
        processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.SFTP);
        
        assertTrue(processor instanceof SftpProcessor);
        
        assertTrue(!processor.isFileSystem());
        
        processor = IoProcessorFactory.instance().getIoProcessor("test");
        
        assertTrue(processor == null);
        
        processor = IoProcessorFactory.instance().getIoProcessor(null);
        
        assertTrue(processor instanceof FileProcessor);
        
        processor = IoProcessorFactory.instance().getIoProcessor("  ");
        
        assertTrue(processor instanceof FileProcessor);
        
        assertTrue(processor instanceof FileProcessor);
        
        processor = IoProcessorFactory.instance().getIoProcessor();
        
        assertTrue(processor instanceof FileProcessor);
        
        processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FTP, "test", 122);
        
        assertTrue(processor instanceof FtpProcessor);
        
        assertTrue("test".equals(((FtpProcessor)processor).getProxyUrl()));
        assertTrue(((FtpProcessor)processor).getProxyPort() == 122);
    }
    
    @Test
    public void testGetList()
        throws Exception
    {
        IoProcessor processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(processor instanceof FileProcessor);
        
        List<FileResource> list = processor.getList(SystemConfig.instance()
                .getHome(), "*", true);
        
        assertNotNull(list);
        
        assertTrue(list.size() > 0);
        
        boolean hasFiles = false;
        boolean hasFolders = false;
        
        for (FileResource file : list)
        {
            hasFiles = hasFiles || !file.isDirectory();
            hasFolders = hasFolders || file.isDirectory();
        }
        
        assertTrue(hasFiles);
        assertTrue(hasFolders);
        
        list = processor.getList("fdjlkfjslfdsj", "*", true);
        
        assertTrue(list == null);
    }
    
    @Test
    public void testGetResource()
        throws Exception
    {
        IoProcessor processor = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(processor instanceof FileProcessor);
        
        List<FileResource> list = processor.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            InputStream input = null;
            
            try
            {
                input = processor.get(SystemConfig.instance().getHome(),
                        file.getName());
                
                assertNotNull(input);
                
                assertTrue(input.available() > 0);
            }
            finally
            {
                if (input != null)
                    input.close();
            }
        }
    }
    
    @Test
    public void testMove()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = source.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.get(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
            
            destination.move(destination, SystemConfig.instance()
                    .getDataFolderName(), SystemConfig.instance()
                    .getDataFolderName(), filename, "new_" + filename);
            
            newFile = new File(SystemConfig.instance().getDataFolderName()
                    + "new_" + filename);
            
            assertTrue(newFile.exists());
            
            newFile.delete();
        }
    }
    
    @Test
    public void testPut()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = destination.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.put(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
        }
    }
    
    @Test
    public void testRename()
        throws Exception
    {
        FileUtils.deleteFilesInFolder(SystemConfig.instance()
                .getDataFolderName(), "test.*");
        
        IoProcessor source = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(source instanceof FileProcessor);
        
        IoProcessor destination = IoProcessorFactory.instance().getIoProcessor(
                IoProcessorFactory.FILE);
        
        assertTrue(destination instanceof FileProcessor);
        
        List<FileResource> list = source.getList(SystemConfig.instance()
                .getHome(), "test.*", false);
        
        for (FileResource file : list)
        {
            String filename = file.getName();
            
            source.get(destination, SystemConfig.instance().getHome(),
                    SystemConfig.instance().getDataFolderName(), filename);
            
            File newFile = new File(SystemConfig.instance().getDataFolderName()
                    + filename);
            
            assertTrue(newFile.exists());
            
            destination.rename(SystemConfig.instance().getDataFolderName(),
                    SystemConfig.instance().getDataFolderName(), filename,
                    "new_" + filename);
            
            newFile = new File(SystemConfig.instance().getDataFolderName()
                    + "new_" + filename);
            
            assertTrue(newFile.exists());
            
            newFile.delete();
        }
    }
    
}
