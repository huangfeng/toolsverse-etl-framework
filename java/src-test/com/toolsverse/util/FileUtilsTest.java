/*
 * FileUtilsTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import static org.junit.Assert.assertTrue;

import java.io.File;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;

/**
 * FileUtilsTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class FileUtilsTest
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
    
    @Test
    public void testChangeExt()
    {
        assertTrue(FileUtils.changeExt(null, "abc") == null);
        assertTrue("abc".equals(FileUtils.changeExt("abc.txt", null)));
        assertTrue(FileUtils.changeExt(null, null) == null);
        assertTrue("c:/temp/fld/xyz.xml".equals(FileUtils.changeExt(
                "c:/temp/fld/xyz.txt", "xml")));
        assertTrue("xyz.xml".equals(FileUtils.changeExt("xyz.txt", "xml")));
        assertTrue("xyz.xml".equals(FileUtils.changeExt("xyz", "xml")));
    }
    
    @Test
    public void testChangeFileName()
    {
        assertTrue("abc".equals(FileUtils.changeFileName(null, "abc")));
        assertTrue("abc".equals(FileUtils.changeFileName("abc", null)));
        assertTrue(FileUtils.changeFileName(null, null) == null);
        assertTrue("c:/temp/fld/abc.txt".equals(FileUtils.changeFileName(
                "c:/temp/fld/xyz.txt", "abc")));
        assertTrue("abc.txt".equals(FileUtils.changeFileName("xyz.txt", "abc")));
        assertTrue("abc".equals(FileUtils.changeFileName("xyz", "abc")));
    }
    
    @Test
    public void testFullNameName()
    {
        assertTrue("c:/temp/test.xml".equals(FileUtils.getFullFileName(
                "c:\\temp", "test", ".xml", true)));
        assertTrue("c:\\temp\\test.xml".equals(FileUtils.getFullFileName(
                "c:\\temp\\", "test", ".xml", false)));
    }
    
    @Test
    public void testGetFilename()
    {
        assertTrue(FilenameUtils.normalize("c:/test/test.xml").equals(
                FilenameUtils.normalize(FileUtils.getFilename(
                        "c:/test/test.xml", null, ".xml", false))));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.dat").equals(
                FilenameUtils.normalize(FileUtils.getFilename(
                        "c:/test/test.dat", null, ".xml", false))));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.xml").equals(
                FilenameUtils.normalize(FileUtils.getFilename(
                        "c:/test/test.dat", null, ".xml", true))));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.xml").equals(
                FilenameUtils.normalize(FileUtils.getFilename("test.dat",
                        "c:/test", ".xml", true))));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.xml").equals(
                FilenameUtils.normalize(FileUtils.getFilename("test.dat",
                        "c:/test/", ".xml", true))));
        
        assertTrue("c:/test/.test.xml".equals(FileUtils.getFilename(".test",
                "c:/test/", ".xml", true)));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.xml").equals(
                FilenameUtils.normalize(FileUtils.getFilename("test.xml",
                        "c:/test/", ".dat", false))));
        
        assertTrue(FilenameUtils.normalize("c:/test/test.dat").equals(
                FilenameUtils.normalize(FileUtils.getFilename("test",
                        "c:/test/", ".dat", true))));
    }
    
    @Test
    public void testGetUnixFolderName()
    {
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:\\temp")));
        
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:\\temp\\")));
        
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:/temp")));
        
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:/temp/")));
        
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:/temp  ")));
        
        assertTrue("c:/temp/".equals(FileUtils.getUnixFolderName("c:/temp/  ")));
        
        assertTrue("c:/temp/"
                .equals(FileUtils.getUnixFolderName("c:/temp\\  ")));
        
        assertTrue("//temp//abc/".equals(FileUtils
                .getUnixFolderName("//temp//abc")));
    }
    
    @Test
    public void testHasWildCard()
    {
        assertTrue(FileUtils.hasWildCard(null) == null);
        assertTrue(FileUtils.hasWildCard("") == null);
        assertTrue(FileUtils.hasWildCard("  ") == null);
        assertTrue(FileUtils.hasWildCard("c:/test.*"));
        assertTrue(!FileUtils.hasWildCard("c:/test.xml"));
        assertTrue(FileUtils.hasWildCard("c:/?test?.xml"));
        assertTrue(FileUtils.hasWildCard("c:/{test}.xml"));
    }
    
    @Test
    public void testHideFileOnWindows()
        throws Exception
    {
        if (!Utils.isWindows())
            return;
        
        String fileName = FileUtils.getFilename("test.hdn", SystemConfig
                .instance().getDataFolderName(), null, false);
        
        try
        {
            FileUtils.writeObject(fileName, fileName);
            
            FileUtils.hideFileOnWindows(fileName);
            
            File file = new File(fileName);
            
            assertTrue(file.isHidden());
        }
        finally
        {
            FileUtils.deleteFile(fileName);
        }
    }
    
}
