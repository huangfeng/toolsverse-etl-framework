/*
 * FileUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.FilenameFilter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Writer;
import java.net.URL;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Enumeration;
import java.util.List;
import java.util.zip.ZipEntry;
import java.util.zip.ZipFile;
import java.util.zip.ZipOutputStream;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.log.Logger;

/**
 * The collection of the static methods for the file operations such as copy, delete, etc.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 1.0
 */

public final class FileUtils
{
    
    /** The buffer. */
    public static int BUFFER = 2048;
    
    /**
     * Adds files to the zip.
     *
     * @param folder the folder with the files
     * @param zipFileName the zip file name
     * @param files the list of the files to add. The file path is ignored, only
     * name and extension combined with the folder argument are used
     * @throws Exception is anything goes wrong
     */
    public static void addFilesToZip(String folder, String zipFileName,
            List<String> files)
        throws Exception
    {
        // Create a buffer for reading the files
        byte[] buf = new byte[BUFFER];
        
        ZipOutputStream out = null;
        
        folder = getUnixFolderName(folder);
        
        // Create the ZIP file
        try
        {
            out = new ZipOutputStream(new FileOutputStream(zipFileName));
            
            if (files == null || files.size() == 0)
                return;
            
            // Compress the files
            for (int i = 0; i < files.size(); i++)
            {
                String fileToAdd = FilenameUtils.getName(files.get(i));
                
                if ((folder + fileToAdd).equalsIgnoreCase(zipFileName))
                    continue;
                
                FileInputStream in = null;
                
                try
                {
                    in = new FileInputStream(folder + fileToAdd);
                    
                    // Add ZIP entry to output stream.
                    out.putNextEntry(new ZipEntry(fileToAdd));
                    
                    // Transfer bytes from the file to the ZIP file
                    int len;
                    while ((len = in.read(buf)) > 0)
                        out.write(buf, 0, len);
                }
                finally
                {
                    // Complete the entry
                    try
                    {
                        out.closeEntry();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                    if (in != null)
                        try
                        {
                            in.close();
                        }
                        catch (Exception ex)
                        {
                            Logger.log(Logger.SEVERE, null,
                                    Resource.ERROR_GENERAL.getValue(), ex);
                            
                        }
                }
            }
        }
        finally
        {
            // Complete the ZIP file
            try
            {
                if (out != null)
                    out.close();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Adds files to the zip.
     *
     * @param folder the folder with the files
     * @param zipFileName the zip file name
     * @param fileName the file name to add. Wildcards are allowed. The file path is
     * ignored, only name and extension combined with the folder
     * argument are used
     * @throws Exception is anything goes wrong
     */
    public static void addFilesToZip(String folder, String zipFileName,
            final String fileName)
        throws Exception
    {
        // Create a buffer for reading the files
        byte[] buf = new byte[BUFFER];
        
        ZipOutputStream out = null;
        
        folder = getUnixFolderName(folder);
        
        // Create the ZIP file
        try
        {
            out = new ZipOutputStream(new FileOutputStream(zipFileName));
            
            File[] files = getFilesInFolder(folder, fileName);
            
            if (files == null || files.length == 0)
                return;
            
            // Compress the files
            for (int i = 0; i < files.length; i++)
            {
                if (!files[i].isFile()
                        || (folder + files[i].getName())
                                .equalsIgnoreCase(zipFileName))
                    continue;
                
                FileInputStream in = null;
                
                try
                {
                    in = new FileInputStream(folder + files[i].getName());
                    
                    // Add ZIP entry to output stream.
                    out.putNextEntry(new ZipEntry(files[i].getName()));
                    
                    // Transfer bytes from the file to the ZIP file
                    int len;
                    while ((len = in.read(buf)) > 0)
                        out.write(buf, 0, len);
                }
                finally
                {
                    // Complete the entry
                    try
                    {
                        out.closeEntry();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                    if (in != null)
                        try
                        {
                            in.close();
                        }
                        catch (Exception ex)
                        {
                            Logger.log(Logger.SEVERE, null,
                                    Resource.ERROR_GENERAL.getValue(), ex);
                            
                        }
                }
            }
        }
        finally
        {
            // Complete the ZIP file
            try
            {
                if (out != null)
                    out.close();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Change file extension.
     * 
     * <pre>
     * c:\abc\xyz.txt, new ext xml --> c:\abc\xyz.xml
     * xyz.txt, new ext xml --> xyz.xml
     * xyz, new ext xml --> xyz.xml 
     * </pre>
     
     * @param fileName the file name
     * @param ext the new extension
     * @return the new file name
     */
    public static String changeExt(String fileName, String ext)
    {
        if (Utils.isNothing(fileName))
            return fileName;
        
        ext = Utils.makeString(ext);
        
        String path = Utils.makeString(FilenameUtils.getFullPath(fileName));
        
        String name = FilenameUtils.getBaseName(fileName);
        
        return path + name + (!Utils.isNothing(ext) ? "." + ext : "");
    }
    
    /**
     * Change file name only.
     *
     * <pre>
     * c:\abc\xyz.txt, new name mnm --> c:\abc\mnm.txt
     * xyz, new name mnm --> mnm
     * xyz.txt, new name mnm --> mnm.txt 
     * </pre>
     *
     * @param fileName the file name
     * @param name the new name
     * @return the file name
     */
    public static String changeFileName(String fileName, String name)
    {
        if (Utils.isNothing(fileName))
            return name;
        
        if (Utils.isNothing(name))
            return fileName;
        
        String path = Utils.makeString(FilenameUtils.getFullPath(fileName));
        
        String ext = FilenameUtils.getExtension(fileName);
        
        return path + name + (!Utils.isNothing(ext) ? "." + ext : "");
    }
    
    /**
     * Copies file.
     *
     * @param in the input file
     * @param out the output file
     * @throws Exception is anything goes wrong
     */
    public static void copyFile(File in, File out)
        throws Exception
    {
        FileChannel sourceChannel = null;
        FileChannel destinationChannel = null;
        try
        {
            sourceChannel = new FileInputStream(in).getChannel();
            destinationChannel = new FileOutputStream(out).getChannel();
            
            destinationChannel.transferFrom(sourceChannel, 0,
                    sourceChannel.size());
        }
        finally
        {
            try
            {
                if (sourceChannel != null)
                    sourceChannel.close();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
            try
            {
                if (destinationChannel != null)
                    destinationChannel.close();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Copies input stream into the file.
     * 
     * @param in
     *            the input stream
     * @param out
     *            the output file
     * 
     * @throws Exception in case of any error
     */
    public static void copyFile(InputStream in, File out)
        throws Exception
    {
        FileOutputStream outS = null;
        byte[] buf = new byte[BUFFER];
        
        try
        {
            outS = new FileOutputStream(out);
            
            // Transfer bytes from the file to the ZIP file
            int len;
            while ((len = in.read(buf)) > 0)
                outS.write(buf, 0, len);
        }
        finally
        {
            try
            {
                if (outS != null)
                    outS.close();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Copies files from the source folder to the destination.
     * 
     * @param source
     *            the source folder name
     * @param dest
     *            the destination folder name
     * @param fName
     *            the file name. Wildcards are allowed
     * 
     * @throws Exception in case of any error 
     */
    public static void copyFilesFromFolderToFolder(String source, String dest,
            final String fName)
        throws Exception
    {
        File[] files = getFilesInFolder(source, fName);
        
        if (files == null || files.length == 0)
            return;
        
        for (int i = 0; i < files.length; i++)
        {
            File file = files[i];
            
            File destFile = new File(dest, file.getName());
            
            if (file.isFile())
            {
                copyFile(file, destFile);
            }
        }
    }
    
    /**
     * Deletes file or folder. If folder is not empty will delete all sub
     * folders and files.
     * 
     * @param path
     *            the full path to the file or folder
     * 
     * @return true, if successful
     */
    public static boolean deleteFile(String path)
    {
        File oFile = new File(path);
        if (oFile.isDirectory())
        {
            File[] aFiles = oFile.listFiles();
            for (File oFileCur : aFiles)
            {
                if (!deleteFile(oFileCur.getAbsolutePath()))
                    return false;
            }
        }
        return oFile.delete();
    }
    
    /**
     * Deletes file in the folder.
     * 
     * @param folder
     *            the folder name
     * @param filename
     *            the file name
     * 
     * @return true, if successful
     */
    public static boolean deleteFile(String folder, String filename)
    {
        File file = new File(getUnixFolderName(folder), filename);
        file.delete();
        
        return !file.exists();
    }
    
    /**
     * Delete files in the folder.
     * 
     * @param folder
     *            the folder name
     * @param fName
     *            the file name. Wild cards are allowed.
     * 
     * @throws Exception in case of any error 
     */
    public static void deleteFilesInFolder(String folder, final String fName)
        throws Exception
    {
        File[] files = getFilesInFolder(folder, fName);
        
        if (files == null || files.length == 0)
            return;
        
        for (int i = 0; i < files.length; i++)
        {
            File file = files[i];
            
            if (file.isFile())
                file.delete();
        }
    }
    
    /**
     * Download file.
     *
     * @param downloadUrl the download url
     * @param fileName the file name
     * @throws Exception the exception
     */
    public static void downloadFile(String downloadUrl, String fileName)
        throws Exception
    {
        URL url = new URL(downloadUrl);
        
        InputStream reader = null;
        
        try
        {
            url.openConnection();
            
            reader = url.openStream();
            
            copyFile(reader, new File(fileName));
        }
        finally
        {
            if (reader != null)
                reader.close();
        }
    }
    
    /**
     * Checks if file or folder exists.
     * 
     * @param filename
     *            the file or folder name
     * 
     * @return true, if file or folder exists
     */
    public static boolean fileExists(String filename)
    {
        File file = new File(filename);
        
        return file.exists();
    }
    
    /**
     * Reads the binary file and returns array of bytes.
     * 
     * @param file
     *            the full file name
     * 
     * @return the array of bytes from the file
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static byte[] getBytesFromFile(File file)
        throws IOException
    {
        InputStream is = new FileInputStream(file);
        
        try
        {
            
            // Get the size of the file
            long length = file.length();
            
            // You cannot create an array using a long type.
            // It needs to be an int type.
            // Before converting to an int type, check
            // to ensure that file is not larger than Integer.MAX_VALUE.
            if (length > Integer.MAX_VALUE)
            {
                // File is too large
            }
            
            // Create the byte array to hold the data
            byte[] bytes = new byte[(int)length];
            
            // Read in the bytes
            int offset = 0;
            int numRead = 0;
            while (offset < bytes.length
                    && (numRead = is.read(bytes, offset, bytes.length - offset)) >= 0)
            {
                offset += numRead;
            }
            
            // Ensure all the bytes have been read in
            if (offset < bytes.length)
            {
                throw new IOException("Could not completely read file "
                        + file.getName());
            }
            
            return bytes;
        }
        finally
        {
            is.close();
        }
    }
    
    /**
     * Creates the full name of the file.
     * 
     * <br>
     * <br>
     * <b>Examples:</b> <br>
     * getFilename("c:temp\test.txt", null, null, false) --&gt; c:\temp\test.txt <br>
     * getFilename("test.txt", "c:\abc", null, false) --&gt; c:\abc\test.txt <br>
     * getFilename("test.txt", "c:\abc", "bak", true) --&gt; c:\abc\test.bak <br>
     * 
     * @param name
     *            the original name of the file
     * @param folder
     *            the folder
     * @param ext
     *            the extension
     * @param forceExt
     *            if <code>true</code> the ext argument will used. Otherwise the
     *            original file extension is used.
     * 
     * @return the filename
     */
    public static String getFilename(String name, String folder, String ext,
            boolean forceExt)
    {
        String path = FilenameUtils.getFullPath(name);
        if (Utils.isNothing(path))
            path = folder;
        
        path = getUnixFolderName(path);
        
        String baseName = "";
        
        if (name != null && name.startsWith("."))
            baseName = name;
        else
            baseName = FilenameUtils.getBaseName(name);
        
        String fExt = FilenameUtils.getExtension(name);
        ext = forceExt || Utils.isNothing(fExt) ? ext : "." + fExt;
        
        return path + baseName + ext;
    }
    
    /**
     * Gets the files in the folder.
     * 
     * @param folder
     *            the folder name
     * @param fName
     *            the file name. Wildcards are allowed
     * 
     * @return the list of the files in the folder
     */
    public static File[] getFilesInFolder(String folder, final String fName)
    {
        if (Utils.isNothing(folder))
            return null;
        
        File directory = new File(folder);
        if (!directory.isDirectory())
            return null;
        
        File[] files = directory.listFiles(new FilenameFilter()
        {
            public boolean accept(File dir, String name)
            {
                if (fName == null)
                    return true;
                else
                    return FilenameUtils.wildcardMatch(name, fName,
                            IOCase.INSENSITIVE);
            }
        });
        
        return files;
    }
    
    /**
     * Gets the folder name by replacing original separators on the separator
     * argument. Separator is either Unix style "/" or Windows style "\".
     * 
     * @param folder
     *            the original folder
     * @param separator
     *            the separator
     * 
     * @return the folder name
     */
    public static String getFolderName(String folder, String separator)
    {
        if (Utils.isNothing(folder))
            return "";
        
        folder = folder.trim() + FilenameUtils.UNIX_SEPARATOR;
        
        if (Utils.isNothing(separator))
        {
            return FilenameUtils.separatorsToUnix(FilenameUtils
                    .normalize(folder));
        }
        
        if (FilenameUtils.UNIX_SEPARATOR == separator.charAt(0))
            return FilenameUtils.separatorsToUnix(FilenameUtils
                    .normalize(folder));
        else
            return FilenameUtils.separatorsToWindows(FilenameUtils
                    .normalize(folder));
    }
    
    /**
     * Gets the full file name.
     * 
     * <br>
     * <br>
     * <b>Examples:</b> <br>
     * getFullFileName("c:\temp", "abc", ".txt", true) --&gt; c:/temp/abc.txt <br>
     * getFullFileName("c:\temp", "abc", ".txt", false) --&gt; c:\temp\abc.txt <br>
     * 
     * @param folder
     *            the folder name
     * @param name
     *            the name of the file
     * @param ext
     *            the extension of the file
     * @param convert
     *            if <code>true</code> converts all separators to the Unix style
     * 
     * @return the full name
     */
    public static String getFullFileName(String folder, String name,
            String ext, boolean convert)
    {
        if (convert)
            return getUnixFolderName(folder) + name + ext;
        else
            return folder + name + ext;
    }
    
    /**
     * Gets the folder name using Unix style separators. Removes extra
     * whitespace.
     * 
     * <br>
     * <br>
     * <b>Examples:</b> <br>
     * getUnixFolderName("c:\temp\abc") --&gt; c:/temp/abc/ <br>
     * getUnixFolderName("//usr//temp") --&gt; //usr//temp/ <br>
     * 
     * @param folder
     *            the original folder name
     * 
     * @return the folder name with Unix style separators
     */
    public static String getUnixFolderName(String folder)
    {
        return getFolderName(FilenameUtils.separatorsToUnix(folder),
                String.valueOf(FilenameUtils.UNIX_SEPARATOR));
    }
    
    /**
     * Checks for wildcards or { or } in the file name.
     * 
     * @param fileName
     *            the file name
     * 
     * @return the <code>Boolean.TRUE</code> if file name contains wildcards or
     *         { or }. Returns null if file name is empty.
     */
    public static Boolean hasWildCard(String fileName)
    {
        if (Utils.isNothing(fileName))
            return null;
        
        return fileName.indexOf("*") >= 0 || fileName.indexOf("?") >= 0
                || fileName.indexOf("{") >= 0 || fileName.indexOf("}") >= 0;
    }
    
    /**
     * Hides file on windows.
     *
     * @param fileName the file name
     */
    public static void hideFileOnWindows(String fileName)
    {
        if (!Utils.isWindows())
            return;
        
        try
        {
            Process p = Runtime.getRuntime().exec("attrib +h " + fileName);
            p.waitFor();
        }
        catch (Throwable ex)
        {
            Logger.log(Logger.SEVERE, null, Resource.ERROR_GENERAL.getValue(),
                    ex);
        }
    }
    
    /**
     * Loads text file into the string.
     * 
     * @param fileName
     *            the file name
     * 
     * @return the string
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static String loadTextFile(String fileName)
        throws IOException
    {
        return loadTextFile(fileName, -1);
    }
    
    /**
     * Loads text file into the string. Limits the max size of the loaded data.
     * 
     * @param fileName
     *            the file name
     * @param maxSize
     *            the max size of the loaded data. If equals to -1 the limit
     *            will not be enforced.
     * 
     * @return the string
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static String loadTextFile(String fileName, long maxSize)
        throws IOException
    {
        File file = new File(fileName);
        
        StringBuilder builder = new StringBuilder();
        
        BufferedReader input = null;
        try
        {
            input = new BufferedReader(new FileReader(file));
            
            String line = null;
            int count = 0;
            long size = 0;
            while ((line = input.readLine()) != null
                    && (maxSize <= 0 || size < maxSize))
            {
                if (maxSize > 0)
                    size = size + line.getBytes().length;
                
                builder.append(count++ > 0 ? "\n" + line : line);
            }
            
            return builder.toString();
        }
        finally
        {
            try
            {
                if (input != null)
                {
                    input.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Creates a folder.
     *
     * @param name the folder name
     * @return true, if successful
     */
    public static boolean mkDir(String name)
    {
        File dir = new File(name);
        
        if (dir.exists() && dir.isDirectory())
            return true;
        
        return dir.mkdirs() && dir.exists();
    }
    
    /**
     * Reads java object from the file.
     * 
     * @param fileName
     *            the file name
     * 
     * @return the object
     * 
     * @throws Exception in case of any error
     */
    public static Object readObject(String fileName)
        throws Exception
    {
        File inputFile = new File(fileName);
        
        Object ret = null;
        if (inputFile.exists())
        {
            ObjectInputStream inputStream = null;
            
            try
            {
                inputStream = new ObjectInputStream(new BufferedInputStream(
                        new FileInputStream(inputFile)));
                ret = inputStream.readObject();
            }
            finally
            {
                if (inputStream != null)
                    inputStream.close();
            }
            
            return ret;
        }
        
        return null;
    }
    
    /**
     * Reads text file into the string. Limits the max size of the loaded data.
     * 
     * @param fileName
     *            the file name
     * @param maxSize
     *            the max size of the loaded data. If equals to -1 the limit
     *            will not be enforced.
     * 
     * @return TypedKeyValue<String, Long> where key is a loaded string and
     *         value is a number of bytes actually read.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static TypedKeyValue<String, Long> readTextFile(String fileName,
            long maxSize)
        throws IOException
    {
        TypedKeyValue<String, Long> ret = new TypedKeyValue<String, Long>(null,
                null);
        
        File file = new File(fileName);
        
        StringBuilder builder = new StringBuilder();
        
        BufferedReader input = null;
        try
        {
            input = new BufferedReader(new FileReader(file));
            
            String line = null;
            int count = 0;
            long size = 0;
            while ((line = input.readLine()) != null
                    && (maxSize <= 0 || size < maxSize))
            {
                size = size + line.getBytes().length;
                
                builder.append(count++ > 0 ? "\n" + line : line);
            }
            
            ret.setKey(builder.toString());
            ret.setValue(size);
            
            return ret;
        }
        finally
        {
            try
            {
                if (input != null)
                {
                    input.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
        }
    }
    
    /**
     * Reads input stream into the string. Limits the max size of the loaded
     * data.
     * 
     * @param inputStream
     *            the input stream
     * @param maxSize
     *            the max size of the loaded data. If equals to -1 the limit
     *            will not be enforced.
     * 
     * @return TypedKeyValue<String, Long> where key is a loaded string and
     *         value is a number of bytes actually read.
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static TypedKeyValue<String, Long> readTextFromStream(
            InputStream inputStream, long maxSize)
        throws IOException
    {
        TypedKeyValue<String, Long> ret = new TypedKeyValue<String, Long>(null,
                null);
        
        StringBuilder builder = new StringBuilder();
        
        BufferedReader input = null;
        DataInputStream in = null;
        try
        {
            in = new DataInputStream(inputStream);
            
            input = new BufferedReader(new InputStreamReader(in));
            
            String line = null;
            int count = 0;
            long size = 0;
            while ((line = input.readLine()) != null
                    && (maxSize <= 0 || size < maxSize))
            {
                size = size + line.getBytes().length;
                
                builder.append(count++ > 0 ? "\n" + line : line);
            }
            
            ret.setKey(builder.toString());
            ret.setValue(size);
            
            return ret;
        }
        finally
        {
            try
            {
                if (in != null)
                {
                    in.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
            
            try
            {
                if (input != null)
                {
                    input.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
            
            try
            {
                if (inputStream != null)
                {
                    inputStream.close();
                }
            }
            catch (IOException ex)
            {
                Logger.log(Logger.SEVERE, null,
                        Resource.ERROR_GENERAL.getValue(), ex);
                
            }
            
        }
    }
    
    /**
     * Saves the text file.
     * 
     * @param fileName
     *            the file name
     * @param text
     *            the text to save
     * 
     * @throws IOException
     *             Signals that an I/O exception has occurred.
     */
    public static void saveTextFile(String fileName, String text)
        throws IOException
    {
        File file = new File(fileName);
        
        Writer output = null;
        try
        {
            output = new BufferedWriter(new FileWriter(file));
            
            output.write(text);
            
        }
        finally
        {
            if (output != null)
                output.close();
        }
    }
    
    /**
     * Unzip files into the folder.
     *
     * @param destFolder the destination folder name
     * @param zipFileName the zip file name
     * @return the the list of strings, each of them is a fully qualified file
     * name
     * @throws Exception in case of any error
     */
    public static List<String> unzipFiles(String destFolder, String zipFileName)
        throws Exception
    {
        ArrayList<String> files = null;
        destFolder = getUnixFolderName(destFolder);
        ZipFile zf = null;
        try
        {
            zf = new ZipFile(zipFileName);
            Enumeration<? extends ZipEntry> zipEntries = zf.entries();
            
            files = new ArrayList<String>();
            
            while (zipEntries.hasMoreElements())
            {
                ZipEntry target = zipEntries.nextElement();
                
                File destinationPath = new File(destFolder, target.getName());
                
                destinationPath.getParentFile().mkdirs();
                
                if (target.isDirectory())
                {
                    files.add(target.getName());
                    
                    continue;
                }
                
                InputStream is = zf.getInputStream(target);
                BufferedInputStream bis = new BufferedInputStream(is);
                
                FileOutputStream fos = null;
                BufferedOutputStream bos = null;
                
                try
                {
                    fos = new FileOutputStream(destinationPath);
                    bos = new BufferedOutputStream(fos);
                    
                    int c;
                    while ((c = bis.read()) != -1)
                        bos.write((byte)c);
                    
                    files.add(target.getName());
                }
                finally
                {
                    try
                    {
                        if (bos != null)
                            bos.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                    try
                    {
                        if (fos != null)
                            fos.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                    try
                    {
                        if (bis != null)
                            bis.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                    try
                    {
                        if (is != null)
                            is.close();
                    }
                    catch (Exception ex)
                    {
                        Logger.log(Logger.SEVERE, null,
                                Resource.ERROR_GENERAL.getValue(), ex);
                        
                    }
                    
                }
            }
        }
        finally
        {
            if (zf != null)
                try
                {
                    zf.close();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.SEVERE, null,
                            Resource.ERROR_GENERAL.getValue(), ex);
                    
                }
        }
        
        return files;
    }
    
    /**
     * Writes Java object into the file.
     *
     * @param fileName the file name
     * @param object the object
     * @throws Exception in case of any error
     */
    public static void writeObject(String fileName, Object object)
        throws Exception
    {
        ObjectOutputStream outputStream = null;
        
        try
        {
            outputStream = new ObjectOutputStream(new BufferedOutputStream(
                    new FileOutputStream(new File(fileName))));
            
            outputStream.writeObject(object);
        }
        finally
        {
            if (outputStream != null)
            {
                outputStream.close();
            }
        }
    }
}
