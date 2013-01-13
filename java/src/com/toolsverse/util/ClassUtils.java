/*
 * ClassUtils.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarInputStream;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.log.Logger;

/**
 * The collection of the static methods for loading classes from the various
 * external resources, add classes to the classpath, etc.
 * 
 * @author Maksym Shcherbinin
 * @version 2.0
 * @since 2.0
 */

public final class ClassUtils
{
    
    /**
     * Adds the all files in the folder, including sub folder which match the pattern to the class path.
     *
     * @param folder the folder
     * @param pattern the pattern
     * @throws Exception the exception
     */
    public static void addFolderToClassPath(String folder, String pattern)
    {
        try
        {
            File[] files = FileUtils.getFilesInFolder(folder, null);
            
            if (files == null)
                return;
            
            for (File file : files)
            {
                if (file.isDirectory())
                    addFolderToClassPath(file.getPath(), pattern);
                else if (FilenameUtils.wildcardMatch(file.getPath(), pattern,
                        IOCase.INSENSITIVE))
                    addToClassPath(file);
            }
        }
        catch (Throwable ex)
        {
            Logger.log(Logger.SEVERE, null, Resource.ERROR_GENERAL.getValue(),
                    ex);
        }
        
    }
    
    /**
     * Adds file to the class path.
     * 
     * @param file
     *            the file to add
     * @throws Exception
     *             if anything goes wrong
     */
    public static void addToClassPath(File file)
        throws Exception
    {
        addToClassPath(file.toURI().toURL());
    }
    
    /**
     * Adds file name to to the class path.
     * 
     * @param fileName
     *            the file name to add
     * @throws Exception
     *             if anything goes wrong
     */
    public static void addToClassPath(String fileName)
        throws Exception
    {
        addToClassPath(new File(fileName));
    }
    
    /**
     * Adds multiple files the to class path.
     * 
     * @param name
     *            the string where individual file names divided by delimiter
     * @param delimiter
     *            the delimiter between file names
     * @throws Exception
     *             if anything goes wrong
     */
    public static void addToClassPath(String name, String delimiter)
        throws Exception
    {
        if (!Utils.isNothing(name))
        {
            String[] cp = name.split(delimiter, -1);
            
            for (String jarName : cp)
            {
                if (Utils.isNothing(jarName))
                    continue;
                
                addToClassPath(jarName);
            }
        }
    }
    
    /**
     * Adds url to the class path.
     * 
     * @param url
     *            the url to add
     * @throws Exception
     *             if anything goes wrong
     */
    public static void addToClassPath(URL url)
        throws Exception
    {
        URLClassLoader sysloader = (URLClassLoader)ClassLoader
                .getSystemClassLoader();
        Class<?> sysclass = URLClassLoader.class;
        
        Method method = sysclass.getDeclaredMethod("addURL",
                new Class[] {URL.class});
        method.setAccessible(true);
        method.invoke(sysloader, new Object[] {url});
    }
    
    /**
     * Converts class name to the form which can be used by the Wings framework
     * as a component name.
     * 
     * <br>
     * <br>
     * <b>Example:</b> com.toolsverse.utils.ClassC --&gt;
     * comtoolsverseutilsClassC
     * 
     * @param className
     *            the name of the class
     * @return the converted string
     * 
     */
    public static String className2Web(String className)
    {
        if (className == null)
            return null;
        
        return className.replaceAll("\\.", "");
    }
    
    /**
     * Deep clones object.
     * 
     * @param obj
     *            the object to clone
     * @return the cloned object
     * @throws Exception
     *             if anything goes wrong
     */
    public static Object clone(Object obj)
        throws Exception
    {
        Object clonedObj = null;
        
        ByteArrayOutputStream baos = null;
        ByteArrayInputStream bais = null;
        
        ObjectOutputStream oos = null;
        ObjectInputStream ois = null;
        
        try
        {
            baos = new ByteArrayOutputStream();
            
            try
            {
                oos = new ObjectOutputStream(baos);
                oos.writeObject(obj);
            }
            finally
            {
                if (oos != null)
                    oos.close();
            }
            
            bais = new ByteArrayInputStream(baos.toByteArray());
            
            try
            {
                ois = new ObjectInputStream(bais);
                clonedObj = ois.readObject();
            }
            finally
            {
                if (ois != null)
                    ois.close();
            }
        }
        finally
        {
            if (baos != null)
                baos.close();
            
            if (bais != null)
                bais.close();
        }
        
        return clonedObj;
    }
    
    /**
     * Gets the list of the classes in the package. Each class must be actual
     * .class file and an instance of the classCastTo argument.
     * 
     * <br>
     * <br>
     * 
     * Example:
     * <code>getClasses("com.toolsverse.pkg", ClassA.class, true)</code> --&gt;
     * returns the list of the classes which can be cast to ClassA in the
     * package com.toolsverse.pkg and sub packages
     * 
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param recursively
     *            if <code>true</code> include classes in sub packages
     * @return the list of the classes in the package
     */
    public static List<Class<?>> getClasses(String packageName,
            Class<?> classCastTo, boolean recursively)
    {
        List<Class<?>> classes = new ArrayList<Class<?>>();
        
        // Get a File object for the package
        File directory = null;
        ClassLoader cld = Thread.currentThread().getContextClassLoader();
        if (cld == null)
            return null;
        String path = packageName.replace('.', '/');
        URL resource = cld.getResource(path);
        if (resource == null)
            return classes;
        
        directory = new File(UrlUtils.decodeUrl(resource.getFile()));
        
        if (directory != null && directory.exists())
        {
            // Get the list of the files contained in the package
            String[] files = directory.list();
            for (int i = 0; i < files.length; i++)
            {
                if (recursively)
                {
                    String fName = path + "/" + files[i];
                    
                    resource = cld.getResource(fName);
                    if (resource != null)
                    {
                        
                        File file = new File(UrlUtils.decodeUrl(resource
                                .getFile()));
                        
                        if (file.isDirectory())
                        {
                            String pName = fName.replace('/', '.');
                            
                            List<Class<?>> innerClasses = getClasses(pName,
                                    classCastTo, recursively);
                            
                            if (innerClasses != null)
                                classes.addAll(innerClasses);
                            
                            continue;
                        }
                    }
                }
                
                // we are only interested in .class files
                if (files[i].endsWith(".class"))
                    try
                    {
                        try
                        {
                            Class<?> classToAdd = Class.forName(packageName
                                    + '.'
                                    + files[i].substring(0,
                                            files[i].length() - 6));
                            
                            if (classCastTo == null
                                    || classCastTo.isAssignableFrom(classToAdd))
                                classes.add(classToAdd);
                        }
                        catch (Throwable ex)
                        {
                            Logger.log(Logger.SEVERE, null,
                                    Resource.ERROR_GENERAL.getValue(), ex);
                        }
                        
                    }
                    catch (Throwable ex)
                    {
                        Logger.log(Logger.INFO, null,
                                Resource.ERROR_CLASS_NOT_FOUND.getValue(), ex);
                        
                    }
            }
        }
        
        return classes;
    }
    
    /**
     * Gets the list of classes in the package first, then adds classes in the
     * plugin path and finally adds classes in the lib path. All discovered jars
     * will be added to the class path. <br>
     * The purpose of this function is to dynamically load classes from the
     * structure in the file system, similar to the following: <br>
     * %home%\classes <br>
     * &nbsp;&nbsp;&nbsp;com\toolseverse\pkg (package ) <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ClassA.class <br>
     * &nbsp;&nbsp;&nbsp;&nbsp;&nbsp;&nbsp;ClassB.class <br>
     * %home%\plugin (plugin path) <br>
     * &nbsp;&nbsp;&nbsp;JarA.jar <br>
     * &nbsp;&nbsp;&nbsp;JarB.jar <br>
     * %home%\lib (lib path) <br>
     * &nbsp;&nbsp;&nbsp;JarC.jar <br>
     * &nbsp;&nbsp;&nbsp;JarD.jar <br>
     * <br>
     * Function works differently on the client where it is possible to have
     * separate lib and plugin folders and on the server where all jars usually
     * placed in the single lib folder.
     * 
     * @param libPath
     *            the path to the core jars. It is possible to use file name
     *            wildcards
     * @param pluginPath
     *            the path to the plugin jars. It is possible to use file name
     *            wildcards
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @return the list of the classes
     */
    public static List<Class<?>> getClasses(String libPath, String pluginPath,
            String packageName, Class<?> classCastTo,
            Map<String, String> addedJars)
    {
        List<Class<?>> classes = null;
        
        boolean isClient = SystemConfig.instance().isClient();
        
        List<Class<?>> classesInFolder = getClasses(packageName, classCastTo,
                true);
        
        if (classesInFolder == null)
            classesInFolder = new ArrayList<Class<?>>();
        
        List<Class<?>> classesInJarPath = null;
        
        if (pluginPath.toLowerCase().indexOf("*.jar") >= 0)
        {
            classesInJarPath = getClassesInPackageInFolder(
                    FilenameUtils.getFullPath(pluginPath),
                    FilenameUtils.getName(pluginPath), packageName,
                    classCastTo, addedJars);
        }
        else
            classesInJarPath = getClassesInPackage(pluginPath, packageName,
                    classCastTo, addedJars);
        
        if (classesInJarPath == null)
            classesInJarPath = new ArrayList<Class<?>>();
        
        if (isClient)
        {
            if (libPath.toLowerCase().indexOf(".jar") >= 0)
            {
                classes = getClassesInPackage(libPath, packageName,
                        classCastTo, addedJars);
            }
            else
                classes = getClassesInPackageInFolder(libPath, packageName,
                        classCastTo, addedJars);
        }
        
        classesInFolder.addAll(classesInJarPath);
        if (classes != null)
            classesInFolder.addAll(classes);
        
        return classesInFolder;
    }
    
    /**
     * Gets the list of the classes in the package. Package must be located in
     * the jar file. Each class must be an instance of the classCastTo argument.
     * Jar file will be added to the class path.
     * 
     * @param jarFileName
     *            the name of the jar file
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @return the list of classes in the package
     */
    public static List<Class<?>> getClassesInPackage(String jarFileName,
            String packageName, Class<?> classCastTo,
            Map<String, String> addedJars)
    {
        return getClassesInPackage(jarFileName, packageName,
                new ArrayList<Class<?>>(), classCastTo, addedJars, null);
    }
    
    /**
     * Gets the list of the classes in the package. Package must be located in
     * the jar file. Each class must be an instance of the classCastTo argument.
     * Jar file will be added to the class path.
     * 
     * @param jarFileName
     *            the name of the jar file
     * @param packageName
     *            the name of the package
     * @param classes
     *            the list of already added classes
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @param ext
     *            the ext
     * @return the list of classes in the package
     */
    private static List<Class<?>> getClassesInPackage(String jarFileName,
            String packageName, List<Class<?>> classes, Class<?> classCastTo,
            Map<String, String> addedJars, String ext)
    {
        if (!isJarToConsider(jarFileName, ext))
            return null;
        
        packageName = packageName == null ? null : packageName.replaceAll(
                "\\.", "/");
        try
        {
            if (SystemConfig.instance().isClient()
                    && (addedJars == null || !addedJars
                            .containsKey(jarFileName)))
            {
                addToClassPath(jarFileName);
                
                if (addedJars != null)
                    addedJars.put(jarFileName, jarFileName);
            }
            
            JarInputStream jarFile = new JarInputStream(new FileInputStream(
                    jarFileName));
            
            try
            {
                
                JarEntry jarEntry;
                
                while (true)
                {
                    jarEntry = jarFile.getNextJarEntry();
                    if (jarEntry == null)
                        break;
                    
                    if (((packageName == null || jarEntry.getName().startsWith(
                            packageName)))
                            && (jarEntry.getName().endsWith(".class")))
                    {
                        String className = jarEntry.getName().replaceAll("/",
                                "\\.");
                        
                        className = className.substring(0,
                                className.indexOf(".class"));
                        
                        try
                        {
                            Class<?> classToAdd = Class.forName(className);
                            
                            if (classCastTo == null
                                    || classCastTo.isAssignableFrom(classToAdd))
                                if (!classes.contains(classToAdd))
                                    classes.add(classToAdd);
                        }
                        catch (Throwable ex)
                        {
                            Logger.log(Logger.INFO, null,
                                    Resource.ERROR_CLASS_NOT_FOUND.getValue(),
                                    ex);
                        }
                    }
                }
            }
            finally
            {
                if (jarFile != null)
                    jarFile.close();
            }
        }
        catch (Throwable e)
        {
            classes.clear();
            
            return classes;
        }
        
        return classes;
    }
    
    /**
     * Gets the list of the classes in the package located in the jar files in
     * the folder. All discovered jars will be added to the class path.
     * 
     * @param folderName
     *            the name of the folder with jar files
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @return the list of the classes in the package
     */
    public static List<Class<?>> getClassesInPackageInFolder(String folderName,
            String packageName, Class<?> classCastTo,
            Map<String, String> addedJars)
    {
        return getClassesInPackageInFolder(folderName, null, packageName,
                classCastTo, addedJars);
        
    }
    
    /**
     * Gets the list of the classes in the package located in the jar files in
     * the folder. Jar files can be limited to the fileName (wildcards are
     * allowed). All discovered jars will be added to the class path.
     * 
     * @param folderName
     *            the name of the folder with jar files
     * @param fileName
     *            the name of the jar file. Wildcards are allowed. If equals to
     *            null <b>*.jar</b> will be used.
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @return the list of the classes in the package
     */
    public static List<Class<?>> getClassesInPackageInFolder(String folderName,
            String fileName, String packageName, Class<?> classCastTo,
            Map<String, String> addedJars)
    {
        return getClassesInPackageInFolder(folderName, fileName, packageName,
                classCastTo, addedJars, null);
    }
    
    /**
     * Gets the list of the classes in the package located in the jar files in
     * the folder. Jar files can be limited to the fileName (wildcards are
     * allowed). All discovered jars will be added to the class path.
     * 
     * @param folderName
     *            the name of the folder with jar files
     * @param fileName
     *            the name of the jar file. Wildcards are allowed. If equals to
     *            null <b>*.jar</b> will be used.
     * @param packageName
     *            the name of the package
     * @param classCastTo
     *            the class to check against.
     * @param addedJars
     *            the jar files already added to the class path
     * @param ext
     *            the extension of the control file
     * @return the list of the classes in the package
     */
    public static List<Class<?>> getClassesInPackageInFolder(String folderName,
            String fileName, String packageName, Class<?> classCastTo,
            Map<String, String> addedJars, String ext)
    {
        File[] files = null;
        List<Class<?>> classes = new ArrayList<Class<?>>();
        
        try
        {
            files = FileUtils.getFilesInFolder(folderName,
                    !Utils.isNothing(fileName) ? fileName : "*.jar");
        }
        catch (Exception ex)
        {
            return classes;
        }
        
        if (files == null || files.length == 0)
            return classes;
        
        for (int i = 0; i < files.length; i++)
            getClassesInPackage(FileUtils.getUnixFolderName(folderName)
                    + files[i].getName(), packageName, classes, classCastTo,
                    addedJars, ext);
        
        return classes;
    }
    
    /**
     * Checks if jar file should be considered for loading.
     * 
     * @param jarFileName
     *            the name of the jar file
     * @param ext
     *            the extension of the file. Default is ".ext"
     * @return true, if jar file name either starts with <b>toolsverse</b> or
     *         file with the same name and extension <b>ext</b> exists in the
     *         same folder as the original jar file.
     */
    private static boolean isJarToConsider(String jarFileName, String ext)
    {
        String name = FilenameUtils.getName(jarFileName);
        
        if (name.indexOf(SystemConfig.JAR_PREFIX) == 0)
            return true;
        
        ext = ext != null ? ext : ".ext";
        
        return new File(FilenameUtils.getFullPath(jarFileName)
                + FilenameUtils.getBaseName(jarFileName) + ext).exists();
    }
    
}
