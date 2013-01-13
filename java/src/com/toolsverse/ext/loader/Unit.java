/*
 * EtlUnit.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext.loader;

import java.util.List;

/**
 * All dynamically loaded modules are grouped in Units. The module not necessary implements ExtensionModule but usually does. 
 * Unit knows where they are, how to store and configure them, etc. The example is a DriverUnit which knows
 * that ETL drivers belong to the com.toolsverse.etl.driver package and sub packages, should be loaded once, can not be reloaded, etc. The class responsible for 
 * actual loading is UnitLoader. 
 *
 * @param <C> the generic type. Not necessary implements ExtensionModule but usually does.
 * 
 * @see com.toolsverse.ext.loader.UnitLoader
 * @see com.toolsverse.ext.ExtensionModule 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class Unit<C>
{
    
    /** The path the modules. UnitLoader will use it first.*/
    private String _path;
    
    /** The path to the jars. UnitLoader will use it second.*/
    private String _jarPath;
    
    /** The root class path. */
    private String _classPath;
    
    /** The obj class. */
    private Class<C> _objClass;
    
    /** The singleton flag. */
    private boolean _singleton;
    
    /**
     * Instantiates a new Unit using default parameters. 
     */
    public Unit()
    {
        _singleton = true;
    }
    
    /**
     * Instantiates a new Unit.
     *
     * @param classPath the root class path
     * @param objClass the class of object. All loaded modules will be casted to objClass.
     * @param singleton the singleton == true all modules will be loaded as singletons
     */
    public Unit(String classPath, Class<C> objClass, boolean singleton)
    {
        _classPath = classPath;
        _objClass = objClass;
        _singleton = singleton;
    }
    
    /**
     * Instantiates a new Unit.
     *
     * @param path the path. UnitLoader will use it first to load modules.
     * @param jarPath the path the jar path. UnitLoader will use it second path to load modules.
     * @param classPath the root class path
     * @param objClass the class of object. All loaded modules will be casted to objClass.
     * @param singleton if singleton == true all modules will be loaded as singletons
     */
    public Unit(String path, String jarPath, String classPath,
            Class<C> objClass, boolean singleton)
    {
        _path = path;
        _jarPath = jarPath;
        _classPath = classPath;
        _objClass = objClass;
        _singleton = singleton;
    }
    
    /**
     * Adds loaded object using name as a key.
     *
     * @param name the unique name of the object
     * @param object the object
     */
    public abstract void add(String name, Object object);
    
    /**
     * Adds loaded object which doesn't have license using name as a key.
     *
     * @param name the unique name of the object
     * @param object the object
     */
    public abstract void addWithoutLicense(String name, Object object);
    
    /**
     * Checks if loader allowed to to load all objects regardless of license.
     *
     * @return true, if successful
     */
    public abstract boolean allowLoadAll();
    
    /**
     * Checks if module can be loaded again.
     *
     * @return true, if successful
     */
    public abstract boolean canBeLoadedAgain();
    
    /**
     * Executed when UnitLoader finishes load.
     *
     * @param loader the UnitLoader
     */
    public abstract void finishLoad(UnitLoader loader);
    
    /**
     * Executed when there is a need to free memory.
     */
    public abstract void free();
    
    /**
     * Gets the root class path to the modules. 
     *
     * @return the class path
     */
    public String getClassPath()
    {
        return _classPath;
    }
    
    /**
     * Gets the jar path.
     *
     * @return the jar path
     */
    public String getJarPath()
    {
        return _jarPath;
    }
    
    /**
     * Gets the list of loaded modules.
     *
     * @return the list
     */
    public abstract List<C> getList();
    
    /**
     * Gets the module class.
     *
     * @return the module class
     */
    public Class<C> getObjClass()
    {
        return _objClass;
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
     * Checks if module should be a singleton.
     *
     * @return true, if it is a singleton
     */
    public boolean isSingleton()
    {
        return _singleton;
    }
    
    /**
     * Returns true if Unit is indented to load configurable extensions. The configurable extension will have a place in the configuration UI and requires xml 
     * configuration file.
     *
     * @return true, if successful
     */
    public abstract boolean loadsConfigurableExtensions();
    
    /**
     * Sets the class path.
     *
     * @param value the new class path
     */
    public void setClassPath(String value)
    {
        _classPath = value;
    }
    
    /**
     * Sets the jar path.
     *
     * @param value the new jar path
     */
    public void setJarPath(String value)
    {
        _jarPath = value;
    }
    
    /**
     * Sets the module class.
     *
     * @param value the new module class
     */
    @SuppressWarnings("unchecked")
    public void setObjClass(Class<?> value)
    {
        _objClass = (Class<C>)value;
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
     * Sets the singleton flag.
     *
     * @param value the new singleton flag
     */
    public void setSingleton(boolean value)
    {
        _singleton = value;
    }
    
    /**
     * Executed when UnitLoader starts loading. Put initialization code here.
     *
     * @return true, if supported
     */
    public abstract boolean startLoad();
}