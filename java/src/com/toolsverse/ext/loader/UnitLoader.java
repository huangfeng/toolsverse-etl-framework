/*
 * UnitLoader.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext.loader;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.exception.DefaultExceptionHandler;
import com.toolsverse.ext.BaseExtension;
import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.license.LicenseManager;
import com.toolsverse.resource.Resource;
import com.toolsverse.util.ClassUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.filter.Filter;
import com.toolsverse.util.filter.FilterContext;
import com.toolsverse.util.log.Logger;

/**
 * The instance of this class dynamically loads Units and all its extensions at run time. There is a hierarchy of folders where units and extension can be found. 
 * Usually loader examines CLASSES folder first, then plugin, then lib folder. It parses all .class and .jar files and looks for classes located under provided class path. 
 * It loads Units first, then actual extensions. 
 * 
 * @see com.toolsverse.ext.loader.Unit
 * @see com.toolsverse.ext.loader.LocalUnitLoader
 * @see com.toolsverse.ext.loader.SharedUnitLoader 
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class UnitLoader
{
    
    /**
     * The Class StoredExtension. Just a place holder for any ExtensionModule. Used to display loaded extension in About dialog and other places.
     */
    public static class StoredExtension extends BaseExtension
    {
        
        /** The display name. */
        String _displayName;
        
        /** The version. */
        String _version;
        
        /** The type. */
        String _type;
        
        /** The vendor. */
        String _vendor;
        
        /** The _license propert name. */
        String _licensePropertName;
        
        /** The _class name. */
        String _className;
        
        /**
         * Instantiates a new StoredExtension.
         *
         * @param ext the ext
         */
        public StoredExtension(ExtensionModule ext)
        {
            _displayName = Utils.isNothing(ext.getDisplayName()) ? ext
                    .getClass().getName() : ext.getDisplayName();
            _version = ext.getVersion();
            _type = Utils.isNothing(ext.getType()) ? ext.getClass().getName()
                    : ext.getType();
            _licensePropertName = ext.getLicensePropertyName();
            _vendor = ext.getVendor();
            _className = ext.getClass().getName();
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.ext.ExtensionModule#getDisplayName()
         */
        public String getDisplayName()
        {
            return _displayName;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
         */
        public String getLicensePropertyName()
        {
            return _licensePropertName;
        }
        
        /**
         * Gets the parent class name.
         *
         * @return the parent class name
         */
        public String getParentClassName()
        {
            return _className;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.ext.ExtensionModule#getType()
         */
        public String getType()
        {
            return _type;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.ext.ExtensionModule#getVendor()
         */
        public String getVendor()
        {
            return _vendor;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see com.toolsverse.ext.ExtensionModule#getVersion()
         */
        public String getVersion()
        {
            return _version;
        }
        
    }
    
    /** units. */
    private Map<String, Unit<?>> _units;
    
    /** loaded units. */
    private Map<String, String> _loadedUnits;
    
    /** jars added to classpath. */
    private Map<String, String> _addedJars;
    
    /** registered paths. */
    private Map<String, String> _registeredPath;
    
    /** extensions. */
    private List<ExtensionModule> _extensions;
    
    /**
     * Instantiates a new UnitLoader.
     */
    public UnitLoader()
    {
        _units = new ConcurrentHashMap<String, Unit<?>>();
        _addedJars = new ConcurrentHashMap<String, String>();
        _registeredPath = new ConcurrentHashMap<String, String>();
        _loadedUnits = new ConcurrentHashMap<String, String>();
        _extensions = new Vector<ExtensionModule>();
    }
    
    /**
     * Adds the ExtensionModule.
     *
     * @param ext the ExtensionModule
     */
    public void addExtension(ExtensionModule ext)
    {
        _extensions.add(new StoredExtension(ext));
    }
    
    /**
     * Frees the memory.
     */
    public void free()
    {
        for (Unit<?> unit : _units.values())
            unit.free();
        
        _units.clear();
        
        _addedJars.clear();
        
        _registeredPath.clear();
        
        _loadedUnits.clear();
        
        _extensions.clear();
    }
    
    /**
     * Gets the map of the added jars.
     *
     * @return the added jars
     */
    public Map<String, String> getAddedJars()
    {
        return _addedJars;
    }
    
    /**
     * Gets the list of extensions.
     *
     * @return the list of extensions
     */
    public List<ExtensionModule> getExtensions()
    {
        return _extensions;
    }
    
    /**
     * Gets the Unit by class.
     *
     * @param objClass the class
     * @return the Unit
     */
    public Unit<?> getUnit(Class<?> objClass)
    {
        return _units.get(objClass.getName());
    }
    
    /**
     * Gets the Unit by unitClass.
     *
     * @param unitClass the unit class
     * @return the Unit
     * @throws Exception in case of any error
     */
    public abstract Unit<?> getUnit(String unitClass)
        throws Exception;
    
    /**
     * Gets the list of units.
     *
     * @return the list of units
     */
    public List<Unit<?>> getUnits()
    {
        return new ArrayList<Unit<?>>(_units.values());
    }
    
    /**
     * Loads all extensions for the collection of units. If extension implements Filter interface it filters itself using provided filterContext.
     *
     * @param units the collection of units
     * @param filterContext the FilterContext
     * @see com.toolsverse.util.filter.Filter
     */
    public void load(Collection<Unit<?>> units, FilterContext filterContext)
    {
        if (units == null)
            return;
        
        for (Unit<?> unit : units)
        {
            if (!unit.canBeLoadedAgain()
                    && _loadedUnits.containsKey(unit.getClass().getName()))
                continue;
            
            _loadedUnits.put(unit.getClass().getName(), unit.getClass()
                    .getName());
            
            if (unit.startLoad())
            {
                
                List<Class<?>> classes = ClassUtils.getClasses(unit.getPath(),
                        unit.getJarPath(), unit.getClassPath(),
                        unit.getObjClass(), _addedJars);
                
                for (Class<?> clazz : classes)
                {
                    Object obj = null;
                    
                    try
                    {
                        obj = ObjectFactory.instance().get(clazz.getName(),
                                null, null, null, unit.isSingleton(),
                                !unit.allowLoadAll());
                        
                        if (obj == null
                                || (filterContext != null
                                        && obj instanceof Filter && !((Filter)obj)
                                            .filter(filterContext)))
                            continue;
                    }
                    catch (Exception ex)
                    {
                        DefaultExceptionHandler.instance().logException(
                                Logger.SEVERE, getClass(),
                                Resource.ERROR_LOADING_EXT.getValue(), ex);
                        
                        continue;
                    }
                    
                    if (obj instanceof ExtensionModule)
                    {
                        addExtension((ExtensionModule)obj);
                        
                        if (!Utils.isNothing(((ExtensionModule)obj)
                                .getLicensePropertyName()))
                        {
                            if (!LicenseManager.instance().hasLicense(
                                    ((ExtensionModule)obj)
                                            .getLicensePropertyName()))
                            {
                                if (unit.allowLoadAll())
                                    unit.addWithoutLicense(clazz.getName(), obj);
                                
                                continue;
                            }
                        }
                    }
                    
                    unit.add(clazz.getName(), obj);
                }
            }
            
            unit.finishLoad(this);
        }
        
    }
    
    /**
     * Loads all extensions for the registered units. If extension implements Filter interface it filters itself using provided filterContext. 
     *
     * @param filterContext the FilterContext
     */
    public void load(FilterContext filterContext)
    {
        load(_units.values(), filterContext);
    }
    
    /**
     * Loads all extensions for the units located in the classPath. The classPath can be a ";" separated string where each token is an individual class path.
     * If extension implements Filter interface it filters itself using provided filterContext. 
     * The path and jar path are respectfully SystemConfig.instance().getPluginsPath() and SystemConfig.instance().getLibsPath() + "*.jar".
     *
     * @param classPath the class path
     * @param filterContext the filter context
     */
    public void loadOthers(String classPath, FilterContext filterContext)
    {
        if (Utils.isNothing(classPath))
            return;
        
        String[] values = classPath.split(";", -1);
        
        String path = SystemConfig.instance().getPluginsPath();
        
        String jarPath = SystemConfig.instance().getLibsPath() + "*.jar";
        
        Collection<Unit<?>> allUnits = null;
        
        for (String value : values)
        {
            Collection<Unit<?>> units = registerAll(path, jarPath, value,
                    filterContext);
            
            if (allUnits == null)
                allUnits = units;
            else
                allUnits.addAll(units);
        }
        
        if (allUnits != null && allUnits.size() > 0)
            load(allUnits, filterContext);
    }
    
    /**
     * Registers unit, using provided path and jar path.
     *
     * @param unit the unit
     * @param path the path
     * @param jarPath the jar path
     */
    public void register(Unit<?> unit, String path, String jarPath)
    {
        unit.setPath(path);
        
        unit.setJarPath(jarPath);
        
        _units.put(unit.getObjClass().getName(), unit);
    }
    
    /**
     * Registers all units. Unit itself is a dynamically loaded module so it examines path folder first, then jar path and loads all units in the classPath.
     *
     * @param path the path
     * @param jarPath the jar path
     * @param classPath the class path
     * @param filterContext the filter context
     * @return the collection
     */
    public Collection<Unit<?>> registerAll(String path, String jarPath,
            String classPath, FilterContext filterContext)
    {
        if (_registeredPath.containsKey(classPath))
            return null;
        
        _registeredPath.put(classPath, classPath);
        
        List<Class<?>> units = ClassUtils.getClasses(path, jarPath, classPath,
                Unit.class, _addedJars);
        
        List<Unit<?>> regUnits = new ArrayList<Unit<?>>();
        
        for (Class<?> unitClass : units)
        {
            Unit<?> unit = null;
            
            try
            {
                unit = getUnit(unitClass.getName());
                
                if (unit == null
                        || (filterContext != null && unit instanceof Filter && !((Filter)unit)
                                .filter(filterContext)))
                    continue;
                
                register(unit, path, jarPath);
                
                regUnits.add(unit);
            }
            catch (Exception ex)
            {
                DefaultExceptionHandler.instance().logException(Logger.SEVERE,
                        getClass(), Resource.ERROR_LOADING_EXT.getValue(), ex);
                
                continue;
            }
            
        }
        
        return regUnits;
    }
}