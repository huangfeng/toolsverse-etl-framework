/*
 * DriverUnit.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.ext.loader.Unit;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.util.KeyValue;

/**
 * The implementation of the {@link Unit} for the {@link Driver}. It "knows"
 * that ETL drivers belong to com.toolsverse.etl.driver package and sub
 * packages, should be loaded once, can not be reloaded, etc.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class DriverUnit extends Unit<Driver> implements DriverDiscovery
{
    
    /** The root class path. */
    private static final String CLASS_PATH = "com.toolsverse.etl.driver";
    
    /** The objects. */
    private final Map<String, Driver> _objects;
    
    /**
     * The list of drivers which can be used by combo box UI control to select
     * available drivers.
     */
    private final List<KeyValue> _select;
    
    /** The list of ETL drivers even without license which can be used by the combo box UI control. */
    private List<KeyValue> _selectAll;
    
    /**
     * Instantiates a new driver unit.
     */
    public DriverUnit()
    {
        super();
        
        _objects = new LinkedHashMap<String, Driver>();
        _select = new ArrayList<KeyValue>();
        _selectAll = new ArrayList<KeyValue>();
        
        setObjClass(Driver.class);
        setClassPath(CLASS_PATH);
        setSingleton(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#add(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void add(String name, Object object)
    {
        _objects.put(name, (Driver)object);
        
        _select.add(new KeyValue(object.getClass().getName(), ((Driver)object)
                .getName()));
        _selectAll.add(new KeyValue(object.getClass().getName(),
                ((Driver)object).getName()));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#addWithoutLicense(java.lang.String,
     * java.lang.Object)
     */
    @Override
    public void addWithoutLicense(String name, Object object)
    {
        _selectAll.add(new KeyValue(object.getClass().getName(),
                ((Driver)object).getName()));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#allowLoadAll()
     */
    @Override
    public boolean allowLoadAll()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#canBeLoadedAgain()
     */
    @Override
    public boolean canBeLoadedAgain()
    {
        return false;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#finishLoad(com.toolsverse.ext.loader.
     * UnitLoader)
     */
    @Override
    public void finishLoad(UnitLoader loader)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#free()
     */
    @Override
    public void free()
    {
        _objects.clear();
        _select.clear();
        _selectAll.clear();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.driver.DriverDiscovery#getDriverByJdbcClassName(java
     * .lang.String)
     */
    public Driver getDriverByJdbcClassName(String jdbcClassName)
    {
        if (_objects == null || _objects.size() == 0)
            return null;
        
        List<Driver> drivers = new ArrayList<Driver>();
        
        for (Driver driver : _objects.values())
        {
            if (jdbcClassName.equals(driver.getJdbcDriverClassName()))
                drivers.add(driver);
        }
        
        return drivers.size() == 1 ? drivers.get(0) : null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#getList()
     */
    @Override
    public List<Driver> getList()
    {
        return new ArrayList<Driver>(_objects.values());
    }
    
    /**
     * Gets the objects.
     * 
     * @return the objects
     */
    public Map<String, Driver> getObjects()
    {
        return _objects;
    }
    
    /**
     * Gets the list of drivers which can be used by combo box UI control to
     * select available drivers.
     * 
     * @return the list of drivers
     */
    public List<KeyValue> getSelect()
    {
        return _select;
    }
    
    /**
     * Gets the list used by combo box to select all etl drivers, event without license.
     *
     * @return the list used by UI control to select all etl drivers, event without license
     */
    public List<KeyValue> getSelectAll()
    {
        return _selectAll;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#loadsConfigurableExtensions()
     */
    @Override
    public boolean loadsConfigurableExtensions()
    {
        return true;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.Unit#startLoad()
     */
    @Override
    public boolean startLoad()
    {
        return true;
    }
    
}