/*
 * AliasReaderUnit.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.aliasreader;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.ext.loader.Unit;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.util.KeyValue;

/**
 * The implementation of the {@link Unit} interface for the {@link AliasReader}.
 *
 * @author Maksym Sherbinin
 * @version 1.0
 * @since 3.0
 */

public class AliasReaderUnit extends Unit<AliasReader>
{
    
    /** The CLASS_PATH. */
    private static final String CLASS_PATH = "com.toolsverse.etl.aliasreader";
    
    /** The objects. */
    private Map<String, AliasReader> _objects;
    
    /** The list of AliasReaders which can be used by the combo box UI control. */
    private List<KeyValue> _select;
    
    /** The list of AliasReaders drivers even without license which can be used by the combo box UI control. */
    private List<KeyValue> _selectAll;
    
    /**
     * Instantiates a new AliasReader unit.
     */
    public AliasReaderUnit()
    {
        super();
        
        _objects = new LinkedHashMap<String, AliasReader>();
        _select = new ArrayList<KeyValue>();
        _selectAll = new ArrayList<KeyValue>();
        
        _select.add(new KeyValue("", ""));
        _selectAll.add(new KeyValue("", ""));
        
        setObjClass(AliasReader.class);
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
        _objects.put(name, (AliasReader)object);
        
        _select.add(new KeyValue(object.getClass().getName(),
                ((AliasReader)object).getName()));
        _selectAll.add(new KeyValue(object.getClass().getName(),
                ((AliasReader)object).getName()));
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
                ((AliasReader)object).getName()));
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
     * @see com.toolsverse.ext.loader.Unit#getList()
     */
    @Override
    public List<AliasReader> getList()
    {
        return new ArrayList<AliasReader>(_objects.values());
    }
    
    /**
     * Gets the objects.
     *
     * @return the objects
     */
    public Map<String, AliasReader> getObjects()
    {
        return _objects;
    }
    
    /**
     * Gets the the list of AliasReaders which can be used by the combo box UI control.
     *
     * @return the list of AliasReaders
     */
    public List<KeyValue> getSelect()
    {
        return _select;
    }
    
    /**
     * Gets the list used by combo box to select all AliasReaders, event without license.
     *
     * @return the list used by UI control to select all AliasReader, event without license
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