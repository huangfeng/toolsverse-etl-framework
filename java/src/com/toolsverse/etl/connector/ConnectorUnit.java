/*
 * ConnectorUnit.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.ext.ExtensionModule;
import com.toolsverse.ext.loader.Unit;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.util.KeyValue;

/**
 * The {@link com.toolsverse.ext.loader.Unit} for the {@link com.toolsverse.etl.connector.DataSetConnector}.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConnectorUnit extends Unit<DataSetConnector<?, ?>>
{
    
    /** The CLASS_PATH. */
    private static final String CLASS_PATH = "com.toolsverse.etl.connector";
    
    /** The objects. */
    private final Map<String, DataSetConnector<?, ?>> _objects;
    
    /** The list used by combo box UI control to select available connectors. */
    private final List<KeyValue> _select;
    
    /** The list of connectors even without license which can be used by the combo box UI control. */
    private List<KeyValue> _selectAll;
    
    /**
     * Instantiates a new ConnectorUnit.
     */
    public ConnectorUnit()
    {
        super();
        
        _objects = new LinkedHashMap<String, DataSetConnector<?, ?>>();
        _select = new ArrayList<KeyValue>();
        _selectAll = new ArrayList<KeyValue>();
        
        setObjClass(DataSetConnector.class);
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
        if (!(object instanceof ExtensionModule))
            return;
        
        _objects.put(name, (DataSetConnector<?, ?>)object);
        
        _select.add(new KeyValue(object.getClass().getName(),
                ((DataSetConnector<?, ?>)object).getName()));
        _selectAll.add(new KeyValue(object.getClass().getName(),
                ((DataSetConnector<?, ?>)object).getName()));
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
                ((DataSetConnector<?, ?>)object).getName()));
        
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
    public List<DataSetConnector<?, ?>> getList()
    {
        return new ArrayList<DataSetConnector<?, ?>>(_objects.values());
    }
    
    /**
     * Gets the map of the data set connectors. The key is name and the value is a an instance of the DataSetConnector. 
     *
     * @return the the data set connectors
     */
    public Map<String, DataSetConnector<?, ?>> getObjects()
    {
        return _objects;
    }
    
    /**
     * Gets the list used by combo box to select available connectors.
     *
     * @return the list used by UI control to select available connectors
     */
    public List<KeyValue> getSelect()
    {
        return _select;
    }
    
    /**
     * Gets the list used by combo box to select all connectors, event without license.
     *
     * @return the list used by UI control to select all connectors, event without license
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
        return false;
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