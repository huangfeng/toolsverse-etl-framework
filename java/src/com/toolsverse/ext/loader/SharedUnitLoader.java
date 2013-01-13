/*
 * SharedUnitLoader.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext.loader;

import com.toolsverse.util.factory.ObjectFactory;

/**
 * This class is a UnitLoader designed to load only extensions which can be shared between sessions. If extension does not have 
 * session specific state - this is a class to load it. The class is a singleton.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SharedUnitLoader extends UnitLoader
{
    // volatile is needed so that multiple thread can reconcile the instance
    /** The _instance. */
    private volatile static SharedUnitLoader _instance;
    
    /**
     * Instance of SharedUnitLoader
     *
     * @return the shared unit loader
     */
    public static SharedUnitLoader instance()
    {
        if (_instance == null)
        {
            synchronized (SharedUnitLoader.class)
            {
                if (_instance == null)
                    _instance = new SharedUnitLoader();
            }
        }
        
        return _instance;
    }
    
    /**
     * Instantiates a new SharedUnitLoader.
     */
    private SharedUnitLoader()
    {
        super();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.loader.UnitLoader#getUnit(java.lang.String)
     */
    @Override
    public Unit<?> getUnit(String unitClass)
        throws Exception
    {
        return (Unit<?>)ObjectFactory.instance().get(unitClass, true);
    }
}