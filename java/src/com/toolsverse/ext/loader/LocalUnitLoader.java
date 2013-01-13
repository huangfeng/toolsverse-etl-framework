/*
 * LocalUnitLoader.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext.loader;

import com.toolsverse.util.factory.ObjectFactory;

/**
 * This class is a UnitLoader designed to load only extensions which can NOT be shared between sessions. If extension does have a 
 * session specific state - this is a class to load it. Compare to SharedUnitLoader this class is not a singleton and must be 
 * instantiated for each session separately.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class LocalUnitLoader extends UnitLoader
{
    
    /**
     * Instantiates a new LocalUnitLoader.
     */
    public LocalUnitLoader()
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
        return (Unit<?>)ObjectFactory.instance().get(unitClass, false);
    }
}