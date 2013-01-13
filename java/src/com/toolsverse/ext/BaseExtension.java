/*
 * BaseExtension.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext;

import java.io.Serializable;

/**
 * The abstract implementation of the ExtensionModule
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class BaseExtension implements ExtensionModule, Serializable
{
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Comparable#compareTo(java.lang.Object)
     */
    public int compareTo(ExtensionModule ext)
    {
        return ExtUtils.compareTo(this, ext);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getConfigFileName()
     */
    public String getConfigFileName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getIconPath()
     */
    public String getIconPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLocalUnitClassPath()
     */
    public String getLocalUnitClassPath()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getXmlConfigFileName()
     */
    public String getXmlConfigFileName()
    {
        return null;
    }
    
}
