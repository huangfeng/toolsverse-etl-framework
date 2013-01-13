/*
 * ExtensionModule.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.ext;

/**
 * The Toolseverse Foundation framework is design to be extensible. The extension modules (plug ins) can be loaded and configured dynamically at run time. 
 * If class is an extension module of any type it must implement ExtensionModule interface. 
 * Examples: apps, editors, formatters, etc.
 * 
 * @see com.toolsverse.ext.loader.Unit
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ExtensionModule extends Comparable<ExtensionModule>
{
    /**
     * Gets the path to the configuration file for the module. The configuration file contains module specific properties, 
     * resides inside package and deployed with the code in the jar file.
     *
     * @return the path to the configuration name
     */
    String getConfigFileName();
    
    /**
     * Gets the display name. This name will be used to display module in the various UI controls.
     *
     * @return the display name
     */
    String getDisplayName();
    
    /**
     * Gets the full path to the icon file. The icon reside inside package and deployed with the code in jar file.
     *
     * @return the icon path
     */
    String getIconPath();
    
    /**
     * Gets the name of the license property. If not null the model is a subject of licensing.  
     * 
     * @return the name of the license property
     */
    String getLicensePropertyName();
    
    /**
     * If extension module is a "local" it must return the path to it's corresponding Unit. Otherwise returns null.
     * The "local" extensions is loaded for each session independently.
     *
     * @return the local unit class path
     * @see com.toolsverse.ext.loader.Unit
     */
    String getLocalUnitClassPath();
    
    /**
     * Gets the type of the extension module.
     *
     * @return the type
     */
    String getType();
    
    /**
     * Gets the vendor.
     *
     * @return the vendor
     */
    String getVendor();
    
    /**
     * Gets the version of the extension module.
     *
     * @return the version
     */
    String getVersion();
    
    /**
     * Gets the path to the xml configuration file for the module. The xml configuration file usually contains module specific UI elements, 
     * resides inside package and deployed with the code in the jar file.
     *
     * @return the path to the configuration name
     */
    String getXmlConfigFileName();
}
