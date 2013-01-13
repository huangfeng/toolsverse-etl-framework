/*
 * DriverDiscovery.java
 * 
 * Copyright 2010-2012 Toolsverse . All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.driver;

/**
 * The instance of this interface discovers a Driver by jdbc class name and
 * other parameters.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface DriverDiscovery
{
    /**
     * Gets the driver by jdbc class name.
     * 
     * @param jdbcClassName
     *            the jdbc class name
     * @return the driver
     */
    Driver getDriverByJdbcClassName(String jdbcClassName);
    
}
