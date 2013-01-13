/*
 * FilterContext.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.filter;

import java.io.Serializable;

/**
 * Sometime it is necessary to filter objects based on execution context. For example we don't want to load objects designed to work
 * in "unit test" mode in production. Implement this empty interface for the classes which must be filtered based on execution context.      
 *
 * @see com.toolsverse.util.filter.Filter
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface FilterContext extends Serializable
{
}
