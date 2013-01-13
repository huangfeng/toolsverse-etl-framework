/*
 * ConnectionParamsProvider.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.common;

/**
 * The provider of the {@link ConnectionParams}.
 *
 * @see com.toolsverse.etl.common.Alias
 *
 * @param <C> the generic type
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface ConnectionParamsProvider<C extends ConnectionParams>
{
    
    /**
     * Gets the connection parameters.
     *
     * @return the connection parameters
     */
    C getConnectionParams();
}
