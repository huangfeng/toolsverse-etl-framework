/*
 * LoadFunctionContext.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import com.toolsverse.etl.common.FunctionContext;
import com.toolsverse.etl.core.config.EtlConfig;

/**
 * <code>LoadFunctionContext</code> is the class that represents execution context, used
 * primarily by etl functions. 
 * 
 * @see com.toolsverse.etl.common.Function 
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class LoadFunctionContext extends FunctionContext
{
    
    /** The destination. */
    private Destination _destination;
    
    /** The etl config. */
    private EtlConfig _config;
    
    /**
     * Instantiates a new load function context.
     */
    public LoadFunctionContext()
    {
        super();
        
        _destination = null;
        _config = null;
    }
    
    /**
     * Gets the etl config associated with the current etl process.
     * 
     * @return the etl config
     */
    public EtlConfig getConfig()
    {
        return _config;
    }
    
    /**
     * Gets the current <code>Destination</code>.
     * 
     * @return the destination
     */
    public Destination getDestination()
    {
        return _destination;
    }
    
    /**
     * Sets the etl config associated with the current etl process.
     * 
     * @param value
     *            The new etl config
     */
    public void setConfig(EtlConfig value)
    {
        _config = value;
    }
    
    /**
     * Sets the current <code>Destination</code>.
     * 
     * @param value
     *            The new destination
     */
    public void setDestination(Destination value)
    {
        _destination = value;
    }
}
