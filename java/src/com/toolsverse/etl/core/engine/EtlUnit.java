/*
 * EtlUnit.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;

/**
 * This data structure used to separate multiple "load" instances executed in
 * parallel.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlUnit
{
    
    /** The connection name. */
    private final String _connectionName;
    
    /** The driver name. */
    private final String _driverClassName;
    
    /** The data writer. */
    private DataSetConnector<DataSetConnectorParams, ?> _writer;
    
    /** The data writer params. */
    private DataSetConnectorParams _writerParams;
    
    /**
     * Instantiates a new etl unit.
     * 
     * @param connectionName
     *            the connection name
     * @param driverClassName
     *            the driver class name
     * @param writer
     *            the data writer
     * @param writerParams
     *            the data writer params
     */
    public EtlUnit(String connectionName, String driverClassName,
            DataSetConnector<DataSetConnectorParams, ?> writer,
            DataSetConnectorParams writerParams)
    {
        _connectionName = connectionName;
        _driverClassName = driverClassName;
        _writer = writer;
        _writerParams = writerParams;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#equals(java.lang.Object)
     */
    @Override
    public boolean equals(Object object)
    {
        if (!(object instanceof EtlUnit))
            return false;
        
        return object.toString().equalsIgnoreCase(toString());
    }
    
    /**
     * Gets the connection name.
     * 
     * @return the connection name
     */
    public String getConnectionName()
    {
        return _connectionName;
    }
    
    /**
     * Gets the driver class name.
     * 
     * @return the driver class name
     */
    public String getDriverClassName()
    {
        return _driverClassName;
    }
    
    /**
     * Gets the data writer.
     * 
     * @return the data writer
     */
    public DataSetConnector<DataSetConnectorParams, ?> getWriter()
    {
        return _writer;
    }
    
    /**
     * Gets the data writer params.
     * 
     * @return the data writer params
     */
    public DataSetConnectorParams getWriterParams()
    {
        return _writerParams;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#hashCode()
     */
    @Override
    public int hashCode()
    {
        return toString().hashCode();
    }
    
    /**
     * Sets the data writer.
     * 
     * @param value
     *            the new data writer
     */
    public void setWriter(DataSetConnector<DataSetConnectorParams, ?> value)
    {
        _writer = value;
    }
    
    /**
     * Sets the data writer params.
     * 
     * @param value
     *            the new data writer params
     */
    public void setWriterParams(DataSetConnectorParams value)
    {
        _writerParams = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#toString()
     */
    @Override
    public String toString()
    {
        return _connectionName + ":" + _driverClassName;
    }
    
}
