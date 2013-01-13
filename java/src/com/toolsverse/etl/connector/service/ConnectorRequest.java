/*
 * ConnectorRequest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.service;

import java.io.Serializable;

import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.driver.Driver;

/**
 * The parameter passed to the {@link ConnectorService} methods. Includes particular implementations of the {@link DataSetConnectorParams} 
 * and {@link DataSetConnector}.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConnectorRequest implements Serializable
{
    
    /** The driver. */
    private final Driver _driver;
    
    /** The max rows. */
    private final int _maxRows;
    
    /** The connector class name. */
    private final String _connectorClassName;
    
    /** The connector. */
    private final DataSetConnector<DataSetConnectorParams, ?> _connector;
    
    /** The connector params. */
    private final DataSetConnectorParams _connectorParams;
    
    /** The object name. */
    private final String _objectName;
    
    /** The owner name. */
    private final String _ownerName;
    
    /** The filter */
    private String _filter;
    
    /**
     * Instantiates a new ConnectorRequest.
     *
     * @param driver the driver
     * @param maxRows the maximum number of rows allowed for data set
     * @param connector the connector
     * @param connectorClassName the connector class name
     * @param connectorParams the DataSetConnectorParams
     * @param objectName the object name
     * @param ownerName the owner name
     */
    public ConnectorRequest(Driver driver, int maxRows,
            DataSetConnector<DataSetConnectorParams, ?> connector,
            String connectorClassName, DataSetConnectorParams connectorParams,
            String objectName, String ownerName)
    {
        _driver = driver;
        _maxRows = maxRows;
        _connector = connector;
        _connectorClassName = connectorClassName;
        _connectorParams = connectorParams;
        _objectName = objectName;
        _ownerName = ownerName;
        _filter = null;
    }
    
    /**
     * Gets the connector.
     *
     * @return the connector
     */
    public DataSetConnector<DataSetConnectorParams, ?> getConnector()
    {
        return _connector;
    }
    
    /**
     * Gets the connector class name.
     *
     * @return the connector class name
     */
    public String getConnectorClassName()
    {
        return _connectorClassName;
    }
    
    /**
     * Gets the DataSetConnectorParams.
     *
     * @return the DataSetConnectorParams
     */
    public DataSetConnectorParams getDataSetConnectorParams()
    {
        return _connectorParams;
    }
    
    /**
     * Gets the driver.
     *
     * @return the driver
     */
    public Driver getDriver()
    {
        return _driver;
    }
    
    /** Gets the filter.
     * 
     *  @return the filter
     */
    public String getFilter()
    {
        return _filter;
    }
    
    /**
     * Gets the maximum rows allowed for the data set.
     *
     * @return the maximum rows allowed for the data set
     */
    public int getMaxRows()
    {
        return _maxRows;
    }
    
    /**
     * Gets the object name.
     *
     * @return the object name
     */
    public String getObjectName()
    {
        return _objectName;
    }
    
    /**
     * Gets the owner name.
     *
     * @return the owner name
     */
    public String getOwnerName()
    {
        return _ownerName;
    }
    
    /**
     * Sets the filter.
     * 
     * @param value the filter
     */
    public void setFilter(String value)
    {
        _filter = value;
    }
    
}
