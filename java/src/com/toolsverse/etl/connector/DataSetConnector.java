/*
 * DataSetConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.io.Serializable;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.driver.Driver;

/**
 * The class which populates and persists {@link com.toolsverse.etl.common.DataSet} must implement <code>DataSetConnector</code> interface.
 * Examples: TextConnector, XmlConnector, SqlConnector, etc.
 *
 * @param <P> the generic DataSetConnectorParams type
 * @param <R> the generic ConnectorResult type
 * @see com.toolsverse.etl.connector.DataSetConnectorParams
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface DataSetConnector<P extends DataSetConnectorParams, R extends ConnectorResult>
        extends Serializable
{
    
    /**
     * Cleans up after DataSetConnector populated or persisted DataSet. Called regardless of exception.
     * Usually used to free allocated resources such as streams, handlers, etc.  
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @throws Exception in case of any error
     */
    void cleanUp(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
    /**
     * Gets the DataSetConnectorParams.
     *
     * @return the DataSetConnectorParams
     */
    P getDataSetConnectorParams();
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    String getName();
    
    /**
     * Persists data set row. For example adds line to the text file.
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @param record the record
     * @param row the row
     * @param records the total number of records
     * @throws Exception in case of any error
     */
    void inlinePersist(P params, DataSet dataSet, Driver driver,
            DataSetRecord record, int row, int records)
        throws Exception;
    
    /**
     * Persists data set.
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @return the ConnectorResult
     * @throws Exception in case of any error
     */
    ConnectorResult persist(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
    /**
     * Populates data set.
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @return the ConnectorResult
     * @throws Exception in case of any error
     */
    ConnectorResult populate(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
    /**
     * Called when DataSetConnector finished persisting data set. Never called if there was an exception.
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @throws Exception in case of any error
     */
    void postPersist(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
    /**
     * Called when DataSetConnector starts persisting data set. 
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @throws Exception in case of any error
     */
    void prePersist(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
    /**
     * Tests the connection.
     *
     * @param connectionParams the connection params
     * @return the status. If null or empty connection is ok
     * @throws Exception the exception in case of any error
     */
    String testConnection(ConnectionParams connectionParams)
        throws Exception;
    
    /**
     * Writes meta data. For example along with delimited text file the DataSetConnector can create a xml file with the same name which contains field definitions.   
     *
     * @param params the DataSetConnectorParams
     * @param dataSet the data set
     * @param driver the driver
     * @return the ConnectorResult
     * @throws Exception in case of any error
     */
    ConnectorResult writeMetaData(P params, DataSet dataSet, Driver driver)
        throws Exception;
    
}
