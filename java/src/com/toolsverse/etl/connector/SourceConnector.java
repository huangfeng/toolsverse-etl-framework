/*
 * SourceConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.connector.sql.SqlConnectorParams;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.log.Logger;

/**
 * Reads data from the exising data set. Supports data streaming.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SourceConnector implements
        DataSetConnector<SourceConnectorParams, ConnectorResult>
{
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#cleanUp(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void cleanUp(SourceConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#getDataSetConnectorParams()
     */
    public SourceConnectorParams getDataSetConnectorParams()
    {
        return new SourceConnectorParams();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnector#getName()
     */
    public String getName()
    {
        return "Source";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#inlinePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver,
     * com.toolsverse.etl.common.DataSetRecord, int, int)
     */
    public void inlinePersist(SourceConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#inlinePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver,
     * com.toolsverse.etl.common.DataSetRecord, int, int)
     */
    public void inlinePersist(SqlConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#persist(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult persist(SourceConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#populate(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult populate(SourceConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        if (params == null || dataSet == null || params.getDataSet() == null)
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null || params.getDataSet() == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
                        .getValue());
            if (params == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_PARAMS_NULL
                        .getValue());
            
            return result;
        }
        
        dataSet.clear();
        
        dataSet.setFields(params.getDataSet().getFields());
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.POPULATING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        try
        {
            // before something
            if (params.getBeforeCallback() != null)
                params.getBeforeCallback().onBefore(dataSet, driver);
            
            int rowCount = params.getDataSet().getRecordCount();
            
            for (int index = 0; index < rowCount; index++)
            {
                if (!params.isSilent() && params.getLogStep() > 0
                        && (index % params.getLogStep()) == 0)
                    Logger.log(
                            Logger.INFO,
                            EtlLogger.class,
                            dataSet.getName()
                                    + ": "
                                    + index
                                    + EtlResource.READING_DATASET_MSG
                                            .getValue());
                
                DataSetRecord record = params.getDataSet().getRecord(index);
                
                dataSet.addRecord(record);
                
                // inline something
                if (params.getAddRecordCallback() != null)
                    params.getAddRecordCallback().onAddRecord(dataSet, driver,
                            record, index - 1);
            }
            
        }
        finally
        {
            if (params.getAfterCallback() != null)
                params.getAfterCallback().onAfter(dataSet, driver);
        }
        
        return new ConnectorResult();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#postPersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void postPersist(SourceConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#prePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void prePersist(SourceConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#testConnection(com.toolsverse
     * .etl.common.ConnectionParams)
     */
    public String testConnection(ConnectionParams connectionParams)
        throws Exception
    {
        return "";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#writeMetaData(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult writeMetaData(SourceConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        return new ConnectorResult();
    }
    
}
