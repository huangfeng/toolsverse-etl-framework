/*
 * ConnectorServiceImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.service;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;

import com.toolsverse.etl.common.CommonEtlUtils;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.connector.ConnectorResult;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorParams;
import com.toolsverse.etl.connector.FileConnectorResource;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.concurrent.ParallelExecutor;
import com.toolsverse.util.factory.ObjectFactory;

/**
 * The default implementation of the {@link ConnectorService} interface.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ConnectorServiceImpl implements ConnectorService
{
    
    /**
     * The ConnectorTask.
     */
    private class ConnectorTask implements Callable<ConnectorResult>
    {
        
        /** The connector. */
        DataSetConnector<DataSetConnectorParams, ?> _connector;
        
        /** The params. */
        FileConnectorParams _params;
        
        /** The data set. */
        DataSet _dataSet;
        
        /** The driver. */
        Driver _driver;
        
        /**
         * Instantiates a new connector task.
         *
         * @param connector the connector
         * @param params the params
         * @param driver the driver
         * @param dataSet the data set
         */
        public ConnectorTask(
                DataSetConnector<DataSetConnectorParams, ?> connector,
                FileConnectorParams params, Driver driver, DataSet dataSet)
        {
            _connector = connector;
            _params = params;
            _dataSet = dataSet;
            _driver = driver;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public ConnectorResult call()
            throws Exception
        {
            return _connector.persist(_params, _dataSet, _driver);
        }
    }
    
    /**
     * Persists data set.
     *
     * @param request the request
     * @param dataSet the data set
     * @return the connector result
     * @throws Exception the exception
     */
    @SuppressWarnings("unchecked")
    public ConnectorResult persistDataSet(ConnectorRequest request,
            DataSet dataSet)
        throws Exception
    {
        DataSetConnector<DataSetConnectorParams, ?> connector = request
                .getConnector() != null ? request.getConnector()
                : (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                        .instance().get(request.getConnectorClassName(), true);
        
        DataSetConnectorParams params = request.getDataSetConnectorParams();
        
        if (params instanceof FileConnectorParams)
        {
            if (!Utils.isNothing(((FileConnectorParams)params).getSplitBy()))
            {
                LinkedHashMap<String, DataSet> map = CommonEtlUtils.split(
                        dataSet, ((FileConnectorParams)params).getSplitBy());
                
                if (map != null
                        && !map.containsKey(LinkedHashMap.class.getName()))
                {
                    String fileName = ((FileConnectorParams)params)
                            .getFileName();
                    
                    ParallelExecutor executor = new ParallelExecutor(
                            map.size() <= 5 ? map.size() : 5);
                    
                    int count = 0;
                    
                    for (String key : map.keySet())
                    {
                        DataSet ds = map.get(key);
                        
                        if (ds == null)
                            continue;
                        
                        count++;
                        
                        FileConnectorParams fileParams = (FileConnectorParams)params
                                .copy();
                        
                        if (!Utils.isNothing(fileName))
                        {
                            fileParams.setFileName(FileUtils.changeFileName(
                                    fileName, fileName + "_" + key));
                        }
                        
                        ConnectorTask task = new ConnectorTask(connector,
                                fileParams, request.getDriver(), ds);
                        
                        executor.addTask(task);
                    }
                    
                    try
                    {
                        executor.waitUntilDone();
                        
                        List<Future<?>> results = executor.getResults();
                        
                        ConnectorResult connectorResult = null;
                        
                        for (Future<?> result : results)
                        {
                            Object execres = result.get();
                            
                            if (execres instanceof ConnectorResult)
                            {
                                connectorResult = (ConnectorResult)execres;
                                
                                if (connectorResult.getRetCode() != ConnectorResult.OK_CODE)
                                    return connectorResult;
                            }
                        }
                        
                        if (results.size() == 1 && connectorResult != null)
                            return connectorResult;
                    }
                    finally
                    {
                        executor.terminate();
                    }
                    
                    ConnectorResult res = new ConnectorResult();
                    
                    res.setResult(Utils.format(
                            FileConnectorResource.FILES_PERSISTED.getValue(),
                            new String[] {String.valueOf(count)}));
                    
                    return res;
                }
            }
        }
        
        return connector.persist(params, dataSet, request.getDriver());
    }
    
    /**
     * Populates data set.
     *
     * @param request the request
     * @return the data set
     * @throws Exception the exception
     */
    @SuppressWarnings("unchecked")
    public DataSet populateDataSet(ConnectorRequest request)
        throws Exception
    {
        DataSetConnector<DataSetConnectorParams, ?> connector = request
                .getConnector() != null ? request.getConnector()
                : (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                        .instance().get(request.getConnectorClassName(), true);
        
        DataSet dataSet = new DataSet();
        dataSet.setName(request.getObjectName());
        dataSet.setOwnerName(request.getOwnerName());
        dataSet.setFilter(request.getFilter());
        
        request.getDataSetConnectorParams().setMaxRows(request.getMaxRows());
        
        connector.populate(request.getDataSetConnectorParams(), dataSet,
                request.getDriver());
        
        return dataSet;
    }
    
}
