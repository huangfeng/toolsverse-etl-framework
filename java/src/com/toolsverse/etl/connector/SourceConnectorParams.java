/*
 * SourceConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector;

import java.util.Map;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.util.ObjectStorage;

/**
 * The {@link com.toolsverse.etl.connector.DataSetConnectorParams} used by {@link SourceConnector}.
 * 
 * @see com.toolsverse.etl.connector.SourceConnector
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SourceConnectorParams extends DataSetConnectorParams
{
    private DataSet _dataSet;
    
    /**
     * Instantiates a new SourceConnectorParams.
     */
    public SourceConnectorParams()
    {
        _dataSet = null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#alias2alias(com.
     * toolsverse.etl.common.Alias)
     */
    @Override
    public Alias alias2alias(Alias source)
        throws Exception
    {
        return source;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#copy()
     */
    @Override
    public DataSetConnectorParams copy()
    {
        SourceConnectorParams params = new SourceConnectorParams();
        
        params.setDataSet(getDataSet());
        
        return params;
    }
    
    /**
     * Gets data set.
     * @return the data set
     */
    public DataSet getDataSet()
    {
        return _dataSet;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#getInitStr(java.lang
     * .String, com.toolsverse.etl.common.Alias)
     */
    @Override
    public String getInitStr(String name, Alias alias)
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(com.toolsverse
     * .etl.common.Alias)
     */
    @Override
    public void init(Alias alis)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(java.util.Map)
     */
    @Override
    public void init(Map<String, String> props)
    {
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnectorParams#init(com.toolsverse
     * .util.ObjectStorage)
     */
    @Override
    public void init(ObjectStorage storage)
    {
    }
    
    /**
     * Set data set.
     * 
     * @param value the data set
     */
    public void setDataSet(DataSet value)
    {
        _dataSet = value;
    }
    
}
