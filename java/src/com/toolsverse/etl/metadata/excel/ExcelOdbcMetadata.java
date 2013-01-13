/*
 * ExcelOdbcMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata.excel;

import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.List;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.metadata.JdbcMetadata;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ListHashMap;

/**
 * The Excel odbc implementation of the Metadata interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelOdbcMetadata extends JdbcMetadata
{
    
    /** The NAME. */
    private static final String NAME = "Excel Odbc";
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.JdbcMetadata#getDbObjectTypes()
     */
    @Override
    public List<String> getDbObjectTypes()
        throws Exception
    {
        List<String> types = getTableTypes();
        
        return types;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.BaseMetadata#getFullObjectName(java.lang.
     * String)
     */
    @Override
    public String getFullObjectName(String pattern)
    {
        if (pattern == null)
            return null;
        
        return SqlUtils.getValue(pattern, 1, METADATA_DELIMITER);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
     */
    @Override
    public String getLicensePropertyName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.JdbcMetadata#getName()
     */
    @Override
    public String getName()
    {
        return NAME;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.JdbcMetadata#getTableMetadataTypes()
     */
    @Override
    public List<String> getTableMetadataTypes()
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_COLUMNS);
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.techrx.tools.admin.metadata.impl.JdbcMetadata#getTablesByType(java
     * .lang.String, java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public DataSet getTablesByType(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        if (getConnectionParamsProvider() == null || getDriver() == null)
            return null;
        
        DbMetadata dbMetadata = null;
        
        try
        {
            
            dbMetadata = getDbMetadata();
            
            DataSet dataSet = null;
            
            ResultSet rs = dbMetadata.getDatabaseMetaData().getTables(catalog,
                    schema, null, null);
            
            ListHashMap<String, String> mapping = new ListHashMap<String, String>();
            mapping.put("TABLE_CAT", "Catalog");
            mapping.put("TABLE_SCHEM", "Schema");
            mapping.put("TABLE_NAME", "Name");
            mapping.put("TABLE_TYPE", "Type");
            mapping.put("REMARKS", "Remarks");
            
            dataSet = new DataSet();
            dataSet.setName(TABLES_DATASET_TYPE);
            dataSet.setKeyFields("Name");
            
            SqlUtils.populateDataSet(dataSet, getDriver(), rs, mapping, false,
                    true, null, null);
            
            return dataSet;
        }
        finally
        {
            dbMetadata.free();
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.FileMetadata#getTableTypes()
     */
    @Override
    public List<String> getTableTypes()
        throws Exception
    {
        List<String> list = new ArrayList<String>();
        
        list.add(TYPE_WORKSHEETS);
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVendor()
     */
    @Override
    public String getVendor()
    {
        return SystemConfig.DEFAULT_VENDOR;
    }
    
}
