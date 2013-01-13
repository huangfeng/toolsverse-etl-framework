/*
 * TextFileMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata.text;

import java.io.InputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.metadata.FileMetadata;
import com.toolsverse.util.FilenameUtils;

/**
 * The implementation of the Metadata interface for the text files.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class TextFileMetadata extends FileMetadata
{
    
    /** The NAME. */
    private static final String NAME = "Text File";
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#getIconPath()
     */
    @Override
    public String getIconPath()
    {
        return "com/toolsverse/etl/metadata/text/images/text_logo.gif";
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getLicensePropertyName()
     */
    public String getLicensePropertyName()
    {
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.Metadata#getName()
     */
    public String getName()
    {
        return NAME;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.FileMetadata#getTablesByType(java.io.InputStream
     * , java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public DataSet getTablesByType(InputStream inputSteam, String name,
            String pattern, String type)
        throws Exception
    {
        return getTablesByType(name, null, null, null);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.metadata.FileMetadata#getTablesByType(java.lang.String
     * , java.lang.String, java.lang.String, java.lang.String)
     */
    @Override
    public DataSet getTablesByType(String catalog, String schema,
            String pattern, String type)
        throws Exception
    {
        DataSet dataSet = new DataSet();
        dataSet.setName(TABLES_DATASET_TYPE);
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName("File");
        fieldDef.setSqlDataType(Types.VARCHAR);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName("Name");
        fieldDef.setSqlDataType(Types.VARCHAR);
        dataSet.addField(fieldDef);
        
        dataSet.setKeyFields("Name");
        
        DataSetRecord record = new DataSetRecord();
        record.add(catalog);
        record.add(FilenameUtils.getBaseName(catalog));
        
        dataSet.addRecord(record);
        
        return dataSet;
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
        
        list.add(TYPE_DATA_SET);
        
        return list;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVendor()
     */
    public String getVendor()
    {
        return SystemConfig.DEFAULT_VENDOR;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getVersion()
     */
    public String getVersion()
    {
        return "3.1";
    }
    
}
