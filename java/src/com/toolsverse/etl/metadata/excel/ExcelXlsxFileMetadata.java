/*
 * ExcelXlsxFileMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata.excel;

import java.io.InputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.xssf.eventusermodel.XSSFReader;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.metadata.FileMetadata;

/**
 * The implementation of the Metadata interface for the xslx Excel files.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelXlsxFileMetadata extends FileMetadata
{
    
    /**
     * The Class SheetReader.
     */
    private class SheetReader
    {
        
        /** The data set. */
        DataSet _dataSet;
        
        /** The file name. */
        String _fName;
        
        /**
         * Instantiates a new sheet reader.
         *
         * @param fName the f name
         * @param dataSet the data set
         */
        public SheetReader(String fName, DataSet dataSet)
        {
            _dataSet = dataSet;
            _fName = fName;
        }
        
        public void process()
            throws Exception
        {
            OPCPackage p = OPCPackage.open(_fName, PackageAccess.READ);
            
            XSSFReader xssfReader = new XSSFReader(p);
            XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator)xssfReader
                    .getSheetsData();
            while (iter.hasNext())
            {
                InputStream stream = iter.next();
                try
                {
                    String sheetName = iter.getSheetName();
                    
                    DataSetRecord dataSetRecord = new DataSetRecord();
                    
                    dataSetRecord.add(_fName);
                    dataSetRecord.add(sheetName);
                    
                    _dataSet.addRecord(dataSetRecord);
                    
                }
                finally
                {
                    stream.close();
                }
            }
            
        }
    }
    
    /** The NAME. */
    private static final String NAME = "Excel (*.xlsx) File";
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.metadata.BaseMetadata#getIconPath()
     */
    @Override
    public String getIconPath()
    {
        return "com/toolsverse/etl/metadata/excel/images/excel_logo.gif";
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
        DataSet dataSet = new DataSet();
        dataSet.setName("tables");
        
        FieldDef fieldDef = new FieldDef();
        fieldDef.setName("File");
        fieldDef.setSqlDataType(Types.VARCHAR);
        dataSet.addField(fieldDef);
        
        fieldDef = new FieldDef();
        fieldDef.setName("Name");
        fieldDef.setSqlDataType(Types.VARCHAR);
        dataSet.addField(fieldDef);
        
        dataSet.setKeyFields("Name");
        
        SheetReader sheetReader = new SheetReader(name, dataSet);
        
        sheetReader.process();
        
        return dataSet;
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
        
        SheetReader sheetReader = new SheetReader(catalog, dataSet);
        
        sheetReader.process();
        
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
        
        list.add(TYPE_WORKSHEETS);
        
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
