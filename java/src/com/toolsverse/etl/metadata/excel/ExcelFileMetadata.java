/*
 * ExcelFileMetadata.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.metadata.excel;

import java.io.FileInputStream;
import java.io.InputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.List;

import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.metadata.FileMetadata;
import com.toolsverse.util.Utils;

/**
 * The implementation of the Metadata interface for the Excel files.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelFileMetadata extends FileMetadata
{
    
    /**
     * The Class SheetReader.
     */
    private class SheetReader implements HSSFListener
    {
        
        /** The data set. */
        DataSet _dataSet;
        
        /** The file name. */
        String _fName;
        
        /** The found sheet flag. */
        boolean _foundSheet;
        
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
            _foundSheet = false;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * org.apache.poi.hssf.eventusermodel.HSSFListener#processRecord(org
         * .apache.poi.hssf.record.Record)
         */
        public void processRecord(Record record)
        {
            switch (record.getSid())
            {
                case BoundSheetRecord.sid:
                    BoundSheetRecord bsrecord = (BoundSheetRecord)record;
                    
                    DataSetRecord dataSetRecord = new DataSetRecord();
                    
                    dataSetRecord.add(_fName);
                    dataSetRecord.add(bsrecord.getSheetname());
                    
                    _dataSet.addRecord(dataSetRecord);
                    
                    _foundSheet = true;
                    
                    break;
                default:
                    if (_foundSheet)
                        throw new RuntimeException(SHEETS_EXTRACTED_EXCEPTION);
            }
            
        }
    }
    
    /** The NAME. */
    private static final String NAME = "Excel (*.xls) File";
    
    /** The SHEETS_EXTRACTED_EXCEPTION. */
    public static final String SHEETS_EXTRACTED_EXCEPTION = "All sheets alreday extracted";
    
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
        
        InputStream din = null;
        
        try
        {
            POIFSFileSystem poifs = new POIFSFileSystem(inputSteam);
            din = poifs.createDocumentInputStream("Workbook");
            HSSFRequest req = new HSSFRequest();
            
            req.addListenerForAllRecords(new SheetReader(name, dataSet));
            HSSFEventFactory factory = new HSSFEventFactory();
            
            try
            {
                factory.processEvents(req, din);
            }
            catch (Exception ex)
            {
                if (!Utils
                        .isParticularException(ex, SHEETS_EXTRACTED_EXCEPTION))
                    throw ex;
            }
        }
        finally
        {
            if (din != null)
                din.close();
        }
        
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
        
        FileInputStream fin = null;
        InputStream din = null;
        
        try
        {
            fin = new FileInputStream(catalog);
            POIFSFileSystem poifs = new POIFSFileSystem(fin);
            din = poifs.createDocumentInputStream("Workbook");
            HSSFRequest req = new HSSFRequest();
            
            req.addListenerForAllRecords(new SheetReader(catalog, dataSet));
            HSSFEventFactory factory = new HSSFEventFactory();
            
            try
            {
                factory.processEvents(req, din);
            }
            catch (Exception ex)
            {
                if (!Utils
                        .isParticularException(ex, SHEETS_EXTRACTED_EXCEPTION))
                    throw ex;
            }
        }
        finally
        {
            if (fin != null)
                fin.close();
            if (din != null)
                din.close();
        }
        
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
