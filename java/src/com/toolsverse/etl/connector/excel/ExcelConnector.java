/*
 * ExcelConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.excel;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.poi.hssf.eventusermodel.FormatTrackingHSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFEventFactory;
import org.apache.poi.hssf.eventusermodel.HSSFListener;
import org.apache.poi.hssf.eventusermodel.HSSFRequest;
import org.apache.poi.hssf.eventusermodel.MissingRecordAwareHSSFListener;
import org.apache.poi.hssf.eventusermodel.dummyrecord.LastCellOfRowDummyRecord;
import org.apache.poi.hssf.eventusermodel.dummyrecord.MissingCellDummyRecord;
import org.apache.poi.hssf.record.BOFRecord;
import org.apache.poi.hssf.record.BlankRecord;
import org.apache.poi.hssf.record.BoolErrRecord;
import org.apache.poi.hssf.record.BoundSheetRecord;
import org.apache.poi.hssf.record.FormulaRecord;
import org.apache.poi.hssf.record.LabelRecord;
import org.apache.poi.hssf.record.LabelSSTRecord;
import org.apache.poi.hssf.record.NoteRecord;
import org.apache.poi.hssf.record.NumberRecord;
import org.apache.poi.hssf.record.RKRecord;
import org.apache.poi.hssf.record.Record;
import org.apache.poi.hssf.record.SSTRecord;
import org.apache.poi.hssf.record.StringRecord;
import org.apache.poi.hssf.usermodel.HSSFWorkbook;
import org.apache.poi.poifs.filesystem.POIFSFileSystem;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.connector.BaseDataSetConnector;
import com.toolsverse.etl.connector.ConnectorResource;
import com.toolsverse.etl.connector.ConnectorResult;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorResource;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * Reads and writes Microsoft xls files using apache.poi library. Supports data streaming.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelConnector extends BaseDataSetConnector implements
        DataSetConnector<ExcelConnectorParams, ConnectorResult>
{
    
    /**
     * The Class XlsProcessor.
     */
    public class XlsProcessor implements HSSFListener
    {
        
        /** The last row number. */
        private int _lastRowNumber;
        
        // Records we pick up as we process
        /** The sst record. */
        private SSTRecord _sstRecord;
        
        /** The next row. */
        private int _nextRow;
        
        /** The next column. */
        private int _nextColumn;
        
        /** The format listener. */
        private FormatTrackingHSSFListener _formatListener;
        
        /** The output next string record. */
        private boolean _outputNextStringRecord;
        
        /** The params. */
        private final ExcelConnectorParams _params;
        
        /** The data set. */
        private final DataSet _dataSet;
        
        /** The driver. */
        private final Driver _driver;
        
        /** The _types. */
        private final Map<Integer, Boolean> _types;
        
        /** The sheet names. */
        private final List<String> _sheetNames;
        
        /** The current sheet index. */
        private int _currentSheetIndex;
        
        /** The data set record. */
        private DataSetRecord _dataSetRecord;
        
        /** The index. */
        private int _index;
        
        /** The "has key" flag. */
        private final boolean _hasKey;
        
        /** The sheet name. */
        private final String _sheetName;
        
        /** The "sheet found" flag. */
        private boolean _sheetFound;
        
        /**
         * Instantiates a new XlsProcessor.
         *
         * @param params the parameters
         * @param dataSet the data set
         * @param driver the driver
         */
        public XlsProcessor(ExcelConnectorParams params, DataSet dataSet,
                Driver driver)
        {
            super();
            
            _params = params;
            _dataSet = dataSet;
            _driver = driver;
            
            _types = new HashMap<Integer, Boolean>();
            
            _lastRowNumber = -1;
            _dataSetRecord = null;
            
            _index = 1;
            
            _sheetNames = new ArrayList<String>();
            
            _currentSheetIndex = -1;
            
            _hasKey = !Utils.isNothing(dataSet.getKeyFields());
            
            _sheetName = (!Utils.isNothing(dataSet.getOwnerName())
                    && !Utils.isNothing(dataSet.getObjectName()) || Utils
                    .isNothing(_params.getSheetName())) ? dataSet.getName()
                    : params.getSheetName();
            
            _sheetFound = false;
        }
        
        /**
         * Checks if "has key" flag was set.
         *
         * @return true, if successful
         */
        public boolean hasKey()
        {
            return _hasKey;
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
            int row = -1;
            int column = -1;
            int fType = Types.VARCHAR;
            Object cellValue = null;
            boolean isNewValue = false;
            TypedKeyValue<Integer, Number> typeAndValue;
            
            if (BOFRecord.sid == record.getSid())
            {
                BOFRecord bof = (BOFRecord)record;
                
                if (bof.getType() == BOFRecord.TYPE_WORKSHEET)
                {
                    _currentSheetIndex++;
                }
            }
            
            boolean isFound = _currentSheetIndex >= 0
                    && _currentSheetIndex == _sheetNames.indexOf(_sheetName);
            
            _sheetFound = _sheetFound || isFound;
            
            if (_currentSheetIndex >= 0 && !isFound)
            {
                if (!_sheetFound)
                    return;
                
                throw new RuntimeException(
                        ExcelConnectorParams.SHEET_ALREADY_EXTRACTED_EXCEPTION);
            }
            
            switch (record.getSid())
            {
                case BoundSheetRecord.sid:
                    BoundSheetRecord bsr = (BoundSheetRecord)record;
                    
                    _sheetNames.add(bsr.getSheetname());
                    
                    break;
                case SSTRecord.sid:
                    _sstRecord = (SSTRecord)record;
                    break;
                case BlankRecord.sid:
                    BlankRecord brec = (BlankRecord)record;
                    
                    row = brec.getRow();
                    column = brec.getColumn();
                    cellValue = null;
                    fType = Types.VARCHAR;
                    isNewValue = true;
                    
                    break;
                case BoolErrRecord.sid:
                    BoolErrRecord berec = (BoolErrRecord)record;
                    
                    row = berec.getRow();
                    column = berec.getColumn();
                    cellValue = berec.getBooleanValue();
                    isNewValue = true;
                    
                    fType = Types.BOOLEAN;
                    
                    break;
                case FormulaRecord.sid:
                    FormulaRecord frec = (FormulaRecord)record;
                    
                    row = frec.getRow();
                    column = frec.getColumn();
                    
                    if (Double.isNaN(frec.getValue()))
                    {
                        // Formula result is a string
                        // This is stored in the next record
                        _outputNextStringRecord = true;
                        _nextRow = frec.getRow();
                        _nextColumn = frec.getColumn();
                    }
                    else
                    {
                        cellValue = Utils.str2Number(
                                _formatListener.formatNumberDateCell(frec),
                                null);
                        
                        fType = Types.NUMERIC;
                        isNewValue = true;
                    }
                    break;
                case StringRecord.sid:
                    if (_outputNextStringRecord)
                    {
                        // String for formula
                        StringRecord srec = (StringRecord)record;
                        cellValue = srec.getString();
                        row = _nextRow;
                        column = _nextColumn;
                        _outputNextStringRecord = false;
                        fType = Types.VARCHAR;
                        isNewValue = true;
                    }
                    break;
                case LabelRecord.sid:
                    LabelRecord lrec = (LabelRecord)record;
                    
                    row = lrec.getRow();
                    column = lrec.getColumn();
                    cellValue = lrec.getValue();
                    fType = Types.VARCHAR;
                    isNewValue = true;
                    
                    break;
                case LabelSSTRecord.sid:
                    LabelSSTRecord lsrec = (LabelSSTRecord)record;
                    
                    if (_sstRecord == null)
                        break;
                    
                    row = lsrec.getRow();
                    column = lsrec.getColumn();
                    
                    fType = Types.VARCHAR;
                    
                    cellValue = _sstRecord.getString(lsrec.getSSTIndex())
                            .toString();
                    
                    typeAndValue = SqlUtils
                            .getNumberTypeAndValue((String)cellValue);
                    
                    if (typeAndValue != null)
                    {
                        fType = typeAndValue.getKey();
                        cellValue = typeAndValue.getValue();
                    }
                    
                    isNewValue = true;
                    
                    break;
                case NoteRecord.sid:
                    break;
                case NumberRecord.sid:
                    NumberRecord numrec = (NumberRecord)record;
                    
                    row = numrec.getRow();
                    column = numrec.getColumn();
                    
                    int fIndex = numrec.getXFIndex();
                    
                    String formatString = _formatListener
                            .getFormatString(numrec);
                    
                    if (_params.isDateTimeFormat(formatString))
                    {
                        cellValue = Utils.str2Date(Utils.date2Str(
                                DateUtil.getJavaDate(numrec.getValue()),
                                _params.getDateTimeFormat()), null, _params
                                .getDateTimeFormat());
                        
                        fType = Types.TIMESTAMP;
                    }
                    else if (_params.isDateFormat(formatString))
                    {
                        cellValue = Utils.str2Date(Utils.date2Str(
                                DateUtil.getJavaDate(numrec.getValue()),
                                _params.getDateFormat()), null, _params
                                .getDateFormat());
                        
                        fType = Types.DATE;
                    }
                    else if (_params.isTimeFormat(formatString))
                    {
                        cellValue = Utils.str2Date(Utils.date2Str(
                                DateUtil.getJavaDate(numrec.getValue()),
                                _params.getTimeFormat()), null, _params
                                .getTimeFormat());
                        
                        fType = Types.TIME;
                    }
                    else if (DateUtil.isADateFormat(fIndex, formatString))
                    {
                        cellValue = DateUtil.getJavaDate(numrec.getValue());
                        
                        if (cellValue instanceof Date
                                && (Utils.getDate((Date)cellValue,
                                        Calendar.YEAR, null) != 1900))
                            fType = Types.TIMESTAMP;
                        else
                        {
                            typeAndValue = SqlUtils
                                    .getNumberTypeAndValue(_formatListener
                                            .formatNumberDateCell(numrec));
                            
                            if (typeAndValue == null)
                            {
                                fType = Types.NUMERIC;
                                cellValue = null;
                            }
                            else
                            {
                                fType = typeAndValue.getKey();
                                cellValue = typeAndValue.getValue();
                            }
                        }
                    }
                    else
                    {
                        typeAndValue = SqlUtils
                                .getNumberTypeAndValue(_formatListener
                                        .formatNumberDateCell(numrec));
                        
                        if (typeAndValue == null)
                        {
                            fType = Types.NUMERIC;
                            cellValue = null;
                        }
                        else
                        {
                            fType = typeAndValue.getKey();
                            cellValue = typeAndValue.getValue();
                        }
                    }
                    
                    isNewValue = true;
                    
                    break;
                case RKRecord.sid:
                    break;
                default:
                    break;
            }
            
            // Handle new row
            if (row > 0 && row != _lastRowNumber)
            {
                try
                {
                    if (row == 1 && _params.getBeforeCallback() != null)
                        _params.getBeforeCallback().onBefore(_dataSet, _driver);
                }
                catch (Exception ex)
                {
                    new RuntimeException(ex);
                }
                
                _dataSetRecord = new DataSetRecord();
                
                if (!_params.isSilent() && _params.getLogStep() > 0
                        && (_index % _params.getLogStep()) == 0)
                    Logger.log(
                            Logger.INFO,
                            EtlLogger.class,
                            _dataSet.getName()
                                    + ": "
                                    + _index
                                    + EtlResource.READING_DATASET_MSG
                                            .getValue());
                _index++;
            }
            
            // Handle missing column
            if (record instanceof MissingCellDummyRecord)
            {
                MissingCellDummyRecord mc = (MissingCellDummyRecord)record;
                row = mc.getRow();
                column = mc.getColumn();
                cellValue = "";
                fType = Types.VARCHAR;
                isNewValue = true;
            }
            
            // If we got something to add, do so
            if (isNewValue && row >= 0 && column >= 0)
            {
                FieldDef fieldDef = null;
                
                // fields defs
                if (row == 0)
                {
                    fieldDef = new FieldDef();
                    fieldDef.setName(cellValue != null ? cellValue.toString()
                            : "field" + column);
                    
                    _dataSet.addField(fieldDef);
                }
                else if (_dataSet.getFieldCount() > column)
                {
                    fieldDef = _dataSet.getFieldDef(column);
                    
                    if (fieldDef != null)
                    {
                        if (!Utils.isEmpty(cellValue))
                        {
                            int type = fieldDef.getSqlDataType();
                            
                            fType = SqlUtils.getFieldType(fType, type,
                                    _types.containsKey(column));
                            
                            fieldDef.setSqlDataType(fType);
                            fieldDef.setNativeDataType(_driver.getType(
                                    new FieldDef(fType, "VARCHAR"), null, null));
                            
                            _types.put(column, true);
                        }
                        else
                            cellValue = null;
                        
                        if (_dataSetRecord != null)
                        {
                            try
                            {
                                if (_params.getAddFieldValueCallback() != null)
                                    _params.getAddFieldValueCallback()
                                            .onAddFieldValue(_dataSet, _driver,
                                                    _dataSetRecord, fieldDef);
                            }
                            catch (Exception ex)
                            {
                                new RuntimeException(ex);
                            }
                            
                            addValue(cellValue, _dataSetRecord, _dataSet);
                        }
                    }
                }
                
            }
            
            // Update column and row count
            if (row > 0)
                _lastRowNumber = row;
            
            // Handle end of row
            if (record instanceof LastCellOfRowDummyRecord)
            {
                // We're onto a new row
                if (_dataSetRecord != null)
                {
                    if (_params.getMaxRows() >= 0
                            && _dataSet.getRecordCount() >= _params
                                    .getMaxRows())
                    {
                        throw new RuntimeException(
                                DataSetConnectorParams.MAX_ROWS_EXCEEDED_EXCEPTION);
                    }
                    
                    boolean added = _dataSet.addRecord(_dataSetRecord);
                    
                    try
                    {
                        if (added && _params.getAddRecordCallback() != null)
                            _params.getAddRecordCallback().onAddRecord(
                                    _dataSet, _driver, _dataSetRecord,
                                    _index - 1);
                    }
                    catch (Exception ex)
                    {
                        new RuntimeException(ex);
                    }
                    
                }
                
            }
            
        }
        
        /**
         * Sets the format listener.
         *
         * @param value the new format listener
         */
        private void setFormatListener(FormatTrackingHSSFListener value)
        {
            _formatListener = value;
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#cleanUp(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void cleanUp(ExcelConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.getOut() != null
                && ((params.getOut() != params.getOutputStream()) || params
                        .isCloseOutput()))
            params.getOut().close();
        
        params.setCleanUpPersistOccurred(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#getDataSetConnectorParams()
     */
    public ExcelConnectorParams getDataSetConnectorParams()
    {
        return new ExcelConnectorParams();
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.ExtensionModule#getDisplayName()
     */
    public String getDisplayName()
    {
        return getName();
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
     * @see com.toolsverse.etl.connector.DataSetConnector#getName()
     */
    public String getName()
    {
        return "Excel (*.xls)";
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
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.ext.BaseExtension#getXmlConfigFileName()
     */
    @Override
    public String getXmlConfigFileName()
    {
        return "com/toolsverse/etl/connector/excel/excel_config.xml";
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
    public void inlinePersist(ExcelConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
        if (record == null)
            return;
        
        int currentRow = params.getCurrentRow();
        
        Row excelRow = params.getSheet().createRow(currentRow);
        
        params.setCurrentRow(++currentRow);
        
        int colCount = dataSet.getFieldCount();
        
        for (int col = 0; col < colCount; col++)
        {
            FieldDef fieldDef = dataSet.getFields().get(col);
            
            if (!fieldDef.isVisible())
                continue;
            
            Object fieldValue = record.get(col);
            int fType = fieldDef.getSqlDataType();
            String value = null;
            Cell dataCell;
            
            if (fieldValue != null)
            {
                value = dataSet.encode(fieldDef, fieldValue, driver,
                        params.getParams(), false);
            }
            
            if (SqlUtils.isNumber(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                
                dataCell.setCellValue(value);
            }
            else if (SqlUtils.isDateOnly(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                
                dataCell.setCellStyle(params.getDateCellStyle());
                
                if (fieldValue instanceof java.util.Date)
                    dataCell.setCellValue((java.util.Date)fieldValue);
                else
                    dataCell.setCellValue(value);
                
            }
            else if (SqlUtils.isTime(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                
                dataCell.setCellStyle(params.getTimeCellStyle());
                
                if (fieldValue instanceof java.util.Date)
                    dataCell.setCellValue((java.util.Date)fieldValue);
                else
                    dataCell.setCellValue(value);
                
            }
            else if (SqlUtils.isTimestamp(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                
                dataCell.setCellStyle(params.getDateTimeCellStyle());
                
                if (fieldValue instanceof java.util.Date)
                    dataCell.setCellValue((java.util.Date)fieldValue);
                else
                    dataCell.setCellValue(value);
                
            }
            else if (SqlUtils.isBoolean(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_BOOLEAN);
                
                if (fieldValue instanceof Boolean)
                    dataCell.setCellValue((Boolean)fieldValue);
                else
                    dataCell.setCellValue(value);
                
            }
            else
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_STRING);
                dataCell.setCellValue(value);
            }
            
        }
        
        if (row >= 0 && records >= 0 && !params.isSilent()
                && params.getLogStep() > 0 && (row % params.getLogStep()) == 0)
            Logger.log(Logger.INFO, EtlLogger.class, dataSet.getName() + ": "
                    + EtlResource.PERSITING_RECORD.getValue() + row
                    + " out of " + records);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#persist(com.toolsverse.
     * etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult persist(ExcelConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        if (dataSet == null
                || params == null
                || (driver == null && dataSet.getDriver() == null)
                || Utils.isNothing(dataSet.getName())
                || dataSet.getFieldCount() == 0
                || (params.isFileNameRequired() && Utils.isNothing(params
                        .getFileName())))
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
                        .getValue());
            if (driver == null && dataSet.getDriver() == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DRIVER_NULL
                        .getValue());
            if (params == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_PARAMS_NULL
                        .getValue());
            if (dataSet != null && dataSet.getFieldCount() == 0)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_FIELDS
                        .getValue());
            if (dataSet != null && Utils.isNothing(dataSet.getName()))
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_NAME
                        .getValue());
            if (params.isFileNameRequired()
                    && Utils.isNothing(params.getFileName()))
                result.addResult(FileConnectorResource.VALIDATION_ERROR_FILE_NAME_NOT_SPECIFIED
                        .getValue());
            
            return result;
        }
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.PERSISTING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        try
        {
            prePersist(params, dataSet, driver);
            
            int records = dataSet.getRecordCount();
            
            // data
            for (int row = 0; row < records; row++)
            {
                DataSetRecord record = dataSet.getRecord(row);
                
                inlinePersist(params, dataSet, driver, record, row, records);
            }
            
            postPersist(params, dataSet, driver);
            
            ConnectorResult connectorResult = new ConnectorResult();
            
            connectorResult.addResult(Utils.format(
                    FileConnectorResource.FILE_PERSISTED.getValue(),
                    new String[] {FilenameUtils.getName(params
                            .getRealFileName())}));
            
            return connectorResult;
        }
        finally
        {
            cleanUp(params, dataSet, driver);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#populate(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult populate(ExcelConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        if (dataSet == null || params == null
                || Utils.isNothing(dataSet.getName())
                || (driver == null && dataSet.getDriver() == null))
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
                        .getValue());
            if (driver == null && dataSet.getDriver() == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DRIVER_NULL
                        .getValue());
            if (params == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_PARAMS_NULL
                        .getValue());
            if (dataSet != null && Utils.isNothing(dataSet.getName()))
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NO_NAME
                        .getValue());
            
            return result;
            
        }
        
        dataSet.clear();
        
        driver = driver != null ? driver : dataSet.getDriver();
        
        if (!params.isSilent())
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.LOADING_DATASET_MSG.getValue()
                            + dataSet.getName() + "...");
        
        FileInputStream fin = null;
        POIFSFileSystem poifs = null;
        
        try
        {
            String fileName = null;
            
            if (params.getInputStream() == null)
            {
                fileName = SystemConfig.instance().getPathUsingAppFolders(
                        params.getFileName(
                                dataSet.getOwnerName() != null ? dataSet
                                        .getOwnerName() : dataSet.getName(),
                                ".xls", true));
                
                fin = new FileInputStream(fileName);
                
                poifs = new POIFSFileSystem(fin);
            }
            else
                poifs = new POIFSFileSystem(params.getInputStream());
            
            XlsProcessor xlsProcessor = new XlsProcessor(params, dataSet,
                    driver);
            
            MissingRecordAwareHSSFListener listener = new MissingRecordAwareHSSFListener(
                    xlsProcessor);
            FormatTrackingHSSFListener formatListener = new FormatTrackingHSSFListener(
                    listener);
            
            xlsProcessor.setFormatListener(formatListener);
            
            HSSFRequest request = new HSSFRequest();
            request.addListenerForAllRecords(formatListener);
            
            HSSFEventFactory factory = new HSSFEventFactory();
            
            try
            {
                factory.processWorkbookEvents(request, poifs);
            }
            catch (Exception ex)
            {
                if (!params.isMaxRowsExceededException(ex)
                        && !params.isSheetAlreadyExatractedException(ex))
                    throw ex;
            }
            
            if (dataSet.getFieldCount() > 0 && dataSet.getRecordCount() == 0
                    && params.getAddRecordCallback() != null)
            {
                params.getAddRecordCallback().onAddRecord(dataSet, driver,
                        null, 0);
            }
            
            ConnectorResult connectorResult = new ConnectorResult();
            
            connectorResult.addResult(Utils.format(
                    FileConnectorResource.FILE_POPULATED.getValue(),
                    new String[] {FilenameUtils.getName(fileName)}));
            
            return connectorResult;
            
        }
        finally
        {
            if (fin != null)
                fin.close();
            
            if (params.getInputStream() != null && params.isCloseInput())
                params.getInputStream().close();
            
            if (params.getAfterCallback() != null)
                params.getAfterCallback().onAfter(dataSet, driver);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#postPersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void postPersist(ExcelConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.getOut() != null)
            params.getWorkbook().write(params.getOut());
        
        params.setPostPersistOccured(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#prePersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    @SuppressWarnings("resource")
    public void prePersist(ExcelConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        String fileName = null;
        
        OutputStream out = null;
        
        if (params.getOutputStream() == null)
        {
            fileName = SystemConfig.instance().getPathUsingAppFolders(
                    params.getFileName(
                            dataSet.getOwnerName() != null ? dataSet
                                    .getOwnerName() : dataSet.getName(),
                            ".xls", true));
            
            params.setRealFileName(fileName);
            
            out = new FileOutputStream(fileName);
            
            if (params.getTransactionMonitor() != null)
                params.getTransactionMonitor().addFile(fileName);
        }
        else
            out = params.getOutputStream();
        
        params.setOut(out);
        
        Workbook workbook = new HSSFWorkbook();
        
        params.setWorkbook(workbook);
        
        Sheet sheet = workbook.createSheet(Utils.isNothing(params
                .getSheetName()) ? dataSet.getName() : params.getSheetName());
        
        params.setSheet(sheet);
        
        Font labelFont = workbook.createFont();
        labelFont.setBoldweight(Font.BOLDWEIGHT_BOLD);
        CellStyle labelCellStyle = workbook.createCellStyle();
        labelCellStyle.setFont(labelFont);
        
        DataFormat dateTimeFormat = workbook.createDataFormat();
        CellStyle dateTimeCellStyle = workbook.createCellStyle();
        dateTimeCellStyle.setDataFormat(dateTimeFormat.getFormat(params
                .getDateTimeFormat()));
        
        params.setDateTimeCellStyle(dateTimeCellStyle);
        
        DataFormat dateFormat = workbook.createDataFormat();
        CellStyle dateCellStyle = workbook.createCellStyle();
        dateCellStyle
                .setDataFormat(dateFormat.getFormat(params.getDateFormat()));
        
        params.setDateCellStyle(dateCellStyle);
        
        DataFormat timeFormat = workbook.createDataFormat();
        CellStyle timeCellStyle = workbook.createCellStyle();
        timeCellStyle
                .setDataFormat(timeFormat.getFormat(params.getTimeFormat()));
        
        params.setTimeCellStyle(timeCellStyle);
        
        // column names
        Row excelRow = sheet.createRow(0);
        
        // metadata
        int col = 0;
        for (FieldDef fieldDef : dataSet.getFields().getList())
        {
            if (!fieldDef.isVisible())
                continue;
            
            Cell labelCell = excelRow.createCell(col++, Cell.CELL_TYPE_STRING);
            labelCell.setCellStyle(labelCellStyle);
            labelCell.setCellValue(fieldDef.getName());
        }
        
        params.setPrePersistOccured(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#writeMetaData(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult writeMetaData(ExcelConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        return new ConnectorResult();
    }
    
}
