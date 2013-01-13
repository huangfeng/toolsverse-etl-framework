/*
 * ExcelXlsxConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.excel;

import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.sql.Types;
import java.util.HashMap;
import java.util.Map;

import javax.xml.parsers.ParserConfigurationException;
import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;

import org.apache.poi.openxml4j.exceptions.OpenXML4JException;
import org.apache.poi.openxml4j.opc.OPCPackage;
import org.apache.poi.openxml4j.opc.PackageAccess;
import org.apache.poi.ss.usermodel.BuiltinFormats;
import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.DataFormat;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.DateUtil;
import org.apache.poi.ss.usermodel.Font;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;
import org.apache.poi.xssf.eventusermodel.ReadOnlySharedStringsTable;
import org.apache.poi.xssf.eventusermodel.XSSFReader;
import org.apache.poi.xssf.model.StylesTable;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.apache.poi.xssf.usermodel.XSSFCellStyle;
import org.apache.poi.xssf.usermodel.XSSFRichTextString;
import org.xml.sax.Attributes;
import org.xml.sax.ContentHandler;
import org.xml.sax.InputSource;
import org.xml.sax.SAXException;
import org.xml.sax.XMLReader;
import org.xml.sax.helpers.DefaultHandler;

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
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * Reads and writes Microsoft xlsx files using apache.poi library. Supports data streaming.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelXlsxConnector extends BaseDataSetConnector implements
        DataSetConnector<ExcelConnectorParams, ConnectorResult>
{
    /**
     * The type of the data value is indicated by an attribute on the cell.
     * The value is usually in a "v" or "t" element within the cell.
     */
    enum xssfDataType
    {
        BOOL, ERROR, FORMULA, INLINESTR, SSTINDEX, NUMBER,
    }
    
    /**
     * The Class XSSFSheetHandler.
     */
    class XSSFSheetHandler extends DefaultHandler
    {
        
        /** Table with styles. */
        private StylesTable _stylesTable;
        
        /** Table with unique strings. */
        private ReadOnlySharedStringsTable _sharedStringsTable;
        
        // Set when V start element is seen
        /** The _v is open. */
        private boolean _vIsOpen;
        
        // Set when cell start element is seen;
        // used when cell close element is seen.
        /** The _next data type. */
        private xssfDataType _nextDataType;
        
        // Used to format numeric cell values.
        /** The _format index. */
        private short _formatIndex;
        
        /** The _format string. */
        private String _formatString;
        
        /** The _formatter. */
        private final DataFormatter _formatter;
        
        // Gathers characters as they are seen.
        /** The _value. */
        private StringBuffer _value;
        
        /** The params. */
        private final ExcelConnectorParams _params;
        
        /** The data set. */
        private final DataSet _dataSet;
        
        /** The driver. */
        private final Driver _driver;
        
        /** The data set record. */
        private DataSetRecord _dataSetRecord;
        
        /** The index. */
        private int _index;
        
        /** The _types. */
        private final Map<Integer, Boolean> _types;
        
        /** The row. */
        private int row;
        
        /** The column. */
        private int column;
        
        /**
         * Accepts objects needed while parsing.
         *
         * @param styles  Table of styles
         * @param strings Table of shared strings
         * @param params the params
         * @param dataSet the data set
         * @param driver the driver
         */
        public XSSFSheetHandler(StylesTable styles,
                ReadOnlySharedStringsTable strings,
                ExcelConnectorParams params, DataSet dataSet, Driver driver)
        {
            _stylesTable = styles;
            _sharedStringsTable = strings;
            _value = new StringBuffer();
            _nextDataType = xssfDataType.NUMBER;
            _formatter = new DataFormatter();
            
            _params = params;
            _dataSet = dataSet;
            _driver = driver;
            
            _dataSetRecord = null;
            
            _index = 1;
            
            _types = new HashMap<Integer, Boolean>();
            
            row = 0;
            column = -1;
        }
        
        /**
         * Captures characters only if a suitable element is open.
         *
         * @param ch the ch
         * @param start the start
         * @param length the length
         * @throws SAXException the sAX exception
         */
        @Override
        public void characters(char[] ch, int start, int length)
            throws SAXException
        {
            if (_vIsOpen)
                _value.append(ch, start, length);
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.xml.sax.helpers.DefaultHandler#endDocument()
         */
        @Override
        public void endDocument()
            throws SAXException
        {
            super.endDocument();
            
            if (_dataSet.getFieldCount() > 0 && _dataSet.getRecordCount() == 0
                    && _params.getAddRecordCallback() != null)
            {
                try
                {
                    _params.getAddRecordCallback().onAddRecord(_dataSet,
                            _driver, null, 0);
                }
                catch (Exception ex)
                {
                    throw new SAXException(ex);
                }
                
            }
            
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.xml.sax.helpers.DefaultHandler#endElement(java.lang.String,
         * java.lang.String, java.lang.String)
         */
        @Override
        public void endElement(String uri, String localName, String name)
            throws SAXException
        {
            int fType = Types.VARCHAR;
            Object cellValue = null;
            boolean isNewValue = false;
            
            if ("v".equals(name) || "t".equals(name))
            {
                // Process the _value contents as required.
                // Do now, as characters() may be called more than once
                switch (_nextDataType)
                {
                    case BOOL:
                        fType = Types.BOOLEAN;
                        
                        char first = _value.charAt(0);
                        
                        cellValue = first == '0' ? false : true;
                        
                        column++;
                        
                        isNewValue = true;
                        
                        break;
                    
                    case ERROR:
                        break;
                    
                    case FORMULA:
                        fType = Types.VARCHAR;
                        
                        cellValue = _value.toString();
                        
                        isNewValue = true;
                        
                        column++;
                        
                        break;
                    
                    case INLINESTR:
                        XSSFRichTextString rtsi = new XSSFRichTextString(
                                _value.toString());
                        
                        fType = Types.VARCHAR;
                        
                        cellValue = rtsi.toString();
                        
                        isNewValue = true;
                        
                        column++;
                        
                        break;
                    
                    case SSTINDEX:
                        String sstIndex = _value.toString();
                        try
                        {
                            int idx = Integer.parseInt(sstIndex);
                            XSSFRichTextString rtss = new XSSFRichTextString(
                                    _sharedStringsTable.getEntryAt(idx));
                            
                            fType = Types.VARCHAR;
                            
                            cellValue = rtss.toString();
                            
                            isNewValue = true;
                            
                            column++;
                            
                        }
                        catch (NumberFormatException ex)
                        {
                            break;
                        }
                        
                        break;
                    
                    case NUMBER:
                        String n = _value.toString();
                        
                        if (_formatString != null)
                        {
                            if (_params.isDateTimeFormat(_formatString)
                                    && Utils.isNumber(n))
                            {
                                cellValue = DateUtil.getJavaDate(Double
                                        .parseDouble(n));
                                
                                fType = Types.TIMESTAMP;
                            }
                            else if (_params.isDateFormat(_formatString)
                                    && Utils.isNumber(n))
                            {
                                cellValue = DateUtil.getJavaDate(Double
                                        .parseDouble(n));
                                
                                fType = Types.DATE;
                            }
                            else if (_params.isTimeFormat(_formatString)
                                    && Utils.isNumber(n))
                            {
                                cellValue = DateUtil.getJavaDate(Double
                                        .parseDouble(n));
                                
                                fType = Types.TIME;
                            }
                            else if (DateUtil.isADateFormat(_formatIndex,
                                    _formatString))
                            {
                                cellValue = DateUtil.getJavaDate(Double
                                        .parseDouble(n));
                                
                                fType = Types.TIMESTAMP;
                            }
                            else
                            {
                                cellValue = _formatter.formatRawCellContents(
                                        Double.parseDouble(n), _formatIndex,
                                        _formatString);
                                
                                fType = Types.NUMERIC;
                            }
                        }
                        else if (Utils.isNumber(n))
                        {
                            Number num = Utils.str2Number(n, null);
                            
                            if (Utils.isDecimal(num))
                            {
                                cellValue = num;
                                
                                fType = Types.NUMERIC;
                            }
                            else
                            {
                                cellValue = num.longValue();
                                
                                fType = Types.INTEGER;
                            }
                        }
                        else
                        {
                            cellValue = n;
                            
                            fType = Types.VARCHAR;
                        }
                        
                        isNewValue = true;
                        
                        column++;
                        
                        break;
                    
                    default:
                        break;
                }
                
                // If we got something to add, do so
                if (isNewValue && row >= 0 && column >= 0)
                {
                    FieldDef fieldDef = null;
                    
                    // fields defs
                    if (row == 0)
                    {
                        fieldDef = new FieldDef();
                        fieldDef.setName(cellValue != null ? cellValue
                                .toString() : "field" + column);
                        
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
                                        new FieldDef(fType, "VARCHAR"), null,
                                        null));
                                
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
                                                .onAddFieldValue(_dataSet,
                                                        _driver,
                                                        _dataSetRecord,
                                                        fieldDef);
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
                
            }
            else if ("row".equals(name) && _dataSet.getFieldCount() > 0)
            {
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
                
                // We're onto a new row
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
                
                row++;
                
                column = -1;
            }
        }
        
        /**
         * Converts an Excel column name like "C" to a zero-based index.
         *
         * @param name the name
         * @return Index corresponding to the specified name
         */
        public int nameToColumn(String name)
        {
            int column = -1;
            for (int i = 0; i < name.length(); ++i)
            {
                int c = name.charAt(i);
                column = (column + 1) * 26 + c - 'A';
            }
            return column;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String,
         * java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        @Override
        public void startElement(String uri, String localName, String name,
                Attributes attributes)
            throws SAXException
        {
            
            if ("inlineStr".equals(name) || "v".equals(name)
                    || "t".equals(name))
            {
                _vIsOpen = true;
                // Clear contents cache
                _value.setLength(0);
            }
            // c => cell
            else if ("c".equals(name))
            {
                // Get the cell reference
                String r = attributes.getValue("r");
                for (int c = 0; c < r.length(); ++c)
                {
                    if (Character.isDigit(r.charAt(c)))
                    {
                        break;
                    }
                }
                
                // Set up defaults.
                _nextDataType = xssfDataType.NUMBER;
                _formatIndex = -1;
                _formatString = null;
                String cellType = attributes.getValue("t");
                String cellStyleStr = attributes.getValue("s");
                if ("b".equals(cellType))
                    _nextDataType = xssfDataType.BOOL;
                else if ("e".equals(cellType))
                    _nextDataType = xssfDataType.ERROR;
                else if ("inlineStr".equals(cellType))
                    _nextDataType = xssfDataType.INLINESTR;
                else if ("s".equals(cellType))
                    _nextDataType = xssfDataType.SSTINDEX;
                else if ("str".equals(cellType))
                    _nextDataType = xssfDataType.FORMULA;
                else if (cellStyleStr != null)
                {
                    // It's a number, but almost certainly one
                    // with a special style or format
                    int styleIndex = Integer.parseInt(cellStyleStr);
                    XSSFCellStyle style = _stylesTable.getStyleAt(styleIndex);
                    _formatIndex = style.getDataFormat();
                    _formatString = style.getDataFormatString();
                    if (_formatString == null)
                        _formatString = BuiltinFormats
                                .getBuiltinFormat(_formatIndex);
                }
            }
            
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
        return "Excel (*.xlsx)";
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
            
            if (SqlUtils.isNumber(fType) && !Utils.isNothing(value))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                
                dataCell.setCellValue(Double.parseDouble(value));
            }
            else if (SqlUtils.isDateOnly(fType))
            {
                if (fieldValue instanceof java.util.Date)
                {
                    dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                    
                    dataCell.setCellStyle(params.getDateCellStyle());
                    
                    dataCell.setCellValue((java.util.Date)fieldValue);
                }
                else
                {
                    if (com.toolsverse.util.DateUtil.isValidDate(value))
                    {
                        dataCell = excelRow.createCell(col,
                                Cell.CELL_TYPE_NUMERIC);
                        
                        dataCell.setCellStyle(params.getDateCellStyle());
                        
                        dataCell.setCellValue(com.toolsverse.util.DateUtil
                                .parse(value));
                    }
                    else
                    {
                        dataCell = excelRow.createCell(col);
                        dataCell.setCellValue(value);
                    }
                }
                
            }
            else if (SqlUtils.isTime(fType))
            {
                if (fieldValue instanceof java.util.Date)
                {
                    dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                    
                    dataCell.setCellStyle(params.getTimeCellStyle());
                    
                    dataCell.setCellValue((java.util.Date)fieldValue);
                }
                else
                {
                    if (com.toolsverse.util.DateUtil.isValidDate(value))
                    {
                        dataCell = excelRow.createCell(col,
                                Cell.CELL_TYPE_NUMERIC);
                        
                        dataCell.setCellStyle(params.getTimeCellStyle());
                        
                        dataCell.setCellValue(com.toolsverse.util.DateUtil
                                .parse(value));
                    }
                    else
                    {
                        dataCell = excelRow.createCell(col);
                        dataCell.setCellValue(value);
                    }
                    
                }
                
            }
            else if (SqlUtils.isTimestamp(fType))
            {
                
                if (fieldValue instanceof java.util.Date)
                {
                    dataCell = excelRow.createCell(col, Cell.CELL_TYPE_NUMERIC);
                    
                    dataCell.setCellStyle(params.getDateTimeCellStyle());
                    
                    dataCell.setCellValue((java.util.Date)fieldValue);
                }
                else
                {
                    if (com.toolsverse.util.DateUtil.isValidDate(value))
                    {
                        dataCell = excelRow.createCell(col,
                                Cell.CELL_TYPE_NUMERIC);
                        
                        dataCell.setCellStyle(params.getDateTimeCellStyle());
                        
                        dataCell.setCellValue(com.toolsverse.util.DateUtil
                                .parse(value));
                    }
                    else
                    {
                        dataCell = excelRow.createCell(col);
                        dataCell.setCellValue(value);
                    }
                }
                
            }
            else if (SqlUtils.isBoolean(fType))
            {
                dataCell = excelRow.createCell(col, Cell.CELL_TYPE_BOOLEAN);
                
                if (fieldValue instanceof Boolean)
                    dataCell.setCellValue((Boolean)fieldValue);
                else
                    dataCell.setCellValue(Utils.str2Boolean(value, false));
                
            }
            else
            {
                dataCell = excelRow.createCell(col);
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
        
        OPCPackage opcPackage = null;
        
        String sheetName = (!Utils.isNothing(dataSet.getOwnerName())
                && !Utils.isNothing(dataSet.getObjectName()) || Utils
                .isNothing(params.getSheetName())) ? dataSet.getName() : params
                .getSheetName();
        
        try
        {
            String fileName = null;
            
            if (params.getInputStream() == null)
            {
                fileName = SystemConfig.instance().getPathUsingAppFolders(
                        params.getFileName(
                                dataSet.getOwnerName() != null ? dataSet
                                        .getOwnerName() : dataSet.getName(),
                                ".xlsx", true));
                
                opcPackage = OPCPackage.open(fileName, PackageAccess.READ);
            }
            else
                opcPackage = OPCPackage.open(params.getInputStream());
            
            try
            {
                process(opcPackage, sheetName, params, dataSet, driver);
            }
            catch (Exception ex)
            {
                if (!params.isMaxRowsExceededException(ex)
                        && !params.isSheetAlreadyExatractedException(ex))
                    throw ex;
            }
            finally
            {
                opcPackage.revert();
            }
            
            ConnectorResult connectorResult = new ConnectorResult();
            
            connectorResult.addResult(Utils.format(
                    FileConnectorResource.FILE_POPULATED.getValue(),
                    new String[] {FilenameUtils.getName(fileName)}));
            
            return connectorResult;
            
        }
        finally
        {
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
                            ".xlsx", true));
            
            params.setRealFileName(fileName);
            
            out = new FileOutputStream(fileName);
            
            if (params.getTransactionMonitor() != null)
                params.getTransactionMonitor().addFile(fileName);
        }
        else
            out = params.getOutputStream();
        
        params.setOut(out);
        
        Workbook workbook = new SXSSFWorkbook(100);
        
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
            
            Cell labelCell = excelRow.createCell(col++);
            labelCell.setCellStyle(labelCellStyle);
            labelCell.setCellValue(fieldDef.getName());
        }
        
        params.setPrePersistOccured(true);
    }
    
    /**
     * Initiates the processing of the XLS workbook file.
     *
     * @param xlsxPackage the xlsx package
     * @param sheetName the sheet name
     * @param params the params
     * @param dataSet the data set
     * @param driver the driver
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws OpenXML4JException the open xm l4 j exception
     * @throws ParserConfigurationException the parser configuration exception
     * @throws SAXException the sAX exception
     */
    public void process(OPCPackage xlsxPackage, String sheetName,
            ExcelConnectorParams params, DataSet dataSet, Driver driver)
        throws IOException, OpenXML4JException, ParserConfigurationException,
        SAXException
    {
        ReadOnlySharedStringsTable strings = new ReadOnlySharedStringsTable(
                xlsxPackage);
        XSSFReader xssfReader = new XSSFReader(xlsxPackage);
        StylesTable styles = xssfReader.getStylesTable();
        XSSFReader.SheetIterator iter = (XSSFReader.SheetIterator)xssfReader
                .getSheetsData();
        while (iter.hasNext())
        {
            InputStream stream = iter.next();
            try
            {
                if (sheetName.equals(iter.getSheetName()))
                {
                    processSheet(styles, strings, stream, params, dataSet,
                            driver);
                    
                    return;
                }
            }
            finally
            {
                stream.close();
            }
        }
    }
    
    /**
     * Parses and shows the content of one sheet
     * using the specified styles and shared-strings tables.
     *
     * @param styles the styles
     * @param strings the strings
     * @param sheetInputStream the sheet input stream
     * @param params the params
     * @param dataSet the data set
     * @param driver the driver
     * @throws IOException Signals that an I/O exception has occurred.
     * @throws ParserConfigurationException the parser configuration exception
     * @throws SAXException the sAX exception
     */
    public void processSheet(StylesTable styles,
            ReadOnlySharedStringsTable strings, InputStream sheetInputStream,
            ExcelConnectorParams params, DataSet dataSet, Driver driver)
        throws IOException, ParserConfigurationException, SAXException
    {
        
        InputSource sheetSource = new InputSource(sheetInputStream);
        SAXParserFactory saxFactory = SAXParserFactory.newInstance();
        SAXParser saxParser = saxFactory.newSAXParser();
        XMLReader sheetParser = saxParser.getXMLReader();
        ContentHandler handler = new XSSFSheetHandler(styles, strings, params,
                dataSet, driver);
        sheetParser.setContentHandler(handler);
        sheetParser.parse(sheetSource);
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
