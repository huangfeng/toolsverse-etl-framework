/*
 * ExcelConnectorParams.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.excel;

import java.io.OutputStream;
import java.util.Map;
import java.util.Properties;

import org.apache.poi.ss.usermodel.CellStyle;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.ss.usermodel.Workbook;

import com.toolsverse.cache.CacheProvider;
import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.FileConnectorParams;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.util.ObjectStorage;
import com.toolsverse.util.Utils;

/**
 * The {@link com.toolsverse.etl.connector.DataSetConnectorParams} used by {@link ExcelConnector}.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class ExcelConnectorParams extends FileConnectorParams
{
    
    /** The PREFIX. */
    public static final String PREFIX = "excel";
    
    /** The SHEET NAME property. */
    public static final String SHEET_NAME_PROP = "sheetname";
    
    /** The SHEET ALREADY EXTRACTED EXCEPTION. */
    public static final String SHEET_ALREADY_EXTRACTED_EXCEPTION = "Sheet already extracted";
    
    /** The sheet name. */
    private String _sheetName;
    
    /** The output stream. */
    private OutputStream _out;
    
    /** The workbook. */
    private Workbook _workbook;
    
    /** The sheet. */
    private Sheet _sheet;
    
    /** The date time cell style. */
    private CellStyle _dateTimeCellStyle;
    
    /** The date cell style. */
    private CellStyle _dateCellStyle;
    
    /** The time cell style. */
    private CellStyle _timeCellStyle;
    
    /** The current row. */
    private int _currentRow;
    
    /**
     * Instantiates a new ExcelConnectorParams.
     */
    public ExcelConnectorParams()
    {
        this(null, true, -1);
    }
    
    /**
     * Instantiates a new ExcelConnectorParams.
     *
     * @param cacheProvider the cache provider
     * @param silent the "is silent" flag
     * @param logStep the log step
     */
    public ExcelConnectorParams(CacheProvider<String, Object> cacheProvider,
            boolean silent, int logStep)
    {
        super(cacheProvider, silent, logStep);
        
        _sheetName = null;
        
        _out = null;
        _workbook = null;
        _sheet = null;
        _dateTimeCellStyle = null;
        _dateCellStyle = null;
        _timeCellStyle = null;
        _currentRow = 1;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.DataSetConnectorParams#copy()
     */
    @Override
    public DataSetConnectorParams copy()
    {
        ExcelConnectorParams params = new ExcelConnectorParams();
        
        params.setFileNameRequired(isFileNameRequired());
        params.setFileName(getFileName());
        params.setSheetName(getSheetName());
        params.setDateFormat(getDateFormat());
        params.setDateTimeFormat(getDateTimeFormat());
        params.setTimeFormat(getTimeFormat());
        params.setUseSelectedDataSet(useSelectedDataSet());
        params.setSplitBy(getSplitBy());
        
        return params;
    }
    
    /**
     * Gets the current row index.
     *
     * @return the current row index
     */
    public int getCurrentRow()
    {
        return _currentRow;
    }
    
    /**
     * Gets the date cell style.
     *
     * @return the date cell style
     */
    public CellStyle getDateCellStyle()
    {
        return _dateCellStyle;
    }
    
    /**
     * Gets the date time cell style.
     *
     * @return the date time cell style
     */
    public CellStyle getDateTimeCellStyle()
    {
        return _dateTimeCellStyle;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.FileConnectorParams#getInitStr(java.lang
     * .String, com.toolsverse.etl.common.Alias)
     */
    @Override
    public String getInitStr(String name, Alias alias)
    {
        String props = Utils.str2PropsStr(alias.getParams(), " ", "\"");
        
        boolean hasSheetName = props != null
                && props.toLowerCase().indexOf(SHEET_NAME_PROP) >= 0;
        
        return FILE_NAME_PROP + "=\"" + alias.getUrl()
                + (!hasSheetName ? "\" " + SHEET_NAME_PROP + "=\"" + name : "")
                + "\" " + props;
    }
    
    /**
     * Gets the output stream.
     *
     * @return the output stream
     */
    public OutputStream getOut()
    {
        return _out;
    }
    
    /**
     * Gets the current sheet.
     *
     * @return the current sheet
     */
    public Sheet getSheet()
    {
        return _sheet;
    }
    
    /**
     * Gets the current sheet name.
     *
     * @return the current sheet name
     */
    public String getSheetName()
    {
        return _sheetName;
    }
    
    /**
     * Gets the time cell style.
     *
     * @return the time cell style
     */
    public CellStyle getTimeCellStyle()
    {
        return _timeCellStyle;
    }
    
    /**
     * Gets the workbook.
     *
     * @return the workbook
     */
    public Workbook getWorkbook()
    {
        return _workbook;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.FileConnectorParams#init(com.toolsverse.
     * etl.common.Alias)
     */
    @Override
    public void init(Alias alis)
    {
        super.init(alis);
        
        Properties props = Utils.getProperties(alis.getParams());
        
        if (props == null || props.size() == 0)
            return;
        
        setSheetName(props.getProperty(SHEET_NAME_PROP));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.etl.connector.FileConnectorParams#init(java.util.Map)
     */
    @Override
    public void init(Map<String, String> props)
    {
        super.init(props);
        
        if (props == null || props.size() == 0)
            return;
        
        setSheetName(props.get(SHEET_NAME_PROP));
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
        if (storage == null)
            return;
        
        setFileNameRequired(true);
        
        setFileName(SystemConfig.instance().getPathUsingAppFolders(
                storage.getString(PREFIX + FILE_NAME_PROP)));
        
        setSheetName(storage.getString(PREFIX + SHEET_NAME_PROP));
        
        setDateFormat(storage.getString(PREFIX + SqlUtils.DATE_FORMAT_PROP));
        setTimeFormat(storage.getString(PREFIX + SqlUtils.TIME_FORMAT_PROP));
        setDateTimeFormat(storage.getString(PREFIX
                + SqlUtils.DATE_TIME_FORMAT_PROP));
        setUseSelectedDataSet(Utils.str2Boolean(
                storage.getString(PREFIX + USE_SELECTED_PROP), false));
        
        setSplitBy(storage.getString(PREFIX + SPLIT_BY_PROP));
    }
    
    /**
     * Checks if given exception is a "sheet already extracted" exception.
     *
     * @param ex the exception
     * @return true, if given exception is a "sheet already extracted" exception
     */
    public boolean isSheetAlreadyExatractedException(Exception ex)
    {
        return Utils.isParticularException(ex,
                SHEET_ALREADY_EXTRACTED_EXCEPTION);
    }
    
    /**
     * Sets the current row.
     *
     * @param value the new current row
     */
    public void setCurrentRow(int value)
    {
        _currentRow = value;
    }
    
    /**
     * Sets the date cell style.
     *
     * @param value the new date cell style
     */
    public void setDateCellStyle(CellStyle value)
    {
        _dateCellStyle = value;
    }
    
    /**
     * Sets the date time cell style.
     *
     * @param value the new date time cell style
     */
    public void setDateTimeCellStyle(CellStyle value)
    {
        _dateTimeCellStyle = value;
    }
    
    /**
     * Sets the output stream.
     *
     * @param value the new output stream
     */
    public void setOut(OutputStream value)
    {
        _out = value;
    }
    
    /**
     * Sets the sheet.
     *
     * @param value the new sheet
     */
    public void setSheet(Sheet value)
    {
        _sheet = value;
    }
    
    /**
     * Sets the sheet name.
     *
     * @param value the new sheet name
     */
    public void setSheetName(String value)
    {
        _sheetName = value;
    }
    
    /**
     * Sets the time cell style.
     *
     * @param value the new time cell style
     */
    public void setTimeCellStyle(CellStyle value)
    {
        _timeCellStyle = value;
    }
    
    /**
     * Sets the workbook.
     *
     * @param value the new workbook
     */
    public void setWorkbook(Workbook value)
    {
        _workbook = value;
    }
}
