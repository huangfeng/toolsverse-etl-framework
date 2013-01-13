/*
 * XmlConnector.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.connector.xml;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;

import javax.xml.parsers.SAXParser;
import javax.xml.parsers.SAXParserFactory;
import javax.xml.transform.Result;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;

import org.w3c.dom.Node;
import org.xml.sax.Attributes;
import org.xml.sax.SAXException;
import org.xml.sax.helpers.DefaultHandler;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
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
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.FilenameUtils;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.XmlUtils;
import com.toolsverse.util.log.Logger;

/**
 * Reads and writes xml files. Supports xsl transformations and data streaming.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class XmlConnector extends BaseDataSetConnector implements
        DataSetConnector<XmlConnectorParams, ConnectorResult>
{
    
    /**
     * The Class XmlDataSetHandler.
     */
    private class XmlDataSetHandler extends DefaultHandler
    {
        
        /** The owner. */
        String _owner;
        
        /** The current column. */
        int _currentCol;
        
        /** The row index. */
        int _index;
        
        /** The data set. */
        DataSet _dataSet;
        
        /** The _field name. */
        String _fieldName;
        
        /** The native field type. */
        String _fieldType;
        
        /** The field type name. */
        String _fieldTypeName;
        
        /** The "field is nullable" flag. */
        boolean _isNullable;
        
        /** The "is encode" flag. */
        boolean _isEncode;
        
        /** The field. */
        FieldDef _fieldDef;
        
        /** The record. */
        DataSetRecord _record;
        
        /** The driver. */
        Driver _driver;
        
        /** The current text. */
        StringBuilder _currentText;
        
        /** The XmlConnectorParams. */
        private final XmlConnectorParams _params;
        
        /**
         * Instantiates a new XmlDataSetHandler.
         *
         * @param params the XmlConnectorParams
         * @param dataSet the data set
         * @param driver the driver
         * @throws Exception in case of any error
         */
        public XmlDataSetHandler(XmlConnectorParams params, DataSet dataSet,
                Driver driver) throws Exception
        {
            super();
            
            _params = params;
            _owner = null;
            _currentCol = 0;
            _index = 1;
            _dataSet = dataSet;
            
            _currentText = new StringBuilder();
            
            _driver = driver;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see org.xml.sax.helpers.DefaultHandler#characters(char[], int, int)
         */
        @Override
        public void characters(char[] ch, int start, int length)
            throws SAXException
        {
            _currentText.append(ch, start, length);
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
        public void endElement(String name, String localName, String nodeName)
            throws SAXException
        {
            // name
            if (DataSet.META_DATA.equalsIgnoreCase(nodeName)
                    && DataSet.META_DATA.equalsIgnoreCase(_owner))
                _owner = DataSet.DATA_SET;
            // row
            else if (DataSet.VALUE.equalsIgnoreCase(nodeName)
                    && DataSet.ROW.equalsIgnoreCase(_owner))
            {
                _fieldDef = _dataSet.getFieldDef(_currentCol);
                _fieldName = _fieldDef.getName();
                
                if (_fieldDef == null)
                    throw new SAXException(
                            EtlResource.FIELD_IS_MISSING_MSG.getValue());
                
                Object fldValue = _currentText.toString();
                
                _currentText = new StringBuilder();
                
                try
                {
                    
                    fldValue = _dataSet.decode(_fieldDef, !Utils
                            .isNothing(fldValue) ? fldValue.toString().trim()
                            : null, _driver, _params.getParams());
                    
                    if (_params.getAddFieldValueCallback() != null)
                        _params.getAddFieldValueCallback().onAddFieldValue(
                                _dataSet, _driver, _record, _fieldDef);
                }
                catch (Throwable t)
                {
                    throw new SAXException(Utils.getStackTraceAsString(t));
                }
                
                addValue(fldValue, _record, _dataSet);
                
                _currentCol++;
            }
            // end of the row
            else if (DataSet.ROW.equalsIgnoreCase(nodeName)
                    && DataSet.ROW.equalsIgnoreCase(_owner))
            {
                if (_params.getMaxRows() >= 0
                        && _dataSet.getRecordCount() >= _params.getMaxRows())
                {
                    throw new SAXException(
                            DataSetConnectorParams.MAX_ROWS_EXCEEDED_EXCEPTION);
                }
                
                boolean added = _dataSet.addRecord(_record);
                
                try
                {
                    if (added && _params.getAddRecordCallback() != null)
                        _params.getAddRecordCallback().onAddRecord(_dataSet,
                                _driver, _record, _index - 1);
                }
                catch (Exception ex)
                {
                    throw new SAXException(ex);
                }
                
                _owner = DataSet.DATA;
            }
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * org.xml.sax.helpers.DefaultHandler#startElement(java.lang.String,
         * java.lang.String, java.lang.String, org.xml.sax.Attributes)
         */
        @Override
        public void startElement(String uri, String localName, String nodeName,
                Attributes attributes)
            throws SAXException
        {
            if (DataSet.DATA_SET.equalsIgnoreCase(nodeName) && _owner == null)
                _owner = DataSet.DATA_SET;
            // start of medatata
            else if (DataSet.META_DATA.equalsIgnoreCase(nodeName)
                    && DataSet.DATA_SET.equalsIgnoreCase(_owner))
            {
                _owner = DataSet.META_DATA;
            }
            // metadata
            else if (DataSet.META_DATA.equalsIgnoreCase(_owner)
                    && DataSet.COL.equalsIgnoreCase(nodeName))
            {
                _fieldName = attributes.getValue(DataSet.TYPE_NAME);
                _fieldType = attributes.getValue(DataSet.TYPE_ATTR);
                _fieldTypeName = attributes.getValue(DataSet.NATIVE_TYPE_ATTR);
                _isNullable = Utils.str2Boolean(
                        attributes.getValue(DataSet.NULLABLE_ATTR), true);
                _isEncode = Utils.str2Boolean(
                        attributes.getValue(DataSet.ENCODE_ATTR), false);
                
                _fieldDef = new FieldDef();
                _fieldDef.setName(_fieldName);
                _fieldDef.setNativeDataType(_fieldTypeName);
                _fieldDef.setSqlDataType(Integer.parseInt(_fieldType));
                _fieldDef.setEncode(_isEncode);
                
                TypedKeyValue<Integer, Integer> scalePrec = SqlUtils
                        .getScaleAndPrecision(_fieldTypeName);
                
                if (scalePrec != null)
                {
                    _fieldDef.setFieldSize(scalePrec.getKey());
                    _fieldDef.setPrecision(scalePrec.getKey());
                    
                    if (scalePrec.getValue() != null
                            && scalePrec.getValue() != -1)
                        _fieldDef.setScale(scalePrec.getValue());
                }
                
                _fieldDef.setNullable(_isNullable);
                
                _dataSet.addField(_fieldDef);
            }
            // start of data
            else if (DataSet.DATA.equalsIgnoreCase(nodeName)
                    && DataSet.DATA_SET.equalsIgnoreCase(_owner))
            {
                try
                {
                    if (_params.getBeforeCallback() != null)
                        _params.getBeforeCallback().onBefore(_dataSet, _driver);
                }
                catch (Exception ex)
                {
                    throw new SAXException(ex);
                }
                
                _owner = DataSet.DATA;
                
                // data
                if (_dataSet.getData() == null)
                    _dataSet.setData(new DataSetData());
            }
            // start of the row
            else if (DataSet.ROW.equalsIgnoreCase(nodeName)
                    && DataSet.DATA.equalsIgnoreCase(_owner))
            {
                _owner = DataSet.ROW;
                
                _currentText = new StringBuilder();
                _currentCol = 0;
                
                _record = new DataSetRecord();
                
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
    public void cleanUp(XmlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        if (params.getOutputStream() != null && params.isCloseOutput())
            params.getOutputStream().close();
        
        if (!Utils.isNothing(params.getTempFileName()))
            FileUtils.deleteFile(params.getTempFileName());
        
        params.setCleanUpPersistOccurred(true);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#getDataSetConnectorParams()
     */
    public XmlConnectorParams getDataSetConnectorParams()
    {
        return new XmlConnectorParams();
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
        return "XML";
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
        return "com/toolsverse/etl/connector/xml/xml_config.xml";
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
    public void inlinePersist(XmlConnectorParams params, DataSet dataSet,
            Driver driver, DataSetRecord record, int row, int records)
        throws Exception
    {
        if (record == null)
            return;
        
        Node node = params.getXml().addFieldUnder(params.getCurrentNode(),
                DataSet.ROW);
        
        int colCount = dataSet.getFieldCount();
        
        for (int col = 0; col < colCount; col++)
        {
            FieldDef field = dataSet.getFieldDef(col);
            
            if (!field.isVisible())
                continue;
            
            Object fieldValue = record.get(col);
            
            String value = dataSet.encode(field, fieldValue, driver,
                    params.getParams());
            
            params.getXml().addFieldUnder(node, DataSet.VALUE, value);
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
    public ConnectorResult persist(XmlConnectorParams params, DataSet dataSet,
            Driver driver)
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
            if (records > 0)
            {
                for (int row = 0; row < records; row++)
                {
                    DataSetRecord record = dataSet.getData().get(row);
                    
                    inlinePersist(params, dataSet, driver, record, row, records);
                }
            }
            
            postPersist(params, dataSet, driver);
        }
        finally
        {
            cleanUp(params, dataSet, driver);
        }
        
        ConnectorResult connectorResult = new ConnectorResult();
        
        connectorResult
                .addResult(Utils.format(FileConnectorResource.FILE_PERSISTED
                        .getValue(), new String[] {FilenameUtils.getName(params
                        .getRealFileName())}));
        
        return connectorResult;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#populate(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult populate(XmlConnectorParams params, DataSet dataSet,
            Driver driver)
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
        
        File file = null;
        String fileName = null;
        
        if (params.getInputStream() == null)
        {
            fileName = SystemConfig.instance().getPathUsingAppFolders(
                    params.getFileName(
                            dataSet.getOwnerName() != null ? dataSet
                                    .getOwnerName() : dataSet.getName(),
                            ".xml", true));
            
            String xslFileName = SystemConfig
                    .instance()
                    .getPathUsingAppFolders(params.getXslFromFileName(fileName));
            
            if (!Utils.isNothing(xslFileName))
            {
                String tempFileName = transform(fileName, null, xslFileName);
                
                if (!Utils.isNothing(tempFileName))
                {
                    params.setTempFileName(tempFileName);
                    
                    file = new File(tempFileName);
                }
            }
            else
                file = new File(fileName);
        }
        else
        {
            String xslFileName = SystemConfig.instance()
                    .getPathUsingAppFolders(params.getXslFromFileName(null));
            
            if (!Utils.isNothing(xslFileName))
            {
                String tempFileName = transform(params.getInputStream(), null,
                        xslFileName);
                
                if (!Utils.isNothing(tempFileName))
                {
                    params.setTempFileName(tempFileName);
                    
                    params.getInputStream().close();
                    
                    params.setInputStream(new FileInputStream(tempFileName));
                }
            }
        }
        
        try
        {
            if (params.getInputStream() != null
                    || (file != null && file.exists()))
            {
                SAXParserFactory factory = SAXParserFactory.newInstance();
                SAXParser saxParser = factory.newSAXParser();
                
                if (params.getInputStream() == null)
                    saxParser.parse(file, new XmlDataSetHandler(params,
                            dataSet, driver));
                else
                    saxParser.parse(params.getInputStream(),
                            new XmlDataSetHandler(params, dataSet, driver));
                
            }
            else
                dataSet.setData(null);
        }
        catch (Exception ex)
        {
            if (!params.isMaxRowsExceededException(ex))
                throw ex;
        }
        finally
        {
            if (params.getInputStream() != null && params.isCloseInput())
                params.getInputStream().close();
            
            if (params.getAfterCallback() != null)
                params.getAfterCallback().onAfter(dataSet, driver);
            
            if (!Utils.isNothing(params.getTempFileName()))
            {
                FileUtils.deleteFile(params.getTempFileName());
            }
        }
        
        ConnectorResult connectorResult = new ConnectorResult();
        
        connectorResult.addResult(Utils.format(
                FileConnectorResource.FILE_POPULATED.getValue(),
                new String[] {FilenameUtils.getName(fileName)}));
        
        return connectorResult;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#postPersist(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public void postPersist(XmlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        String fileName = null;
        
        if (params.getXml() != null)
        {
            
            if (params.getOutputStream() == null)
            {
                
                fileName = SystemConfig.instance().getPathUsingAppFolders(
                        params.getFileName(
                                dataSet.getOwnerName() != null ? dataSet
                                        .getOwnerName() : dataSet.getName(),
                                ".xml", true));
                
                if (!Utils.isNothing(params.getXslToFileName(fileName)))
                {
                    String outputFileName = FileUtils
                            .getUnixFolderName(FilenameUtils
                                    .getFullPath(fileName))
                            + Utils.getUUIDName() + ".xml";
                    
                    try
                    {
                        params.getXml().writeToFile(outputFileName);
                        
                        transform(
                                outputFileName,
                                fileName,
                                SystemConfig.instance().getPathUsingAppFolders(
                                        params.getXslToFileName(fileName)));
                    }
                    finally
                    {
                        FileUtils.deleteFile(outputFileName);
                    }
                    
                }
                else
                    params.getXml().writeToFile(fileName);
                
                params.setRealFileName(fileName);
            }
            else
            {
                String xslTo = SystemConfig.instance().getPathUsingAppFolders(
                        params.getXslToFileName(null));
                
                if (!Utils.isNothing(xslTo))
                {
                    InputStream inputStream = new ByteArrayInputStream(params
                            .getXml().xmlToString().getBytes());
                    
                    transform(inputStream, params.getOutputStream(), xslTo);
                    
                }
                else
                    params.getXml().dumpTo(params.getOutputStream());
            }
        }
        
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
    public void prePersist(XmlConnectorParams params, DataSet dataSet,
            Driver driver)
        throws Exception
    {
        XmlUtils xml = new XmlUtils(DataSet.DATA_SET, null);
        Node rootNode = writeMetaData(dataSet, xml);
        
        Node currentNode = xml.addFieldUnder(rootNode, DataSet.DATA);
        
        params.setXml(xml);
        params.setCurrentNode(currentNode);
        
        params.setPrePersistOccured(true);
    }
    
    /**
     * Transforms given xml using style sheet.
     *
     * @param xml the xml
     * @param out the output stream
     * @param xsltFileName the xslt file name
     * @return the string
     * @throws Exception in case of any error
     */
    public String transform(InputStream xml, OutputStream out,
            String xsltFileName)
        throws Exception
    {
        File xsltFile = new File(xsltFileName);
        String outputFileName = out == null ? FileUtils
                .getUnixFolderName(FilenameUtils.getFullPath(xsltFileName))
                + Utils.getUUIDName() + ".xml" : null;
        
        if (!xsltFile.exists())
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_XSL.getValue());
        
        @SuppressWarnings("resource")
        OutputStream ost = out != null ? out : new FileOutputStream(
                outputFileName);
        
        try
        {
            transform(ost, new StreamSource(xml), new StreamSource(xsltFile));
        }
        finally
        {
            if (ost != null && ost != out)
                ost.close();
        }
        
        return outputFileName;
    }
    
    /**
     * Transforms given xmlSource which must be formatted as xml using the xsltSource.
     *
     * @param out the output stream
     * @param xmlSource the xml
     * @param xsltSource the xsl style sheet
     * @throws Exception in case of any error
     */
    public void transform(OutputStream out, Source xmlSource, Source xsltSource)
        throws Exception
    {
        try
        {
            Result result = new StreamResult(out);
            
            // create an instance of TransformerFactory
            TransformerFactory transFact = TransformerFactory.newInstance();
            
            Transformer trans = transFact.newTransformer(xsltSource);
            
            trans.transform(xmlSource, result);
        }
        finally
        {
            if (out != null)
                out.close();
        }
    }
    
    /**
     * Transforms given xml file using style sheet.
     *
     * @param xmlFileName the xml file name
     * @param outputFileName the output file name
     * @param xsltFileName the xslt file name
     * @return the string
     * @throws Exception in case of any error
     */
    public String transform(String xmlFileName, String outputFileName,
            String xsltFileName)
        throws Exception
    {
        File xmlFile = new File(xmlFileName);
        File xsltFile = new File(xsltFileName);
        
        if (outputFileName == null)
            outputFileName = FileUtils.getUnixFolderName(FilenameUtils
                    .getFullPath(xmlFileName)) + Utils.getUUIDName() + ".xml";
        
        if (!xmlFile.exists())
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_SOURCE.getValue());
        
        if (!xsltFile.exists())
            throw new Exception(
                    ConnectorResource.TRANSFORMATION_ERROR_NO_XSL.getValue());
        
        transform(new FileOutputStream(outputFileName), new StreamSource(
                xmlFile), new StreamSource(xsltFile));
        
        return outputFileName;
    }
    
    /**
     * Writes meta data.
     *
     * @param dataSet the data set
     * @param xml the xml
     * @return the DOM node
     * @throws Exception in case of any error
     */
    private Node writeMetaData(DataSet dataSet, XmlUtils xml)
        throws Exception
    {
        Node rootNode;
        Node currentNode;
        Node node;
        
        rootNode = xml.getFirstNodeNamed(DataSet.DATA_SET);
        
        xml.addFieldUnder(rootNode, DataSet.NAME, dataSet.getName());
        currentNode = xml.addFieldUnder(rootNode, DataSet.META_DATA);
        
        int fields = dataSet.getFieldCount();
        
        // metadata
        for (int col = 0; col < fields; col++)
        {
            FieldDef fieldDef = dataSet.getFieldDef(col);
            
            if (!fieldDef.isVisible())
                continue;
            
            node = xml.addFieldUnder(currentNode, DataSet.COL);
            xml.addAttribute(node, DataSet.TYPE_NAME, fieldDef.getName());
            xml.addAttribute(node, DataSet.TYPE_ATTR,
                    String.valueOf(fieldDef.getSqlDataType()));
            xml.addAttribute(node, DataSet.NATIVE_TYPE_ATTR,
                    fieldDef.getNativeDataType());
            xml.addAttribute(node, DataSet.NULLABLE_ATTR,
                    Boolean.toString(fieldDef.isNullable()));
            
            if (dataSet.isFieldEncoded(fieldDef))
                xml.addAttribute(node, DataSet.ENCODE_ATTR,
                        Boolean.TRUE.toString());
            
        }
        
        return rootNode;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.connector.DataSetConnector#writeMetaData(com.toolsverse
     * .etl.connector.DataSetConnectorParams, com.toolsverse.etl.common.DataSet,
     * com.toolsverse.etl.driver.Driver)
     */
    public ConnectorResult writeMetaData(XmlConnectorParams params,
            DataSet dataSet, Driver driver)
        throws Exception
    {
        if (dataSet == null || params == null
                || Utils.isNothing(dataSet.getName())
                || dataSet.getFieldCount() == 0)
        {
            ConnectorResult result = new ConnectorResult();
            result.setRetCode(ConnectorResult.VALIDATION_FAILED_CODE);
            
            if (dataSet == null)
                result.addResult(ConnectorResource.VALIDATION_ERROR_DATA_SET_NULL
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
            
            return result;
        }
        
        XmlUtils xml = new XmlUtils(DataSet.DATA_SET, null);
        writeMetaData(dataSet, xml);
        
        String fileName = SystemConfig.instance().getPathUsingAppFolders(
                params.getFileName(
                        dataSet.getOwnerName() != null ? dataSet.getOwnerName()
                                : dataSet.getName(), ".xml", true));
        
        xml.writeToFile(fileName);
        
        if (params.getTransactionMonitor() != null)
            params.getTransactionMonitor().addFile(fileName);
        
        ConnectorResult connectorResult = new ConnectorResult();
        
        connectorResult.addResult(Utils.format(
                FileConnectorResource.FILE_PERSISTED.getValue(),
                new String[] {FilenameUtils.getName(fileName)}));
        
        return connectorResult;
        
    }
    
}
