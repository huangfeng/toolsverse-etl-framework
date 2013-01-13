/*
 * Loader.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.CallableStatement;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.ConnectionParams;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.connector.AddRecordCallback;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.function.DefFunctions;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.DriverUnit;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.ext.loader.SharedUnitLoader;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.util.ClassUtils;
import com.toolsverse.util.Utils;
import com.toolsverse.util.concurrent.ParallelExecutor;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Loads data into the destinations. Multiple loads can be executed in parallel.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Loader
{
    /**
     * The Class LoaderCallback.
     */
    private class LoaderCallback implements AddRecordCallback
    {
        
        /** The etl config. */
        EtlConfig _config;
        
        /** The scenario. */
        Scenario _scenario;
        
        /** The destination. */
        Destination _destination;
        
        /** The data writer params. */
        DataSetConnectorParams _writerParams;
        
        /** The data writer. */
        DataSetConnector<DataSetConnectorParams, ?> _writer;
        
        /** The parent code generator. */
        CodeGen _parentCodeGen;
        
        /**
         * Instantiates a new loader callback.
         *
         * @param config the etl config
         * @param scenario the scenario
         * @param destination the destination
         * @param writer the data writer
         * @param writerParams the data writer params
         * @param parentCodeGen the parent code generator
         */
        public LoaderCallback(EtlConfig config, Scenario scenario,
                Destination destination,
                DataSetConnector<DataSetConnectorParams, ?> writer,
                DataSetConnectorParams writerParams, CodeGen parentCodeGen)
        {
            _config = config;
            _scenario = scenario;
            _destination = destination;
            
            _writer = writer;
            _writerParams = writerParams;
            
            _parentCodeGen = parentCodeGen;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * com.toolsverse.etl.connector.AddRecordCallback#onAddRecord(com.toolsverse
         * .etl.common.DataSet, com.toolsverse.etl.driver.Driver,
         * com.toolsverse.etl.common.DataSetRecord, int)
         */
        public void onAddRecord(DataSet dataSet, Driver driver,
                DataSetRecord record, int index)
            throws Exception
        {
            prepareAndExecute(_config, _scenario, _destination, _writer,
                    _writerParams, record == null, true, _parentCodeGen);
        }
    }
    
    /**
     * The Class ParallellExecuteConnection.
     */
    private class ParallellExecuteConnection implements Callable<Object>
    {
        
        /** The etl config. */
        private final EtlConfig _config;
        
        /** The scenario. */
        private final Scenario _scenario;
        
        /** The load index. */
        private final int _loadIndex;
        
        /** The current etl unit. */
        private final EtlUnit _unit;
        
        /** The destinations. */
        private final List<Destination> _dests;
        
        /**
         * Instantiates a new parallel execute connection.
         * 
         * @param config
         *            the etl config
         * @param scenario
         *            the scenario
         * @param loadIndex
         *            the load index
         * @param unit
         *            the etl unit
         * @param dests
         *            the destinations
         */
        public ParallellExecuteConnection(EtlConfig config, Scenario scenario,
                int loadIndex, EtlUnit unit, List<Destination> dests)
        {
            _config = config;
            _scenario = scenario;
            _loadIndex = loadIndex;
            _unit = unit;
            _dests = dests;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public Object call()
            throws Exception
        {
            loadDestinationsForConnection(_config, _scenario, _loadIndex,
                    _unit, _dests);
            
            return null;
        }
    }
    
    /**
     * The Class ParallellLoad.
     */
    private class ParallellLoad implements Callable<Object>
    {
        
        /** The destination. */
        private final Destination _destination;
        
        /** The etl config. */
        private final EtlConfig _config;
        
        /** The scenario. */
        private final Scenario _scenario;
        
        /** The driver. */
        private final Driver _driver;
        
        /** The extractor. */
        private final Extractor _extractor;
        
        /** The "is single" flag. */
        private final boolean _isSingle;
        
        /** The connection. */
        private final Connection _conn;
        
        /** The loader. */
        private final Loader _loader;
        
        /** The code generator. */
        private final CodeGen _codeGen;
        
        /**
         * Instantiates a new parallell load.
         * 
         * @param destination
         *            the destination
         * @param config
         *            the etl config
         * @param scenario
         *            the scenario
         * @param driver
         *            the driver
         * @param extractor
         *            the extractor
         * @param isSingle
         *            the "is single" flag
         * @param conn
         *            the connection
         * @param loader
         *            the loader
         * @param code
         *            the code generator
         */
        public ParallellLoad(Destination destination, EtlConfig config,
                Scenario scenario, Driver driver, Extractor extractor,
                boolean isSingle, Connection conn, Loader loader, CodeGen code)
        {
            _destination = destination;
            _config = config;
            _scenario = scenario;
            _driver = driver;
            _extractor = extractor;
            _isSingle = isSingle;
            _conn = conn;
            _loader = loader;
            _codeGen = code;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public Object call()
            throws Exception
        {
            loadDestination(_destination, _config, _scenario, _driver,
                    _extractor, _isSingle, _conn, _loader, _codeGen);
            
            return null;
        }
    }
    
    /** This suffix is added to the connection name to get a clean up connection name. */
    private static final String CLEAN_UP_CONN = "cleanupconnection";
    
    /** The transaction monitor. */
    private final TransactionMonitor _transactionMonitor;
    
    /**
     * Instantiates a new loader.
     * 
     * @param transactionMonitor
     *            the transaction monitor
     */
    public Loader(TransactionMonitor transactionMonitor)
    {
        _transactionMonitor = transactionMonitor;
    }
    
    /**
     * Copy data from the source to the given data set.
     * 
     * @param dataSet
     *            the data set
     * @param source
     *            the source
     * @throws Exception
     *             the exception in case of any error
     */
    private void copyDataFromSource(DataSet dataSet, Source source)
        throws Exception
    {
        if (source != null && source.getDataSet() != null)
        {
            dataSet.setData(source.getDataSet().getData());
            dataSet.setDataSetIndex(source.getDataSet().getDataSetIndex());
            
            if (source.getDataSet().getFields() != null)
                for (FieldDef field : source.getDataSet().getFields().getList())
                {
                    dataSet.addField((FieldDef)ClassUtils.clone(field));
                    
                }
            
            dataSet.setEncode(source.getDataSet().isEncode());
        }
    }
    
    /**
     * Prepares and executes init sql for the driver.
     * 
     * @param conn
     *            the connection
     * @param driver
     *            the driver
     * @param isNewConnection
     *            the "is new connection" flag. If true creates new connection.
     * @throws Exception
     *             in case of any error
     */
    @SuppressWarnings("resource")
    private void executeInitSql(Connection conn, Driver driver,
            boolean isNewConnection)
        throws Exception
    {
        CallableStatement callableStatement = null;
        PreparedStatement preparedStatement = null;
        Statement statement = null;
        String script = "";
        
        script = driver.getInitSql();
        if (!Utils.isNothing(script) && isNewConnection)
        {
            
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.CREATING_TEMP_TABLES_MSG.getValue());
            
            try
            {
                if (driver.supportsCallableStatement()
                        && driver.supportsAnonymousBlocks())
                {
                    callableStatement = conn.prepareCall(script);
                    statement = callableStatement;
                    
                    try
                    {
                        callableStatement.execute();
                    }
                    // we ignore exceptions during creation of the temp
                    // tables
                    catch (Exception ex)
                    {
                        if (!driver.ignoreExceptionsDuringInit())
                            throw ex;
                    }
                }
                else
                {
                    String[] stmts = SqlUtils.parseSql(script);
                    
                    for (int index = 0; index < stmts.length; index++)
                    {
                        String sql = stmts[index];
                        
                        if (!Utils.isNothing(sql))
                            try
                            {
                                preparedStatement = conn.prepareStatement(sql);
                                statement = preparedStatement;
                                preparedStatement.executeUpdate();
                            }
                            // we ignore exceptions during creation of the temp
                            // tables
                            catch (Exception ex)
                            {
                                if (!driver.ignoreExceptionsDuringInit())
                                    throw ex;
                            }
                    }
                }
            }
            finally
            {
                if (driver.supportsCallableStatement())
                    SqlUtils.cleanUpSQLData(statement, null, this);
                else
                    SqlUtils.cleanUpSQLData(preparedStatement, null, this);
            }
        }
    }
    
    /**
     * Prepares and executes init sql for the driver.
     * 
     * @param conn
     *            the connection
     * @param driver
     *            the driver
     * @param isNewConnection
     *            the "is new connection" flag. If true creates new connection.
     * @throws Exception
     *             in case of any error
     */
    private void executeStartTransactionSql(Connection conn, Driver driver,
            boolean isNewConnection)
        throws Exception
    {
        CallableStatement callableStatement = null;
        PreparedStatement preparedStatement = null;
        Statement statement = null;
        String script = "";
        
        script = driver.getStartTransactionSql();
        if (!Utils.isNothing(script) && isNewConnection)
        {
            try
            {
                if (driver.supportsCallableStatement()
                        && driver.supportsAnonymousBlocks())
                {
                    callableStatement = conn.prepareCall(script);
                    statement = callableStatement;
                    
                    callableStatement.execute();
                }
                else
                {
                    String[] stmts = SqlUtils.parseSql(script);
                    
                    for (int index = 0; index < stmts.length; index++)
                    {
                        String sql = stmts[index];
                        
                        if (!Utils.isNothing(sql))
                        {
                            preparedStatement = conn.prepareStatement(sql);
                            statement = preparedStatement;
                            preparedStatement.executeUpdate();
                        }
                    }
                }
            }
            finally
            {
                if (driver.supportsCallableStatement())
                    SqlUtils.cleanUpSQLData(statement, null, this);
                else
                    SqlUtils.cleanUpSQLData(preparedStatement, null, this);
            }
        }
    }
    
    /**
     * Gets the data reader.
     * 
     * @param name
     *            the data reader class name
     * @return the data reader
     */
    @SuppressWarnings("unchecked")
    private DataSetConnector<DataSetConnectorParams, ?> getReader(String name)
    {
        DataSetConnector<DataSetConnectorParams, ?> connector = (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                .instance().get(
                        !Utils.isNothing(name) ? name
                                : XmlConnector.class.getName(), true);
        
        return connector;
    }
    
    /**
     * Gets the data writer.
     * 
     * @param name
     *            the data writer class name
     * @return the data writer
     */
    @SuppressWarnings("unchecked")
    private DataSetConnector<DataSetConnectorParams, ?> getWriter(String name)
    {
        if (Utils.isNothing(name))
            return null;
        
        DataSetConnector<DataSetConnectorParams, ?> connector = (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                .instance().get(name, true);
        
        return connector;
    }
    
    /**
     * Checks if data set can be reused from the source.
     * 
     * @param destination
     *            the destination
     * @param scenario
     *            the scenario
     * @return true, if data set can be reused from the source
     */
    private boolean isReuseFromSource(Destination destination, Scenario scenario)
    {
        return EtlUtils.isExtract(scenario.getAction())
                && destination.getSource() != null;
    }
    
    /**
     * Checks if destination is single. The load code for the "single"
     * destination can not be combined with others and executed independently.
     * 
     * @param config
     *            the etl config
     * @param driver
     *            the driver
     * @param scenario
     *            the scenario
     * @param destination
     *            the destination
     * @return true, if is single
     */
    private boolean isSingle(EtlConfig config, Driver driver,
            Scenario scenario, Destination destination)
    {
        return Destination.SCOPE_SINGLE
                .equalsIgnoreCase(destination.getScope())
                || destination.isStream();
    }
    
    /**
     * Checks if "stream" mode is set. If true data will be streamed from the
     * source to destination without storing them in the memory.
     * 
     * @param destination
     *            the destination
     * @param scenario
     *            the scenario
     * @return true, if "stream" mode is set
     */
    private boolean isStream(Destination destination, Scenario scenario)
    {
        return destination.getSource() != null
                && EtlUtils.isLoad(scenario.getAction())
                && destination.isStream();
    }
    
    /**
     * Loads data for all destinations, if requested - in parallel threads.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param loadIndex
     *            the load index
     * @throws Exception
     *             in case of any error
     */
    public void load(EtlConfig config, Scenario scenario, int loadIndex)
        throws Exception
    {
        Map<EtlUnit, List<Destination>> sortedDests = sortDestinations(config,
                scenario);
        
        if (sortedDests == null || sortedDests.size() == 0)
            return;
        
        ParallelExecutor executor = null;
        
        if (scenario.isParallelConnections() && sortedDests.size() > 1)
            executor = new ParallelExecutor(sortedDests.size());
        
        try
        {
            for (EtlUnit unit : sortedDests.keySet())
            {
                List<Destination> dests = sortedDests.get(unit);
                
                loadDestinationsForConnection(executor, config, scenario,
                        loadIndex, unit, dests);
            }
        }
        finally
        {
            if (executor != null)
            {
                try
                {
                    executor.waitUntilDone();
                }
                finally
                {
                    executor.terminate();
                }
            }
        }
    }
    
    /**
     * Loads blobs and clobs.
     * 
     * @param config
     *            the etl config
     * @param dest
     *            the destination
     * @param driver
     *            the driver
     * @return true, if successful
     * @throws Exception
     *             in case of any error
     */
    private boolean loadBlobs(EtlConfig config, Destination dest, Driver driver)
        throws Exception
    {
        DataSet dataSet = dest.getDataSet();
        
        if (dataSet == null)
            return false;
        
        boolean hasBlob = dataSet.hasLargeObjects();
        
        if (!hasBlob)
            return false;
        
        if (!driver.supportsCallableStatement())
        {
            return true;
        }
        
        Logger.log(Logger.INFO, EtlLogger.class,
                EtlResource.LOADING_BLOB_MSG.getValue() + dataSet.getName()
                        + "...");
        
        int rCount = dataSet.getRecordCount();
        int fCount = dataSet.getFieldCount();
        
        long index = 1;
        
        for (int row = 0; row < rCount; row++)
        {
            DataSetRecord record = dataSet.getRecord(row);
            
            if (config != null && config.getLogStep() > 0
                    && (index % config.getLogStep()) == 0)
                Logger.log(Logger.INFO, EtlLogger.class, dataSet.getName()
                        + ": " + index + " out of " + rCount
                        + EtlResource.READING_BLOBS_MSG.getValue());
            index++;
            
            for (int col = 0; col < fCount; col++)
            {
                FieldDef fieldDef = dataSet.getFieldDef(col);
                int fType = fieldDef.getSqlDataType();
                
                if (!SqlUtils.isLargeObject(fType))
                    continue;
                
                Object value = dataSet.getFieldValue(record, col);
                if (value == null)
                    continue;
                
                String fieldName = fieldDef.getName();
                Variable var = dest.getVariable(fieldName);
                
                if (var == null)
                {
                    var = new Variable();
                    var.setName(fieldName);
                    var.setFunctionClassName(driver.getDefaultFunctionClass());
                    var.setFunction(DefFunctions.DEFAULT_FUNCTION);
                    var.setTableName(dataSet.getObjectName());
                    
                    dest.getVariables().put(fieldName, var);
                }
                
                Object pkValue = null;
                
                if (Utils.isNothing(var.getLinkedVarName()))
                    pkValue = row;
                else
                    pkValue = dataSet.getFieldValue(record,
                            var.getLinkedVarName());
                if (pkValue != null)
                    if (SqlUtils.isClob(fType))
                        dataSet.getDriver().updateStagingClob(
                                dataSet.getConnection(), var,
                                pkValue.toString(), value);
                    else
                        dataSet.getDriver().updateStagingBlob(
                                dataSet.getConnection(), var,
                                pkValue.toString(), value);
            }
        }
        
        return true;
    }
    
    /**
     * Loads data for the given destination.
     * 
     * @param destination
     *            the destination
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param driver
     *            the driver
     * @param extractor
     *            the extractor
     * @param isSingle
     *            the "is single" flag. The load code for the "single"
     *            destination can not be combined with others and executed
     *            independently.
     * @param conn
     *            the connection
     * @param loader
     *            the loader
     * @param codeGen
     *            the code generator
     * @throws Exception
     *             in case of any error
     */
    private void loadDestination(Destination destination, EtlConfig config,
            Scenario scenario, Driver driver, Extractor extractor,
            boolean isSingle, Connection conn, Loader loader, CodeGen codeGen)
        throws Exception
    {
        DataSetConnector<DataSetConnectorParams, ?> dataReader = null;
        DataSetConnector<DataSetConnectorParams, ?> dataWriter = null;
        DataSet dataSet = new DataSet();
        boolean isException = false;
        boolean donePostTasks = false;
        DataSetConnectorParams readerParams = null;
        DataSetConnectorParams writerParams = null;
        
        TaskExecutor taskExecutor = new TaskExecutor();
        
        try
        {
            setDataSet(dataSet, destination, driver, conn);
            
            TaskResult taskResult = taskExecutor.executePreTasks(config,
                    destination, scenario, dataSet);
            
            if (taskResult != null
                    && taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                return;
            
            Alias alias = (Alias)config
                    .getConnectionFactory()
                    .getConnectionParams(
                            Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                                    : destination.getConnectionName());
            
            dataReader = getReader(destination.getDataReaderClassName());
            
            readerParams = dataReader.getDataSetConnectorParams();
            
            if (destination.getDataReaderParams() != null)
                readerParams.init(destination.getDataReaderParams());
            else if (alias != null)
                readerParams.init(alias);
            
            String writerClassName = !Utils.isNothing(destination
                    .getDataWriterClassName()) ? destination
                    .getDataWriterClassName() : (alias != null ? alias
                    .getConnectorClassName() : null);
            
            dataWriter = getWriter(writerClassName);
            
            if (dataWriter != null)
            {
                writerParams = dataWriter.getDataSetConnectorParams();
                
                if (alias != null)
                    writerParams.init(alias);
                
                if (destination.getDataWriterParams() != null)
                    writerParams.init(destination.getDataWriterParams());
                
                writerParams.setTransactionMonitor(_transactionMonitor);
            }
            
            if (isStream(destination, scenario))
            {
                
                destination.setDataSet(dataSet);
                
                try
                {
                    LoaderCallback loaderCallback = new LoaderCallback(config,
                            scenario, destination, dataWriter, writerParams,
                            codeGen);
                    
                    extractor.extract(config, scenario,
                            destination.getSource(), destination,
                            loaderCallback);
                    
                    if (dataWriter != null)
                        dataWriter.postPersist(writerParams, dataSet, driver);
                    
                }
                finally
                {
                    if (dataWriter != null)
                        dataWriter.cleanUp(writerParams, dataSet, driver);
                }
                
                return;
            }
            
            if (!isReuseFromSource(destination, scenario))
                dataReader.populate(readerParams, dataSet, driver);
            else
                copyDataFromSource(dataSet, destination.getSource());
            
            destination.setDataSet(dataSet);
            
            if (isSingle)
            {
                try
                {
                    if (dataWriter != null)
                    {
                        dataWriter.prePersist(writerParams, dataSet, driver);
                    }
                    
                    prepareAndExecute(config, scenario, destination,
                            dataWriter, writerParams, false, false, codeGen);
                    
                    if (dataWriter != null)
                        dataWriter.postPersist(writerParams, dataSet, driver);
                    
                }
                finally
                {
                    if (dataWriter != null)
                        dataWriter.cleanUp(writerParams, dataSet, driver);
                    
                }
                return;
            }
            
            boolean hasBlobs = !Destination.LOAD_DELETE
                    .equalsIgnoreCase(destination.getLoadAction())
                    && loadBlobs(config, destination, driver);
            
            taskResult = taskExecutor.executeBeforeEtlTasks(config,
                    destination, scenario);
            
            if (taskResult != null
                    && taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                return;
            
            try
            {
                if (destination.isEmpty() && dataSet.isEmpty())
                    return;
                else
                {
                    try
                    {
                        if (dataWriter != null)
                        {
                            dataWriter
                                    .prePersist(writerParams, dataSet, driver);
                            
                            codeGen.getUnit().setWriter(dataWriter);
                            codeGen.getUnit().setWriterParams(writerParams);
                        }
                        else if (codeGen.getUnit() != null)
                        {
                            codeGen.getUnit().setWriter(null);
                            codeGen.getUnit().setWriterParams(null);
                        }
                        
                        codeGen.prepare(config, scenario, destination, false,
                                false);
                        
                        if (dataWriter != null)
                            dataWriter.postPersist(writerParams, dataSet,
                                    driver);
                        
                    }
                    finally
                    {
                        if (dataWriter != null)
                            dataWriter.cleanUp(writerParams, dataSet, driver);
                        
                    }
                    
                }
            }
            finally
            {
                donePostTasks = true;
                
                if (!isException)
                    taskExecutor.executePostTasks(config, destination,
                            scenario, dataSet);
                
                Source src = destination.getSource();
                
                if (src != null && src.getUsageCounter() <= 1)
                    src.decUsageCounter();
                
                if ((src == null || src.getUsageCounter() <= 0)
                        && destination.getDataSet() != null
                        && (!hasBlobs || driver.supportsCallableStatement()))
                {
                    destination.getDataSet().clearData();
                }
            }
        }
        catch (Exception ex)
        {
            isException = true;
            throw ex;
        }
        finally
        {
            if (!isException && !donePostTasks)
                taskExecutor.executePostTasks(config, destination, scenario,
                        dataSet);
            if (destination.getCache() != null)
                destination.getCache().clear();
        }
    }
    
    /**
     * Loads data for destination.
     * 
     * @param executor
     *            the executor
     * @param destination
     *            the destination
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param driver
     *            the driver
     * @param extractor
     *            the extractor
     * @param isSingle
     *            the "is single" flag. The load code for the "single"
     *            destination can not be combined with others and executed
     *            independently.
     * @param conn
     *            the connection
     * @param loader
     *            the loader
     * @param codeGen
     *            the code generator
     * @throws Exception
     *             in case of any error
     */
    private void loadDestination(ParallelExecutor executor,
            Destination destination, EtlConfig config, Scenario scenario,
            Driver driver, Extractor extractor, boolean isSingle,
            Connection conn, Loader loader, CodeGen codeGen)
        throws Exception
    {
        if (!destination.isParallel() || executor == null || driver == null
                || !driver.supportsParallelLoad())
            loadDestination(destination, config, scenario, driver, extractor,
                    isSingle, conn, loader, codeGen);
        else
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.ADDING_THREAD_FOR_LOADING_MSG.getValue()
                            + destination.getName() + "...");
            
            executor.addTask(new ParallellLoad(destination, config, scenario,
                    driver, extractor, isSingle, conn, loader, codeGen));
        }
    }
    
    /**
     * Loads data for destinations which belong to the same connection.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param loadIndex
     *            the load index
     * @param unit
     *            the etl unit
     * @param dests
     *            the destinations
     * @throws Exception
     *             in case of any error
     */
    private void loadDestinationsForConnection(EtlConfig config,
            Scenario scenario, int loadIndex, EtlUnit unit,
            List<Destination> dests)
        throws Exception
    {
        if (dests == null)
            return;
        
        Destination destination = null;
        
        CodeGen codeGen = (CodeGen)ObjectFactory.instance().get(
                scenario.getCodeGenClass());
        
        codeGen.setUnit(unit);
        
        Logger.log(Logger.INFO, EtlLogger.class,
                EtlResource.LOADING_ALL_MSG.getValue());
        
        // connection
        Connection conn = config.getConnectionFactory().getConnection(
                unit.getConnectionName());
        
        ConnectionParams connectionParams = config.getConnectionFactory()
                .getConnectionParams(conn);
        
        boolean isNewConnection = true;
        
        if (_transactionMonitor != null)
            isNewConnection = _transactionMonitor.addConnection(conn);
        
        // driver
        
        EtlFactory etlFactory = new EtlFactory();
        
        UnitLoader unitLoader = null;
        
        DriverUnit driverUnit = null;
        
        if (connectionParams != null)
        {
            unitLoader = SharedUnitLoader.instance();
            
            driverUnit = (DriverUnit)unitLoader.getUnit(Driver.class);
        }
        
        Driver driver = null;
        if (unit == null || Utils.isNothing(unit.getDriverClassName()))
            driver = etlFactory.getDriver(
                    scenario.getDriverClassName(),
                    driverUnit,
                    connectionParams != null ? connectionParams
                            .getUniqueProperty() : null);
        else
            driver = etlFactory.getDriver(
                    unit.getDriverClassName(),
                    driverUnit,
                    connectionParams != null ? connectionParams
                            .getUniqueProperty() : null);
        
        Connection cleanUpConn = conn;
        
        if (driver.needSeparateConnectionForDdl())
        {
            cleanUpConn = config.getConnectionFactory().getConnection(
                    unit.getConnectionName() + CLEAN_UP_CONN);
            
            if (cleanUpConn == null)
            {
                ConnectionParams cleanUpAlias = connectionParams.copy(unit
                        .getConnectionName() + CLEAN_UP_CONN);
                
                cleanUpConn = config.getConnectionFactory().getConnection(
                        cleanUpAlias);
                
                config.getConnectionFactory().addConnection(cleanUpConn,
                        cleanUpAlias, unit.getConnectionName() + CLEAN_UP_CONN,
                        null);
                
            }
            
            if (_transactionMonitor != null)
                _transactionMonitor.addConnection(cleanUpConn);
        }
        
        ParallelExecutor executor = null;
        
        if (!scenario.isAttrSet(Scenario.NO_TEMP_TABLES_ATTR))
            executeInitSql(conn, driver, isNewConnection);
        
        executeStartTransactionSql(conn, driver, isNewConnection);
        
        if (scenario.getParallelDests() > 0)
            executor = new ParallelExecutor(scenario.getParallelDests());
        
        Extractor extractor = new Extractor(_transactionMonitor);
        
        Logger.log(Logger.INFO, EtlLogger.class,
                EtlResource.PREPARING_SQL_SCRIPT_MSG.getValue());
        
        boolean needToAssebble = false;
        
        try
        {
            for (int i = 0; i < dests.size(); i++)
            {
                destination = dests.get(i);
                
                if (!destination.isEnabled()
                        || !EtlUtils.checkCondition(config,
                                destination.getName(), scenario.getVariables(),
                                destination))
                    continue;
                
                if (executor != null)
                {
                    if (executor.isTerminated())
                        return;
                    
                    // wait
                    if (Destination.WAIT_TYPE.equalsIgnoreCase(destination
                            .getType()))
                    {
                        executor.waitUntilDone();
                        
                        continue;
                    }
                }
                
                boolean isSingle = isSingle(config, driver, scenario,
                        destination);
                
                needToAssebble = needToAssebble || !isSingle;
                
                if ((isStream(destination, scenario) || isSingle)
                        && codeGen.getStatus() == CodeGen.PREPARED_STATUS)
                {
                    try
                    {
                        codeGen.assembleCode(config, scenario, driver,
                                loadIndex, false);
                        
                        codeGen.execute(config, scenario, driver, false, conn,
                                cleanUpConn, null);
                        
                    }
                    finally
                    {
                        codeGen.reset();
                        
                        codeGen.setUnit(unit);
                    }
                }
                
                loadDestination(executor, destination, config, scenario,
                        driver, extractor, isSingle, conn, (isSingle) ? this
                                : null, codeGen);
            }
        }
        catch (Exception ex)
        {
            // need to clean up here
            codeGen.cleanUpOnException(config, scenario, driver, cleanUpConn);
            
            throw ex;
        }
        
        finally
        {
            if (executor != null)
            {
                try
                {
                    executor.waitUntilDone();
                }
                finally
                {
                    executor.terminate();
                }
                
            }
        }
        
        try
        {
            if (needToAssebble
                    && codeGen.getStatus() == CodeGen.PREPARED_STATUS)
            {
                codeGen.assembleCode(config, scenario, driver, loadIndex, false);
                
                codeGen.execute(config, scenario, driver, false, conn,
                        cleanUpConn, null);
                
            }
        }
        finally
        {
            codeGen.cleanUp(config, scenario, driver, cleanUpConn);
        }
    }
    
    /**
     * Loads data for destinations which belong to the same connection, if requested - in parallel threads.
     * 
     * @param executor
     *            the executor
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param loadIndex
     *            the load index
     * @param unit
     *            the etl unit
     * @param dests
     *            the destinations
     * @throws Exception
     *             in case of any error
     */
    private void loadDestinationsForConnection(ParallelExecutor executor,
            EtlConfig config, Scenario scenario, int loadIndex, EtlUnit unit,
            List<Destination> dests)
        throws Exception
    {
        if (executor == null)
            loadDestinationsForConnection(config, scenario, loadIndex, unit,
                    dests);
        else
        {
            Logger.log(
                    Logger.INFO,
                    EtlLogger.class,
                    EtlResource.ADDING_THREAD_FOR_UNIT_MSG.getValue()
                            + unit.toString() + "...");
            
            if (executor.isTerminated())
                return;
            
            executor.addTask(new ParallellExecuteConnection(config, scenario,
                    loadIndex, unit, dests));
        }
    }
    
    /**
     * Prepares and executes code for the destination.
     *
     * @param config the etl config
     * @param scenario the scenario
     * @param destination the destination
     * @param writer the data writer
     * @param writerParams the data writer params
     * @param onlyInit the "only init" flag
     * @param onCallback the on callback
     * @param parentCodeGen the parent code generator
     * @throws Exception in case of any error
     */
    private void prepareAndExecute(EtlConfig config, Scenario scenario,
            Destination destination,
            DataSetConnector<DataSetConnectorParams, ?> writer,
            DataSetConnectorParams writerParams, boolean onlyInit,
            boolean onCallback, CodeGen parentCodeGen)
        throws Exception
    {
        CodeGen codeGen = (CodeGen)ObjectFactory.instance().get(
                scenario.getCodeGenClass());
        
        String driverClassName = Utils.isNothing(destination
                .getDriverClassName()) ? scenario.getDriverClassName()
                : destination.getDriverClassName();
        
        EtlFactory etlFactory = new EtlFactory();
        
        String cName = Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                : destination.getConnectionName();
        
        Connection conn = config.getConnectionFactory().getConnection(cName);
        
        ConnectionParams connectionParams = config.getConnectionFactory()
                .getConnectionParams(conn);
        
        UnitLoader unitLoader = null;
        DriverUnit driverUnit = null;
        
        if (connectionParams != null)
        {
            unitLoader = SharedUnitLoader.instance();
            driverUnit = (DriverUnit)unitLoader.getUnit(Driver.class);
        }
        
        Driver driver = etlFactory.getDriver(driverClassName, driverUnit,
                connectionParams != null ? connectionParams.getUniqueProperty()
                        : null);
        
        if (_transactionMonitor != null)
            _transactionMonitor.addConnection(conn);
        
        Connection cleanUpConn = conn;
        
        if (driver.needSeparateConnectionForDdl())
        {
            cleanUpConn = config.getConnectionFactory().getConnection(
                    cName + CLEAN_UP_CONN);
            
            if (cleanUpConn == null)
            {
                ConnectionParams cleanUpAlias = connectionParams.copy(cName
                        + CLEAN_UP_CONN);
                
                cleanUpConn = config.getConnectionFactory().getConnection(
                        cleanUpAlias);
                
                config.getConnectionFactory().addConnection(cleanUpConn,
                        cleanUpAlias, cName + CLEAN_UP_CONN, null);
                
                if (_transactionMonitor != null)
                    _transactionMonitor.addConnection(cleanUpConn);
            }
        }
        
        boolean hasBlobs = false;
        
        boolean setBlobs = onCallback
                && driver.supportsCallableStatement()
                && !Destination.LOAD_DELETE.equalsIgnoreCase(destination
                        .getLoadAction());
        
        if (setBlobs && !driver.supportsParamsInAnonymousBlocks())
            hasBlobs = loadBlobs(config, destination, driver);
        
        codeGen.setUnit(new EtlUnit(cName, driverClassName, writer,
                writerParams));
        
        try
        {
            codeGen.prepare(config, scenario, destination, true, onlyInit);
            
            codeGen.assembleCode(config, scenario, driver, 0, true);
            
            if (parentCodeGen != null && parentCodeGen != codeGen)
                parentCodeGen.copy(codeGen);
            
            codeGen.execute(
                    config,
                    scenario,
                    driver,
                    true,
                    conn,
                    cleanUpConn,
                    setBlobs && driver.supportsParamsInAnonymousBlocks() ? destination
                            : null);
        }
        finally
        {
            codeGen.cleanUp(config, scenario, driver, cleanUpConn);
        }
        
        if (hasBlobs)
            driver.deleteStagingBinary(conn, destination.getDataSet()
                    .getObjectName());
    }
    
    /**
     * Populates dataSet attributes from the source to destination.
     * 
     * @param dataSet
     *            the data set
     * @param destination
     *            the destination
     * @param driver
     *            the driver
     * @param conn
     *            the connection
     */
    private void setDataSet(DataSet dataSet, Destination destination,
            Driver driver, Connection conn)
    {
        if (destination.getSource() != null)
        {
            dataSet.setName(destination.getSource().getName());
            
            dataSet.setKeyFields(destination.getSource().getKeyFields());
        }
        else
            dataSet.setName(destination.getName());
        dataSet.setTableName(destination.getObjectName());
        dataSet.setDriver(driver);
        dataSet.setConnection(conn);
    }
    
    /**
     * Sorts destinations by connection name.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @return the sorted destinations
     */
    private Map<EtlUnit, List<Destination>> sortDestinations(EtlConfig config,
            Scenario scenario)
    {
        Destination destination = null;
        LinkedHashMap<EtlUnit, List<Destination>> sorted = new LinkedHashMap<EtlUnit, List<Destination>>();
        List<Destination> destinations = null;
        
        if (scenario.getDestinations() != null)
            for (int i = 0; i < scenario.getDestinations().size(); i++)
            {
                destination = scenario.getDestinations().get(i);
                
                EtlUnit unit = new EtlUnit(
                        Utils.isNothing(destination.getConnectionName()) ? EtlConfig.DEST_CONNECTION_NAME
                                : destination.getConnectionName(),
                        Utils.isNothing(destination.getDriverClassName()) ? scenario
                                .getDriverClassName() : destination
                                .getDriverClassName(), null, null);
                
                destinations = sorted.get(unit);
                
                if (destinations == null)
                {
                    destinations = new ArrayList<Destination>();
                    
                    sorted.put(unit, destinations);
                }
                
                destinations.add(destination);
            }
        
        return sorted;
    }
}
