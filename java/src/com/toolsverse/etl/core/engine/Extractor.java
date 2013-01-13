/*
 * Extractor.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Callable;

import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.common.DataSet;
import com.toolsverse.etl.common.DataSetData;
import com.toolsverse.etl.common.DataSetRecord;
import com.toolsverse.etl.common.FieldDef;
import com.toolsverse.etl.common.OnException;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.connector.AddRecordCallback;
import com.toolsverse.etl.connector.BeforeCallback;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.connector.DataSetConnectorParams;
import com.toolsverse.etl.connector.SourceConnector;
import com.toolsverse.etl.connector.SourceConnectorParams;
import com.toolsverse.etl.connector.sql.SqlConnector;
import com.toolsverse.etl.connector.sql.SqlConnectorParams;
import com.toolsverse.etl.connector.xml.XmlConnector;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.util.FileUtils;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;
import com.toolsverse.util.concurrent.ParallelExecutor;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Extracts data from the multiple sources. Extracts can run in parallel.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Extractor
{
    
    /**
     * The Class ExtractorBeforeCallback.
     */
    private class ExtractorBeforeCallback implements BeforeCallback
    {
        
        /** The data writer. */
        DataSetConnector<DataSetConnectorParams, ?> _writer;
        
        /** The data writer params. */
        DataSetConnectorParams _writerParams;
        
        /**
         * Instantiates a new extractor before callback.
         * 
         * @param writer
         *            the data writer
         * @param writerParams
         *            the data writer params
         */
        public ExtractorBeforeCallback(
                DataSetConnector<DataSetConnectorParams, ?> writer,
                DataSetConnectorParams writerParams)
        {
            _writer = writer;
            _writerParams = writerParams;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see
         * com.toolsverse.etl.connector.BeforeCallback#onBefore(com.toolsverse
         * .etl.common.DataSet, com.toolsverse.etl.driver.Driver)
         */
        public void onBefore(DataSet dataSet, Driver driver)
            throws Exception
        {
            if (_writer != null)
                _writer.prePersist(_writerParams, dataSet, driver);
        }
        
    }
    
    /**
     * The Class ExtractorCallback.
     */
    private class ExtractorCallback implements AddRecordCallback
    {
        
        /** The task executor. */
        TaskExecutor _taskExecutor;
        
        /** The "inline tasks exist" flag. */
        boolean _inlineTasksExist;
        
        /** The etl config. */
        EtlConfig _config;
        
        /** The source. */
        Source _source;
        
        /** The _scenario. */
        Scenario _scenario;
        
        /** The destination. */
        Destination _destination;
        
        /** The loader. */
        AddRecordCallback _loader;
        
        /** The data writer. */
        DataSetConnector<DataSetConnectorParams, ?> _writer;
        
        /** The data writer params. */
        DataSetConnectorParams _writerParams;
        
        /**
         * Instantiates a new extractor callback.
         * 
         * @param taskExecutor
         *            the task executor
         * @param inlineTasksExist
         *            the "inline tasks exist" flag
         * @param config
         *            the etl config
         * @param source
         *            the source
         * @param scenario
         *            the scenario
         * @param destination
         *            the destination
         * @param loader
         *            the loader
         * @param writer
         *            the data writer
         * @param writerParams
         *            the data writer params
         */
        public ExtractorCallback(TaskExecutor taskExecutor,
                boolean inlineTasksExist, EtlConfig config, Source source,
                Scenario scenario, Destination destination,
                AddRecordCallback loader,
                DataSetConnector<DataSetConnectorParams, ?> writer,
                DataSetConnectorParams writerParams)
        {
            _taskExecutor = taskExecutor;
            _inlineTasksExist = inlineTasksExist;
            _config = config;
            _source = source;
            _scenario = scenario;
            _destination = destination;
            _loader = loader;
            _writer = writer;
            _writerParams = writerParams;
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
            TaskResult taskResult = null;
            
            if (_inlineTasksExist)
                taskResult = _taskExecutor.executeInlineTasks(_config, _source,
                        _scenario, index);
            
            if (taskResult != null)
            {
                if (taskResult.getResult() == TaskResult.TaskResultCode.REJECT
                        || taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                {
                    if (record != null && dataSet != null
                            && dataSet.getRecordCount() > 0)
                    {
                        dataSet.deleteRecord(dataSet.getRecordCount() - 1);
                    }
                    
                    return;
                }
            }
            
            if (_writer != null)
                _writer.inlinePersist(_writerParams, dataSet,
                        dataSet.getDriver(), record, -1, -1);
            
            if (_source.isEmpty() || _loader != null)
            {
                DataSetData data = dataSet.getData();
                ListHashMap<String, FieldDef> fields = dataSet.getFields();
                if (_loader != null)
                {
                    _destination.getDataSet().setData(data);
                    _destination.getDataSet().setFields(fields);
                    
                    _loader.onAddRecord(dataSet, driver, record, index);
                }
                if (data != null)
                    data.clear();
            }
            
        }
    }
    
    /**
     * The Class ParallelExtract.
     */
    private class ParallelExtract implements Callable<Object>
    {
        
        /** The etl config. */
        private final EtlConfig _config;
        
        /** The scenario. */
        private final Scenario _scenario;
        
        /** The source. */
        private final Source _source;
        
        /**
         * Instantiates a new parallel extract.
         * 
         * @param config
         *            the etl config
         * @param scenario
         *            the scenario
         * @param source
         *            the source
         */
        public ParallelExtract(EtlConfig config, Scenario scenario,
                Source source)
        {
            _config = config;
            _scenario = scenario;
            _source = source;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public Object call()
            throws Exception
        {
            extract(_config, _scenario, _source, null, null);
            
            return null;
        }
    }
    
    /** The transaction monitor. */
    private final TransactionMonitor _transactionMonitor;
    
    /**
     * Instantiates a new extractor.
     * 
     * @param transactionMonitor
     *            the transaction monitor
     */
    public Extractor(TransactionMonitor transactionMonitor)
    {
        _transactionMonitor = transactionMonitor;
    }
    
    /**
     * Extracts data from the source.
     * 
     * @param executor
     *            the executor
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param source
     *            the source
     * @param isExtract
     *            the "is extract" flag
     * @return true, if successful
     * @throws Exception
     *             in case of any error
     */
    private boolean doExtract(ParallelExecutor executor, EtlConfig config,
            Scenario scenario, Source source, boolean isExtract)
        throws Exception
    {
        if (!source.isEnabled()
                || !EtlUtils.checkCondition(config, source.getName(),
                        scenario.getVariables(), source))
            return true;
        
        if (executor != null)
        {
            if (executor.isTerminated())
                return false;
            
            // wait
            if (source.isStub())
            {
                executor.waitUntilDone();
                
                return true;
            }
        }
        
        if (isExtract)
            justExtract(config, scenario, source, executor);
        
        return true;
        
    }
    
    /**
     * Extracts data from all sources.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @throws Exception
     *             in case of any error
     */
    public void extract(EtlConfig config, Scenario scenario)
        throws Exception
    {
        Logger.log(Logger.INFO, EtlLogger.class,
                EtlResource.EXTRACTING_ALL_MSG.getValue());
        if (scenario.getSources() == null)
            return;
        
        ParallelExecutor executor = null;
        
        if (scenario.getParallelSources() > 1)
            executor = new ParallelExecutor(scenario.getParallelSources());
        
        try
        {
            for (int i = 0; i < scenario.getSources().size(); i++)
            {
                Source source = scenario.getSources().get(i);
                
                Destination dest = scenario.getDestinationBySource(source);
                
                if (!doExtract(executor, config, scenario, source,
                        !isStream(scenario, dest)))
                    return;
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
     * Extracts data from the source. Runs extract in its own thread if
     * requested.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param source
     *            the source
     * @param destination
     *            the destination
     * @param addRecordCallback
     *            the add record callback
     * @throws Exception
     *             in case of any error
     */
    public void extract(EtlConfig config, Scenario scenario, Source source,
            Destination destination, AddRecordCallback addRecordCallback)
        throws Exception
    {
        Connection connection = config.getConnectionFactory().getConnection(
                source.getConnectionName());
        
        if (_transactionMonitor != null)
            _transactionMonitor.addConnection(connection);
        
        source.setConnection(connection);
        
        DataSet dataSet = extractDataSet(config, source, scenario, destination,
                addRecordCallback);
        
        if (dataSet != null)
        {
            if (!source.isEmpty())
                source.setDataSet(dataSet);
            else
                source.setDataSet(null);
        }
    }
    
    /**
     * Extracts data to the data set from the source.
     * 
     * @param config
     *            the etl config
     * @param source
     *            the source
     * @param scenario
     *            the scenario
     * @param destination
     *            the destination
     * @param addRecordCallback
     *            the add record callback
     * @return the data set
     * @throws Exception
     *             in case of any error
     */
    @SuppressWarnings("unchecked")
    private DataSet extractDataSet(EtlConfig config, Source source,
            Scenario scenario, Destination destination,
            AddRecordCallback addRecordCallback)
        throws Exception
    {
        PreparedStatement statement = null;
        ResultSet rs = null;
        DataSet dataSet = null;
        String driverName = !Utils.isNothing(source.getDriverClassName()) ? source
                .getDriverClassName() : scenario.getDriverClassName();
        String sql = source.getSql();
        ListHashMap<String, Variable> variables = scenario.getVariables();
        Map<String, Object> bindVars = new HashMap<String, Object>();
        
        Alias alias = (Alias)config.getConnectionFactory().getConnectionParams(
                source.getConnectionName());
        
        EtlFactory etlFactory = new EtlFactory();
        
        try
        {
            if (!Utils.isNothing(sql) && source.getConnection() != null)
            {
                sql = EtlUtils.mergeSqlWithVars(config, sql, source.getUsing(),
                        variables, bindVars);
                
                if ((scenario.getOnPopulateDataSet() == Scenario.ON_ACTION_SAVE && source
                        .getOnPopulateDataSet() == Scenario.ON_ACTION_PARENT)
                        || source.getOnPopulateDataSet() == Scenario.ON_ACTION_SAVE)
                    FileUtils.saveTextFile(EtlUtils.getSqlFileName("s_"
                            + source.getName(), -1), sql);
                
                try
                {
                    statement = source.getConnection().prepareStatement(sql);
                }
                catch (Exception ex)
                {
                    int action = source.handleException(source.getConnection(),
                            sql, ex, -1, null);
                    
                    if (action != OnException.ON_PARSE_EXCEPTION)
                        throw ex;
                    else
                        return null;
                }
                
                for (int i = 0; i < bindVars.size(); i++)
                {
                    String bindVar = (String)bindVars.get(String.valueOf(i));
                    
                    SqlUtils.setBindVar(statement, bindVar, i + 1);
                }
                
                try
                {
                    rs = statement.executeQuery();
                }
                catch (Exception ex)
                {
                    int action = source.handleException(source.getConnection(),
                            sql, ex, -1, null);
                    
                    if (action == OnException.ON_EXCEPTION_RAISE)
                        throw ex;
                }
            }
            
            dataSet = new DataSet();
            dataSet.setName(source.getName());
            dataSet.setEncode(source.isEncoded());
            dataSet.setKeyFields(source.getKeyFields());
            dataSet.setDriver(etlFactory.getDriver(driverName, null, null));
            
            source.setDataSet(dataSet);
            
            TaskExecutor taskExecutor = new TaskExecutor();
            
            TaskResult taskResult = taskExecutor.executePreTasks(config,
                    source, scenario, dataSet);
            
            if (taskResult != null
                    && taskResult.getResult() == TaskResult.TaskResultCode.STOP)
                return dataSet;
            
            String readerClassName = !Utils.isNothing(source
                    .getDataReaderClassName()) ? source
                    .getDataReaderClassName() : (alias != null ? alias
                    .getConnectorClassName() : null);
            
            DataSetConnector<DataSetConnectorParams, ?> reader = null;
            
            Source linkedSource = null;
            
            if (!Utils.isNothing(source.getLinkedSourceName()))
            {
                linkedSource = scenario.getSources().get(
                        source.getLinkedSourceName().toUpperCase());
                
                reader = (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                        .instance().get(SourceConnector.class.getName(), true);
            }
            else
                reader = getReader(readerClassName);
            
            boolean inlineTasksExist = source.getInlineTasks() != null
                    && source.getInlineTasks().size() > 0;
            
            DataSetConnector<DataSetConnectorParams, ?> writer = null;
            DataSetConnectorParams writerParams = null;
            
            if ((scenario.getOnPersistDataSet() == Scenario.ON_ACTION_SAVE && source
                    .getOnPersistDataSet() == Scenario.ON_ACTION_PARENT)
                    || source.getOnPersistDataSet() == Scenario.ON_ACTION_SAVE)
            {
                writer = getWriter(source.getDataWriterClassName());
                
                writerParams = writer.getDataSetConnectorParams();
                
                if (source.getDataWriterParams() != null)
                {
                    writerParams.init(source.getDataWriterParams());
                }
                else if (alias != null)
                {
                    writerParams.init(writerParams.alias2alias(alias));
                }
                
                writerParams.setTransactionMonitor(_transactionMonitor);
            }
            
            ExtractorCallback extractorCallback = new ExtractorCallback(
                    taskExecutor, inlineTasksExist, config, source, scenario,
                    destination, addRecordCallback, writer, writerParams);
            
            ExtractorBeforeCallback extractorBeforeCallback = new ExtractorBeforeCallback(
                    writer, writerParams);
            
            DataSetConnectorParams readerParams = reader
                    .getDataSetConnectorParams();
            readerParams.setAddRecordCallback(extractorCallback);
            readerParams.setBeforeCallback(extractorBeforeCallback);
            readerParams.setCacheProvider(config);
            readerParams.setSilent(false);
            readerParams.setLogStep(config.getLogStep());
            
            if (config != null && config.getGlobalRowLimit() != null
                    && config.getGlobalRowLimit() >= 0)
                readerParams.setMaxRows(config.getGlobalRowLimit());
            
            if (readerParams instanceof SqlConnectorParams)
            {
                SqlConnectorParams params = (SqlConnectorParams)readerParams;
                
                params.setResultSet(rs);
            }
            else if (readerParams instanceof SourceConnectorParams)
            {
                ((SourceConnectorParams)readerParams).setDataSet(linkedSource
                        .getDataSet());
            }
            else
            {
                if (alias != null)
                {
                    readerParams.init(alias);
                }
                
                if (source.getDataReaderParams() != null
                        && source.getDataReaderParams().size() > 0)
                {
                    readerParams.init(source.getDataReaderParams());
                }
            }
            
            try
            {
                reader.populate(readerParams, dataSet, dataSet.getDriver());
                
                if (writer != null)
                    writer.postPersist(writerParams, dataSet,
                            dataSet.getDriver());
            }
            finally
            {
                if (writer != null)
                    writer.cleanUp(writerParams, dataSet, dataSet.getDriver());
            }
            
            taskResult = taskExecutor.executePostTasks(config, source,
                    scenario, dataSet);
            
            dataSet = taskResult != null && taskResult.getDataSet() != null ? taskResult
                    .getDataSet() : dataSet;
        }
        finally
        {
            SqlUtils.cleanUpSQLData(statement, rs, this);
        }
        
        return dataSet;
    }
    
    /**
     * Extracts mandatory data sets. The mandatory data sets cannot be combined
     * with the load.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @throws Exception
     *             in case of any error
     */
    public void extractMandatory(EtlConfig config, Scenario scenario)
        throws Exception
    {
        Logger.log(Logger.INFO, EtlLogger.class,
                EtlResource.EXTRACTING_ALL_MSG.getValue());
        if (scenario.getMandatorySources() == null)
            return;
        
        ParallelExecutor executor = null;
        
        if (scenario.getParallelSources() > 1)
            executor = new ParallelExecutor(scenario.getParallelSources());
        
        try
        {
            for (int i = 0; i < scenario.getMandatorySources().size(); i++)
            {
                Source source = scenario.getMandatorySources().get(i);
                
                if (!doExtract(executor, config, scenario, source, true))
                    return;
            }
        }
        finally
        {
            if (executor != null)
                executor.terminate();
        }
    }
    
    /**
     * Gets the data reader using given class name.
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
                                : SqlConnector.class.getName(), true);
        
        return connector;
    }
    
    /**
     * Gets the data writer using given class name.
     * 
     * @param name
     *            the data writer class name
     * @return the writer
     */
    @SuppressWarnings("unchecked")
    private DataSetConnector<DataSetConnectorParams, ?> getWriter(String name)
    {
        DataSetConnector<DataSetConnectorParams, ?> connector = (DataSetConnector<DataSetConnectorParams, ?>)ObjectFactory
                .instance().get(
                        !Utils.isNothing(name) ? name
                                : XmlConnector.class.getName(), true);
        
        return connector;
    }
    
    /**
     * Checks if "stream" mode set for the destination. If true data will be
     * streamed from the source to destination without storing them in the
     * memory.
     * 
     * @param scenario
     *            the scenario
     * @param dest
     *            the destination
     * @return true, if "stream" is set for the destination
     */
    private boolean isStream(Scenario scenario, Destination dest)
    {
        return dest != null && EtlUtils.isExtractLoad(scenario.getAction())
                && dest.isStream();
        
    }
    
    /**
     * Extracts data from the source.
     * 
     * @param config
     *            the etl config
     * @param scenario
     *            the scenario
     * @param source
     *            the source
     * @param parallelExecutor
     *            the parallel executor
     * @throws Exception
     *             in case of any error
     */
    private void justExtract(EtlConfig config, Scenario scenario,
            Source source, ParallelExecutor parallelExecutor)
        throws Exception
    {
        if (!source.isParallel() || parallelExecutor == null)
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.EXTRACTING_MSG.getValue() + source.getName()
                            + "...");
            
            extract(config, scenario, source, null, null);
        }
        else
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.ADDING_THREAD_FOR_EXTRACTING_MSG.getValue()
                            + source.getName() + "...");
            
            parallelExecutor.addTask(new ParallelExtract(config, scenario,
                    source));
        }
        
    }
}
