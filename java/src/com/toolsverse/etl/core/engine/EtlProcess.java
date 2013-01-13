/*
 * EtlProcess.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.math.BigDecimal;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Date;
import java.util.List;
import java.util.concurrent.Callable;

import javax.script.Bindings;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.connector.ConnectorUnit;
import com.toolsverse.etl.connector.DataSetConnector;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.connection.DefaultTransactionMonitor;
import com.toolsverse.etl.core.util.EtlUtils;
import com.toolsverse.etl.driver.Driver;
import com.toolsverse.etl.driver.DriverUnit;
import com.toolsverse.etl.resource.EtlResource;
import com.toolsverse.etl.sql.connection.TransactionMonitor;
import com.toolsverse.etl.sql.util.SqlUtils;
import com.toolsverse.etl.util.EtlLogger;
import com.toolsverse.ext.loader.SharedUnitLoader;
import com.toolsverse.ext.loader.UnitLoader;
import com.toolsverse.resource.Resource;
import com.toolsverse.updater.Updater;
import com.toolsverse.updater.service.UpdaterResponse;
import com.toolsverse.util.ClassUtils;
import com.toolsverse.util.Script;
import com.toolsverse.util.Utils;
import com.toolsverse.util.concurrent.ParallelExecutor;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * This class executes one or multiple etl scenarios. Reads configuration file
 * if needed.
 * 
 * @see com.toolsverse.etl.core.engine.Scenario
 * @see com.toolsverse.etl.core.engine.Extractor
 * @see com.toolsverse.etl.core.engine.Loader
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlProcess
{
    /**
     * The EtlProcess modes
     * <br>
     * INCLUDED - ETL engine included into other Toolsverse apps, such as Data Explorer
     * <br>
     * STANDALONE - ETL engine is a standalone application
     * <br>
     * EMBEDDED - ETL engine embedded into third-party application
     *
     */
    public static enum EtlMode
    {
        /**  ETL engine included into other Toolsverse apps, such as Data Explorer */
        INCLUDED,
        /** ETL engine is a standalone application */
        STANDALONE,
        /** ETL engine embedded into third-party application */
        EMBEDDED
    }
    
    /**
     * The Class ParallellExecuteScenario.
     */
    private class ParallellExecuteScenario implements Callable<Object>
    {
        
        /** The etl config. */
        private final EtlConfig _conf;
        
        /** The scenario. */
        private final Scenario _scenario;
        
        /** The load index. */
        private final int _index;
        
        /** The transaction monitor. */
        private final TransactionMonitor _transactionMonitor;
        
        /**
         * Instantiates a new parallel execute scenario.
         * 
         * @param config
         *            the etl config
         * @param scenario
         *            the scenario
         * @param loadIndex
         *            the load index
         * @param transactionMonitor
         *            the transaction monitor
         */
        public ParallellExecuteScenario(EtlConfig config, Scenario scenario,
                int loadIndex, TransactionMonitor transactionMonitor)
        {
            _conf = config;
            _scenario = scenario;
            _index = loadIndex;
            _transactionMonitor = transactionMonitor;
        }
        
        /*
         * (non-Javadoc)
         * 
         * @see java.util.concurrent.Callable#call()
         */
        public Object call()
            throws Exception
        {
            executeInLoop(_scenario, _conf, _index, _transactionMonitor);
            
            return null;
        }
    }
    
    /**
     * Reads etl config from the file and executes one or multiple scenarios if
     * any configured.
     * 
     * @param args
     *            String[]
     */
    public static void main(String[] args)
    {
        EtlProcess process = new EtlProcess(EtlMode.STANDALONE);
        
        EtlConfig etlConfig = new EtlConfig();
        
        if (process.parseArgs(args, etlConfig))
            process.execute(null, etlConfig);
        
        System.exit(0);
    }
    
    /** The mode. The standalone mode implies executing main(...) */
    private final EtlMode _mode;
    
    /**
     * Instantiates a new etl process.
     * 
     * @param mode
     *            the mode. The standalone mode implies executing
     *            main(...)
     */
    public EtlProcess(EtlMode mode)
    {
        _mode = mode;
        
        if (_mode == EtlMode.STANDALONE)
        {
            Logger.getLogger().setLevel(EtlConfig.class, Logger.INFO);
            
            init();
            
            String version = SystemConfig.instance().getSystemProperty(
                    SystemConfig.VERSION, "3.1");
            
            System.out.println(SystemConfig.instance().getTitle(
                    EtlConfig.DEFAULT_TITLE)
                    + " " + version + ". Use -? for help.");
        }
        else if (_mode == EtlMode.EMBEDDED)
        {
            init();
        }
    }
    
    /**
     * Checks for updates
     * 
     */
    private void checkUpdate()
    {
        System.out.println("");
        System.out.println("Checking for updates...");
        
        Updater.instance().checkForUpdates(null, false);
        
        UpdaterResponse response = Updater.instance().getResponse();
        
        if (response.getResponseCode() == UpdaterResponse.ResponseCode.ERROR
                || response.getResponseCode() == UpdaterResponse.ResponseCode.NO_SERVICE)
            System.out
                    .println("Update status: update service is not available");
        else
            System.out.println("Update status: "
                    + UpdaterResponse.CODES.get(response.getResponseCode()));
    }
    
    /**
     * Executed at the very end. Closes resources, logs metrics etc.
     * 
     * @param config
     *            the etl config
     * @param response
     *            the etl response
     */
    private void done(EtlConfig config, EtlResponse response)
    {
        if (config.getConnectionFactory() != null)
            config.getConnectionFactory().releaseConnections();
        
        if (config.getCache() != null)
            config.getCache().clear();
        
        response.setEndTime(new Date());
        
        if (response.getException() != null)
            Logger.log(Logger.FATAL, EtlConfig.class,
                    EtlResource.EXECUTION_ERROR_MSG.getValue(),
                    response.getException());
        
        if (response.getRetCode() == EtlConfig.RETURN_OK)
            Logger.log(Logger.INFO, EtlConfig.class,
                    EtlResource.FINISH_SUCCESS_MSG.getValue());
        else if (response.getRetCode() == EtlConfig.RETURN_ERROR)
            Logger.log(Logger.INFO, EtlConfig.class,
                    EtlResource.FINISH_ERROR_MSG.getValue());
        else if (response.getRetCode() == EtlConfig.RETURN_CONFIG_NOT_INITIALIZED)
            Logger.log(Logger.INFO, EtlConfig.class,
                    EtlResource.FINISH_CONFIG_NOT_INITIALIZED_ERROR_MSG
                            .getValue());
        
        Logger.log(Logger.INFO, EtlConfig.class,
                EtlResource.START_TIME_MSG.getValue() + response.getStartTime());
        Logger.log(Logger.INFO, EtlConfig.class,
                EtlResource.END_TIME_MSG.getValue() + response.getEndTime());
    }
    
    /**
     * Evaluates loop condition.
     *
     * @param scenario the scenario
     * @param config the etl config
     * @param index the index
     * @param counter the counter
     * @param execCounter the execution counter
     * @param loopCounter the loop counter
     * @param evals the number of evaluations
     * @return the object
     * @throws Exception in case of any error
     */
    private Object evalLoopCond(Scenario scenario, EtlConfig config, int index,
            int counter, int execCounter, int loopCounter, int evals)
        throws Exception
    {
        Script script = new Script();
        
        script.compile(config, scenario.getName() + "loop",
                scenario.getLoopCode(), scenario.getLoopLang());
        
        Bindings bindings = script.getBindings(config, scenario.getLoopLang());
        
        bindings.put("etlConfig", config);
        bindings.put("scenario", scenario);
        bindings.put("index", index);
        bindings.put("counter", counter);
        bindings.put("execCounter", execCounter);
        bindings.put("loopCounter", loopCounter);
        bindings.put("evals", evals);
        
        return script.eval(config, bindings, scenario.getName() + "loop",
                scenario.getLoopLang());
    }
    
    /**
     * Executes etl process.
     * 
     * @return the etl response
     */
    public EtlResponse execute()
    {
        return execute((EtlRequest)null);
    }
    
    /**
     * Executes process using given etl config.
     * 
     * @param config
     *            the etl config
     * @return the etl response
     */
    public EtlResponse execute(EtlConfig config)
    {
        return execute(null, config);
    }
    
    /**
     * Executes process using given etl request.
     * 
     * @param request
     *            the etl request
     * @return the etl response
     */
    public EtlResponse execute(EtlRequest request)
    {
        return execute(request, null);
    }
    
    /**
     * Executes etl process using given etl request and etl config.
     * 
     * @param request
     *            the etl request
     * @param config
     *            the etl config
     * @return the etl response
     */
    private EtlResponse execute(EtlRequest request, EtlConfig config)
    {
        List<Scenario> execute = null;
        
        EtlResponse response = new EtlResponse();
        
        if (request == null || request.getEtlConfig() == null)
        {
            if (config == null)
                config = new EtlConfig();
            
            try
            {
                if (request == null && !config.initConfigXml())
                {
                    response.setRetCode(EtlConfig.RETURN_NO_CONFIG);
                    response.setEndTime(new Date());
                    
                    if (_mode == EtlMode.STANDALONE)
                        printHelp(config);
                    
                    return response;
                }
                
                execute = config.getExecute();
            }
            catch (Exception ex)
            {
                Logger.log(Logger.FATAL, EtlLogger.class,
                        EtlResource.FINISH_CONFIG_NOT_INITIALIZED_ERROR_MSG
                                .getValue(), ex);
                
                response.setException(ex);
                response.setRetCode(EtlConfig.RETURN_NO_CONFIG);
                response.setEndTime(new Date());
                
                return response;
            }
        }
        else
        {
            config = request.getEtlConfig();
            
            if (config.getAliasesMap() != null
                    && config.getAliasesMap().size() > 0)
                try
                {
                    config.updateConnections();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.FATAL, EtlLogger.class,
                            EtlResource.FINISH_CONFIG_NOT_INITIALIZED_ERROR_MSG
                                    .getValue(), ex);
                    
                    response.setException(ex);
                    response.setRetCode(EtlConfig.RETURN_NO_CONFIG);
                    response.setEndTime(new Date());
                    
                    return response;
                }
            
        }
        
        int countOfTheParallelScenarious = 0;
        
        if (request != null && request.getScenarios() != null
                && request.getScenarios().size() > 0)
        {
            execute = request.getScenarios();
            
            for (Scenario sc : request.getScenarios())
            {
                if (sc.isParallelInnerScenario())
                {
                    countOfTheParallelScenarious++;
                }
            }
        }
        
        Scenario scenario = null;
        Scenario sc = null;
        
        ParallelExecutor executor = null;
        
        EtlFactory etlFactory = new EtlFactory();
        
        countOfTheParallelScenarious = countOfTheParallelScenarious > 0 ? countOfTheParallelScenarious
                : config.getCountOfTheParallelScenarious();
        
        try
        {
            if (countOfTheParallelScenarious > 0)
                executor = new ParallelExecutor(countOfTheParallelScenarious);
            
            if (execute != null)
                for (int i = 0; i < execute.size(); i++)
                {
                    sc = execute.get(i);
                    
                    if (!sc.isReady())
                    {
                        scenario = etlFactory.getScenario(config, sc.getName());
                        
                        if (scenario == null)
                            continue;
                        
                        scenario.setParallelScenario(sc.isParallelScenario());
                        scenario.setAction(sc.getAction());
                        
                        // set variables
                        sc.assignVars(scenario);
                        
                        waitUntilDone(executor, !scenario.isParallelScenario());
                    }
                    else
                        scenario = sc;
                    
                    executeOrAddTask(executor, scenario.isParallelScenario(),
                            scenario, config, 0, null);
                }
        }
        catch (Exception ex)
        {
            response.setException(ex);
            response.setCode(config.getLastExecutedCode());
            response.setLine(config.getErrorLine());
            response.setFileName(config.getLastExecutedFileName());
            response.setRetCode(EtlConfig.RETURN_ERROR);
        }
        finally
        {
            if (executor != null)
            {
                try
                {
                    executor.waitUntilDone();
                }
                catch (Exception ex)
                {
                    Logger.log(Logger.SEVERE, EtlLogger.class,
                            Resource.ERROR_GENERAL.getValue(), ex);
                    
                }
                
                executor.terminate();
            }
            
            done(config, response);
        }
        
        return response;
    }
    
    /**
     * Executes etl scenario.
     * 
     * @param scenario
     *            the scenario
     * @param config
     *            the etl config
     * @param index
     *            the scenario index. -1 if it is a main scenario.
     * @param transactionMonitor
     *            the transaction monitor
     * @throws Exception
     *             in case of any error
     */
    private void execute(Scenario scenario, EtlConfig config, int index,
            TransactionMonitor transactionMonitor)
        throws Exception
    {
        if (scenario == null || config == null)
            return;
        
        if (scenario.getOnPersistDataSet() == Scenario.ON_ACTION_SKIP
                && ((EtlUtils.isExtract(scenario.getAction()) && !EtlUtils
                        .isLoad(scenario.getAction())) || (!EtlUtils
                        .isExtract(scenario.getAction()) && EtlUtils
                        .isLoad(scenario.getAction()))))
            scenario.setOnPersistDataSet(Scenario.ON_ACTION_SAVE);
        
        boolean isMain = true;
        
        if (transactionMonitor == null)
        {
            transactionMonitor = (TransactionMonitor)ObjectFactory.instance()
                    .get(TransactionMonitor.class.getName(),
                            DefaultTransactionMonitor.class.getName(), null,
                            null, false, true);
            
            transactionMonitor.setConnectionFactory(config
                    .getConnectionFactory());
        }
        else
            isMain = false;
        
        ParallelExecutor executor = null;
        
        if (scenario.getParallelInnerScenarious() > 0)
            executor = new ParallelExecutor(
                    scenario.getParallelInnerScenarious());
        
        EtlFactory etlFactory = new EtlFactory();
        
        try
        {
            EtlUtils.substituteVars(scenario.getVariables());
            
            if (scenario.getExecute() != null)
                for (int i = 0; i < scenario.getExecute().size(); i++)
                {
                    Scenario iScenario = scenario.getExecute().get(i);
                    Scenario innerScenario;
                    
                    if (Utils.isNothing(iScenario.getScriptName()))
                    {
                        innerScenario = etlFactory.getScenario(config,
                                iScenario.getName());
                        
                        if (innerScenario == null)
                            return;
                        
                        iScenario.assignVars(innerScenario);
                    }
                    else
                        innerScenario = iScenario;
                    
                    if (EtlConfig.NOTHING != iScenario.getAction())
                        innerScenario.setAction(iScenario.getAction());
                    else
                        innerScenario.setAction(scenario.getAction());
                    
                    innerScenario.setOnPersistDataSet(scenario
                            .getOnPersistDataSet());
                    innerScenario.setOnPopulateDataSet(scenario
                            .getOnPopulateDataSet());
                    innerScenario.setOnSave(scenario.getOnSave());
                    innerScenario.setCommitEachBlock(scenario
                            .isCommitEachBlock());
                    innerScenario.setIsInner(true);
                    innerScenario.setParallelInnerScenario(iScenario
                            .isParallelInnerScenario());
                    
                    // set variables
                    scenario.assignVars(innerScenario);
                    
                    waitUntilDone(executor,
                            !innerScenario.isParallelInnerScenario());
                    
                    executeOrAddTask(executor,
                            innerScenario.isParallelInnerScenario(),
                            innerScenario, config, index++, transactionMonitor);
                }
            
            waitUntilDone(executor, true);
            
            // actual execution of the single scenario
            executeSingleScenario(scenario, config, index, transactionMonitor);
            
            if (isMain)
            {
                if (Thread.currentThread().isInterrupted())
                    transactionMonitor.rollback(null);
                else
                    transactionMonitor.commit();
            }
            
        }
        catch (Exception ex)
        {
            transactionMonitor.rollback(ex);
            
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
    }
    
    /**
     * Executes etl scenario in the loop.
     * 
     * @param scenario
     *            the etl scenario
     * @param config
     *            the etl config
     * @param index
     *            the scenario index
     * @param transactionMonitor
     *            the transaction monitor
     * @throws Exception
     *             in case of any error
     */
    private void executeInLoop(Scenario scenario, EtlConfig config, int index,
            TransactionMonitor transactionMonitor)
        throws Exception
    {
        if (!EtlUtils.checkCondition(config, scenario.getName(),
                scenario.getVariables(), scenario))
            return;
        
        if (Utils.isNothing(scenario.getLoopCode()))
        {
            execute(scenario, config, index, transactionMonitor);
            
            return;
        }
        
        Connection con = null;
        
        if (Variable.DEFAULT_LANG.equalsIgnoreCase(scenario.getLoopLang()))
        {
            
            if (Utils.isNothing(scenario.getLoopConnectionName()))
                con = config.getConnectionFactory().getConnection(
                        EtlConfig.SOURCE_CONNECTION_NAME);
            else
                con = config.getConnectionFactory().getConnection(
                        scenario.getLoopConnectionName());
            
            if (con == null)
            {
                Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                        EtlResource.CHECK_CONDITION_MSG.getValue(),
                        new String[] {scenario.getName()}));
                
                return;
            }
        }
        
        String pattern = scenario.getLoopVarPattern();
        String field = scenario.getLoopField();
        if (pattern != null)
            pattern = EtlUtils.mergeSqlWithVars(config, pattern, null,
                    scenario.getVariables(), null);
        Variable var = scenario.getVariable(scenario.getLoopVarName());
        String params = "";
        
        int loopCounter = 1;
        
        if (!Utils.isNothing(scenario.getLoopCount()))
        {
            String lCount = EtlUtils.mergeSqlWithVars(config,
                    scenario.getLoopCount(), null, scenario.getVariables(),
                    null);
            
            try
            {
                loopCounter = new BigDecimal(lCount).intValue();
            }
            catch (Exception ex)
            {
                loopCounter = 1;
            }
        }
        
        int counter = 1;
        int execCounter = 1;
        
        if (Variable.DEFAULT_LANG.equalsIgnoreCase(scenario.getLoopLang()))
        {
            
            PreparedStatement st = null;
            ResultSet rs = null;
            
            Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                    EtlResource.PREPARING_COUNTER_MSG.getValue(),
                    new String[] {scenario.getName()}));
            
            try
            {
                st = con.prepareStatement(EtlUtils.mergeSqlWithVars(config,
                        scenario.getLoopCode(), null, scenario.getVariables(),
                        null));
                
                rs = st.executeQuery();
                
                while (rs.next())
                {
                    if (!Utils.isNothing(pattern) && var != null)
                        params = SqlUtils.prepareParams(rs, params, field);
                    
                    if (counter == loopCounter)
                    {
                        counter = 1;
                        
                        Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                                EtlResource.EXECUTING_COUNTER_MSG.getValue(),
                                new String[] {scenario.getName(),
                                        String.valueOf(execCounter)}));
                        
                        setLoopVariable(var, pattern, params);
                        
                        execute(scenario, config, index, transactionMonitor);
                        
                        params = "";
                        
                        execCounter++;
                    }
                    else
                        counter++;
                }
                
                // one last time
                if (counter > 1)
                {
                    Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                            EtlResource.EXECUTING_COUNTER_MSG.getValue(),
                            new String[] {scenario.getName(),
                                    String.valueOf(execCounter)}));
                    
                    setLoopVariable(var, pattern, params);
                    
                    execute(scenario, config, index, transactionMonitor);
                    
                }
            }
            finally
            {
                SqlUtils.cleanUpSQLData(st, rs, this);
            }
        }
        else
        {
            int evals = 0;
            
            Object ret = evalLoopCond(scenario, config, index, counter,
                    execCounter, loopCounter, evals++);
            
            while (ret != null && !"null".equals(ret))
            {
                if (!Utils.isNothing(pattern) && var != null)
                    params = SqlUtils.prepareParams(params, ret);
                
                if (counter == loopCounter)
                {
                    counter = 1;
                    
                    Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                            EtlResource.EXECUTING_COUNTER_MSG.getValue(),
                            new String[] {scenario.getName(),
                                    String.valueOf(execCounter)}));
                    
                    setLoopVariable(var, pattern, params);
                    
                    execute(scenario, config, index, transactionMonitor);
                    
                    params = "";
                    
                    execCounter++;
                }
                else
                    counter++;
                
                ret = evalLoopCond(scenario, config, index, counter,
                        execCounter, loopCounter, evals++);
            }
            
            // one last time
            if (counter > 1)
            {
                Logger.log(Logger.INFO, EtlLogger.class, Utils.format(
                        EtlResource.EXECUTING_COUNTER_MSG.getValue(),
                        new String[] {scenario.getName(),
                                String.valueOf(execCounter)}));
                
                setLoopVariable(var, pattern, params);
                
                execute(scenario, config, index, transactionMonitor);
                
            }
        }
        
    }
    
    /**
     * Execute or add task.
     * 
     * @param executor
     *            the executor
     * @param isParallel
     *            the is parallel
     * @param scenario
     *            the scenario
     * @param config
     *            the config
     * @param index
     *            the index
     * @param transactionMonitor
     *            the transaction monitor
     * @throws Exception
     *             in case of any error
     */
    private void executeOrAddTask(ParallelExecutor executor,
            boolean isParallel, Scenario scenario, EtlConfig config, int index,
            TransactionMonitor transactionMonitor)
        throws Exception
    {
        if (!isParallel || executor == null)
            executeInLoop(scenario, config, index, transactionMonitor);
        else
        {
            Logger.log(Logger.INFO, EtlLogger.class,
                    EtlResource.ADDING_THREAD_FOR_SCENARIO_MSG.getValue()
                            + scenario.getName() + "...");
            
            executor.addTask(new ParallellExecuteScenario(config, scenario,
                    index, transactionMonitor));
        }
    }
    
    /**
     * Execute single etl scenario.
     * 
     * @param scenario
     *            the scenario
     * @param config
     *            the etl config
     * @param index
     *            the scenario index
     * @param transactionMonitor
     *            the transaction monitor
     * @throws Exception
     *             in case of any error
     */
    private void executeSingleScenario(Scenario scenario, EtlConfig config,
            int index, TransactionMonitor transactionMonitor)
        throws Exception
    {
        Extractor extractor = new Extractor(transactionMonitor);
        
        if (EtlUtils.isExtract(scenario.getAction()))
            extractor.extract(config, scenario);
        else
            extractor.extractMandatory(config, scenario);
        
        if (EtlUtils.isLoad(scenario.getAction()))
        {
            Loader loader = new Loader(transactionMonitor);
            
            loader.load(config, scenario, index);
        }
    }
    
    /**
     * Gets the config file name.
     * 
     * @return the config file name
     */
    private String getConfigFileName()
    {
        return "com/toolsverse/etl/core/config/config.properties";
    }
    
    /**
     * Initializes components of the ETL engine 
     */
    private void init()
    {
        SystemConfig.instance().loadProps(getConfigFileName());
        SystemConfig.instance().setSystemProperty(SystemConfig.APP_NAME,
                EtlConfig.DEFAULT_APP_NAME);
        SystemConfig.instance().setSystemProperty(SystemConfig.TITLE,
                EtlConfig.DEFAULT_TITLE);
        
        ClassUtils.addFolderToClassPath(SystemConfig.instance().getHome()
                + SystemConfig.JDBC_PATH, "*.jar");
        
        loadUnits();
    }
    
    /**
     * Load units.
     */
    private void loadUnits()
    {
        UnitLoader unitLoader = SharedUnitLoader.instance();
        
        unitLoader.loadOthers(
                SystemConfig.instance().getSystemPropertyForDeployment(
                        SystemConfig.SHARED_CLASSPATH, null), null);
    }
    
    /**
     * Parses the command line arguments.
     * 
     * @param args
     *            the command line arguments
     * @param etlConfig
     *            the etl config
     * @return true, if successful
     */
    private boolean parseArgs(String[] args, EtlConfig etlConfig)
    {
        boolean cont = true;
        
        if (args != null)
        {
            for (String arg : args)
            {
                arg = arg.trim().toLowerCase();
                
                if (arg.matches("-(\\?|h|help)"))
                {
                    cont = false;
                    
                    printHelp(etlConfig);
                }
                
                if (arg.matches("-(d|c|config|drivers|)"))
                {
                    cont = false;
                    
                    printConfig(etlConfig);
                }
                
                if (arg.matches("-(v|version)"))
                {
                    cont = false;
                    
                    checkUpdate();
                }
                
                if (arg.matches("-(u|update)"))
                {
                    cont = false;
                    
                    update();
                }
                
            }
        }
        
        return cont;
    }
    
    /**
     * Prints the config.
     * 
     * @param etlConfig
     *            the etl config
     */
    public void printConfig(EtlConfig etlConfig)
    {
        UnitLoader unitLoader = SharedUnitLoader.instance();
        
        DriverUnit driverUnit = (DriverUnit)unitLoader.getUnit(Driver.class);
        
        List<Driver> drivers = driverUnit.getList();
        
        System.out.println("");
        System.out.println("Drivers:");
        for (Driver driver : drivers)
            System.out.println("  " + driver.getName());
        
        ConnectorUnit connectorUnit = (ConnectorUnit)unitLoader
                .getUnit(DataSetConnector.class);
        
        List<DataSetConnector<?, ?>> connectors = connectorUnit.getList();
        
        System.out.println("");
        System.out.println("Connectors:");
        
        for (DataSetConnector<?, ?> connector : connectors)
            System.out.println("  " + connector.getName());
        
    }
    
    /**
     * Prints the help.
     * 
     * @param etlConfig
     *            the etl config
     */
    private void printHelp(EtlConfig etlConfig)
    {
        System.out.println("");
        System.out.println("USAGE:");
        System.out
                .println("   etlprocess [-? | -h | -help | -c | -d | -v | -u | -config | -drivers | -version | -update ]");
        System.out.println("");
        System.out.println("where");
        System.out
                .println("   -? or -h or -help                Display this help message.");
        System.out
                .println("   -c or -d or -config or -drivers  Display drivers and connectors.");
        System.out
                .println("   -v or -version                   Check software version.");
        System.out
                .println("   -u or -update                    Check software version and install update if new version exists.");
        
        System.out.println("");
        System.out.println("Configuration file name: "
                + etlConfig.getXmlConfigFileName());
        System.out.println("Etl scenarios folder:    "
                + etlConfig.getScenarioPath());
    }
    
    /**
     * Sets the loop variable.
     * 
     * @param var
     *            the variable
     * @param pattern
     *            the pattern
     * @param params
     *            the params
     */
    private void setLoopVariable(Variable var, String pattern, String params)
    {
        if (var == null || Utils.isNothing(pattern))
            return;
        
        pattern = EtlUtils
                .mergeStringWithParams(pattern, new String[] {params});
        
        var.setValue(pattern);
    }
    
    /**
     * Updates software
     * 
     */
    public void update()
    {
        System.out.println("");
        System.out.println("Checking for updates...");
        
        Updater.instance().checkForUpdates(null, false);
        
        UpdaterResponse response = Updater.instance().getResponse();
        
        if (response.getResponseCode() == UpdaterResponse.ResponseCode.ERROR
                || response.getResponseCode() == UpdaterResponse.ResponseCode.NO_SERVICE)
        {
            System.out
                    .println("Update status: update service is not available");
            
            return;
        }
        else if (response.getResponseCode() == UpdaterResponse.ResponseCode.READY_TO_DOWNLOAD)
        {
            System.out.println("Update status: "
                    + UpdaterResponse.CODES.get(response.getResponseCode())
                    + ". Downloading...");
            
            Updater.instance().downloadUpdate(response, null);
            
            response = Updater.instance().getResponse();
        }
        
        System.out.println("Update status: "
                + UpdaterResponse.CODES.get(response.getResponseCode()));
        
    }
    
    /**
     * Waits until done. Basically waits until all currently execute in parallel
     * scenarios finished or terminated.
     * 
     * @param executor
     *            the executor
     * @param wait
     *            the wait flag.
     * @throws Exception
     *             in case of any error
     */
    private void waitUntilDone(ParallelExecutor executor, boolean wait)
        throws Exception
    {
        if (executor != null)
        {
            if (executor.isTerminated())
                return;
            
            // wait
            if (wait)
                executor.waitUntilDone();
        }
    }
}
