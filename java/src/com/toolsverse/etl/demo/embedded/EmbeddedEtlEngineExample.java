/*
 * EmbeddedEtlEngineExample.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.demo.embedded;

import java.util.Calendar;
import java.util.Date;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlFactory;
import com.toolsverse.etl.core.engine.EtlProcess;
import com.toolsverse.etl.core.engine.EtlRequest;
import com.toolsverse.etl.core.engine.EtlResponse;
import com.toolsverse.etl.core.engine.Scenario;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * This example demonstrates embedding ETL engine into application.  
 * <pre>
 * There are two modes: 
 * 1. Load ETL configuration file. Execute ETL process using connections and
 * scenario specified in the file. 
 * 2. Manually create connection aliases and load ETL scenario. Execute ETL process using these aliases and scenario.
 * 
 * The ETl scenario is APP_HOME/data/scenario/xml2text.xml. It is configured to extract data from the XML file and 
 * load it as is into text file. 
 * 
 * The input file is APP_HOME/data/etl_test.xml, output is a APP_HOME/data/etl_test.txt for the first mode 
 * and APP_HOME/data/etl_test.dat for the second. 
 * 
 * Output files are created only after scenario is executed.
 * 
 * USAGE:
 *  EmbeddedEtlEngineExample [-? | -h | -help | -c | -config | -l | -load | -m | -manual]
 *   where
 *     -? or -h or -help    Display help message.
 *     -c or -config        Display loaded drivers and connectors.
 *     -l or -load          Load configuration file, execute ETL scenario.
 *     -m or -manual        Manually create connection aliases, load and execute ETL scenario.
 * </pre>
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class EmbeddedEtlEngineExample
{
    /**
     * Configures ETL engine and executes ETL scenario. 
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        EmbeddedEtlEngineExample engine = new EmbeddedEtlEngineExample();
        
        try
        {
            // instantiates ETL config
            EtlConfig etlConfig = new EtlConfig();
            
            // creates embedded ETL process
            EtlProcess etlProcess = new EtlProcess(EtlProcess.EtlMode.EMBEDDED);
            
            // prints out name and version
            System.out.println(SystemConfig.instance().getTitle(
                    EtlConfig.DEFAULT_TITLE)
                    + " "
                    + SystemConfig.instance().getSystemProperty(
                            SystemConfig.VERSION));
            
            // parsers arguments and executes appropriate action
            engine.parseArgs(args, etlProcess, etlConfig);
        }
        catch (Exception ex)
        {
            System.out.println(Utils.getStackTraceAsString(ex));
        }
        
        System.exit(0);
    }
    
    /**
     * Manually creates connection aliases for the ETL process, loads given ETL scenario from the default folder.
     * Executes ETl scenario.
     * @param config the ETL config
     * @param scenariFileName the ETL scenario file name
     * @param etlProcess the ETl process
     * @return ETL response
     * @throws Exception in case of any error
     */
    private EtlResponse createConfigAndExecute(EtlConfig config,
            String scenariFileName, EtlProcess etlProcess)
        throws Exception
    {
        // initializes ETl config
        config.init();
        
        // creates source alias
        Alias source = new Alias();
        source.setName("xml_con");
        source.setConnectorClassName("com.toolsverse.etl.connector.xml.XmlConnector");
        source.setUrl("{app.data}/etl_test.xml");
        
        // creates destination alias
        Alias destination = new Alias();
        destination.setName("text_con");
        destination
                .setConnectorClassName("com.toolsverse.etl.connector.text.TextConnector");
        destination.setUrl("{app.data}/etl_test.dat");
        destination.setParams("delimiter=';';firstrow=false;metadata=false");
        
        // adds aliases. ETL process will create connections from these aliases
        config.addAliasToMap(source.getName(), source);
        config.addAliasToMap(destination.getName(), destination);
        
        // instantiates ETL factory
        EtlFactory etlFactory = new EtlFactory();
        
        // loads given ETl scenario, sets ETL action
        Scenario scenario = etlFactory.getScenario(config, scenariFileName);
        scenario.setAction(EtlConfig.EXTRACT_LOAD);
        
        // creates ETl request using given config, scenario and log level
        EtlRequest request = new EtlRequest(config, scenario, Logger.INFO);
        
        // executes ETL process
        return etlProcess.execute(request);
    }
    
    /**
     * Gets the formatted message from the ETL response
     * 
     * @param response the ETl response
     * @param scenarioName the ETL scenario name
     * 
     * @return formatted message from the ETL response
     */
    private String getMessage(EtlResponse response, String scenarioName)
    {
        String msg = "";
        
        String start = response != null ? response.getStartTime().toString()
                : new Date().toString();
        String end = response != null && response.getEndTime() != null ? response
                .getEndTime().toString() : new Date().toString();
        String diff = response != null ? String
                .valueOf(Utils.getDateDiff(response.getEndTime(),
                        response.getStartTime(), Calendar.SECOND)) : "0";
        
        if (response.getRetCode() == EtlConfig.RETURN_OK)
            msg = Utils
                    .format("\nScenario %1 executed.\nStarted at %2, finished at %3, total execution time %4 seconds.",
                            new String[] {scenarioName, start, end, diff});
        else
        {
            msg = Utils
                    .format("\nScenario %1 executed with errors.\nStarted at %2, finished at %3, total execution time %4 seconds.",
                            new String[] {scenarioName, start, end, diff})
                    + "\n"
                    + Utils.getStackTraceAsString(response.getException());
            
        }
        
        return msg;
        
    }
    
    /**
     * Loads ETL configuration file using given name. Creates connections for the ETL process
     * by parsing config file. Executes ETL scenario specified in the ETL configuration file.
     * @param config the ETl config
     * @param configFileName the ETL config file name
     * @param etlProcess the ETL process
     * @return ETl response
     * @throws Exception in case of any error
     */
    private EtlResponse loadConfigAndExecute(EtlConfig config,
            String configFileName, EtlProcess etlProcess)
        throws Exception
    {
        // loads specified configuration file
        // from the default folder: APP_HOME/config. Creates connections and
        // loads scenario in process.
        if (!config.initConfigXml(configFileName))
        {
            System.out.println("Cannot read " + config.getXmlConfigFileName()
                    + ". Check log for details.");
            
            System.exit(0);
        }
        
        // executes ETL scenario specified in the configuration file
        return etlProcess.execute(config);
    }
    
    /**
     * Parses command line arguments and executes appropriate actions.
     * 
     * @param args
     *            the command line arguments
     * @param etlProcess the ETL process           
     * @param etlConfig
     *            the ETL config
     * @throws Exception in case of any error           
     */
    private void parseArgs(String[] args, EtlProcess etlProcess,
            EtlConfig etlConfig)
        throws Exception
    {
        if (args != null && args.length > 0)
        {
            for (String arg : args)
            {
                arg = arg.trim().toLowerCase();
                
                if (arg.matches("-(\\?|h|help)"))
                {
                    printHelp();
                }
                
                if (arg.matches("-(c|config)"))
                {
                    etlProcess.printConfig(etlConfig);
                }
                
                if (arg.matches("-(l|load)"))
                {
                    // loads specified ETL configuration file. Executes ETL
                    // scenario specified in the file
                    EtlResponse response = loadConfigAndExecute(etlConfig,
                            "test_etl_config.xml", etlProcess);
                    
                    // prints out formatted output
                    // from the ETL response
                    System.out.println(getMessage(response, etlConfig
                            .getExecute().get(0).getName()));
                }
                else if (arg.matches("-(m|manual)"))
                {
                    // manually creates ETL configuration, loads ETL scenario
                    // from the default folder and executes it
                    EtlResponse response = createConfigAndExecute(etlConfig,
                            "xml2text.xml", etlProcess);
                    
                    // prints out formatted output
                    // from the ETL response
                    System.out.println(getMessage(response, "xml2text.xml"));
                    
                }
                
            }
        }
        else
            printHelp();
    }
    
    /**
     * Prints the help.
     */
    private void printHelp()
    {
        System.out.println("");
        System.out.println("USAGE:");
        System.out
                .println("   EmbeddedEtlEngineExample [-? | -h | -help | -c | -config | -l | -load | -m | -manual]");
        System.out.println("");
        System.out.println("where");
        System.out
                .println("   -? or -h or -help    Display this help message.");
        System.out
                .println("   -c or -config        Display loaded drivers and connectors.");
        System.out
                .println("   -l or -load          Load configuration file, load and execute ETL scenario.");
        System.out
                .println("   -m or -manual        Manualy create connection aliases, load and execute ETL scenario.");
    }
}