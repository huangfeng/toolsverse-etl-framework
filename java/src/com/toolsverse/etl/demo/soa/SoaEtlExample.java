/*
 * SoaEtlExample.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.demo.soa;

import java.util.Calendar;
import java.util.Date;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.etl.common.Alias;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.etl.core.engine.EtlRequest;
import com.toolsverse.etl.core.engine.EtlResponse;
import com.toolsverse.etl.core.engine.Scenario;
import com.toolsverse.etl.core.service.EtlService;
import com.toolsverse.service.ServiceFactory;
import com.toolsverse.service.web.ServiceProxyWeb;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * This example demonstrates running ETL in the client-server mode. The ETL request must include all connection aliases
 * which will be used by remote ETL process to create connections. Also you need to specify scenario file name. 
 * <br>
 * <br>
 * The ETL server must be up and running. You can check server status by entering server's url in the Web browser.
 * The default server's URL is http://localhost:8080/dataexplorer/ide. If you your's is different use -Dapp.server.url JVM option.
 * <pre>
 * Example: -Dapp.server.url=http://host:port/dataexplorer/ide.
 * </pre>
 * Please refer to the Data Explorer User guide if you need to install ETL server.
 * 
 * <pre>
 * The ETl scenario is WEB_APP_HOME/data/scenario/xml2text.xml. It is configured to extract data from the XML file and 
 * load it as is into text file. 
 * 
 * The input file is WEB_APP_HOME/data/etl_test.xml, output - WEB_APP_HOME/data/etl_test.dat. 
 * 
 * Output file is created only after scenario is executed.
 * </pre>
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public class SoaEtlExample
{
    /**
     * Creates ETL request and remotely executes ETL scenario. 
     * 
     * @param args the command line arguments
     */
    public static void main(String[] args)
    {
        SoaEtlExample engine = new SoaEtlExample();
        
        try
        {
            EtlResponse response = engine.execute();
            
            // prints out formatted output
            // from the ETL response
            System.out.println(engine.getMessage(response, "xml2text.xml"));
        }
        catch (Exception ex)
        {
            System.out.println(Utils.getStackTraceAsString(ex));
        }
        
        System.exit(0);
    }
    
    /**
     * Creates connection aliases for ETL process, sets scenario name, remotely executes ETL scenario.
     * 
     * @return ETL response
     * @throws Exception in case of any error
     */
    private EtlResponse execute()
        throws Exception
    {
        // initializes system config, loads properties
        SystemConfig.instance();
        
        // instantiates ETL config
        EtlConfig config = new EtlConfig();
        
        // initializes ETl config
        config.init();
        
        // creates source alias
        Alias source = new Alias();
        source.setName("xml_con");
        source.setConnectorClassName("com.toolsverse.etl.connector.xml.XmlConnector");
        source.setUrl("{app.root.data}/etl_test.xml");
        
        // creates destination alias
        Alias destination = new Alias();
        destination.setName("text_con");
        destination
                .setConnectorClassName("com.toolsverse.etl.connector.text.TextConnector");
        destination.setUrl("{app.root.data}/etl_test.dat");
        destination.setParams("delimiter=';';firstrow=false;metadata=false");
        
        // adds aliases. The remote ETL process will create connections from
        // these aliases
        config.addAliasToMap(source.getName(), source);
        config.addAliasToMap(destination.getName(), destination);
        
        // creates empty ETL scenario, sets ETL action. The remote ETL process
        // will load it from
        // XML at run-time.
        Scenario scenario = new Scenario();
        scenario.setName("xml2text.xml");
        scenario.setAction(EtlConfig.EXTRACT_LOAD);
        
        // creates ETL request using given config, scenario and log level
        EtlRequest request = new EtlRequest(config, scenario, Logger.INFO);
        
        if (Utils.isNothing(SystemConfig.instance().getSystemProperty(
                SystemConfig.SERVER_URL)))
            SystemConfig.instance().setSystemProperty(SystemConfig.SERVER_URL,
                    "http://localhost:8080/dataexplorer/ide");
        
        // gets ETL service from the factory. The ServiceProxyWeb used as a
        // dynamic proxy
        EtlService etlService = (EtlService)ServiceFactory.getService(
                EtlService.class, ServiceProxyWeb.class.getName());
        
        // remotely executes ETL process
        return etlService.executeEtl(request);
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
    
}
