/*
 * EtlRequest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.util.log.Logger;

/**
 * This data structure used to send etl request.
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class EtlRequest implements Serializable
{
    
    /** The etl config. */
    private EtlConfig _config;
    
    /** The scenarios. */
    private List<Scenario> _scenarios;
    
    /** The log level. */
    private int _logLevel;
    
    /**
     * Instantiates a new etl request.
     */
    public EtlRequest()
    {
        _config = null;
        _scenarios = null;
        _logLevel = Logger.SEVERE;
    }
    
    /**
     * Instantiates a new etl request.
     *
     * @param config the etl config
     * @param scenarios the scenarios to execute
     * @param logLevel the log level
     */
    public EtlRequest(EtlConfig config, List<Scenario> scenarios, int logLevel)
    {
        _config = config;
        _scenarios = scenarios;
        _logLevel = logLevel;
    }
    
    /**
     * Instantiates a new etl request.
     *
     * @param config the etl config
     * @param scenario the scenario to execute
     * @param logLevel the log level
     */
    public EtlRequest(EtlConfig config, Scenario scenario, int logLevel)
    {
        _config = config;
        
        _scenarios = null;
        
        addScenario(scenario);
        
        _logLevel = logLevel;
    }
    
    /**
     * Adds scenario
     * @param scenario the ETl scenario to add
     */
    public void addScenario(Scenario scenario)
    {
        if (scenario == null)
            return;
        
        if (_scenarios == null)
        {
            _scenarios = new ArrayList<Scenario>();
        }
        
        _scenarios.add(scenario);
    }
    
    /**
     * Gets the etl config.
     *
     * @return the etl config
     */
    public EtlConfig getEtlConfig()
    {
        return _config;
    }
    
    /**
     * Gets the log level.
     *
     * @return the log level
     */
    public int getLogLevel()
    {
        return _logLevel;
    }
    
    /**
     * Gets the scenarios.
     *
     * @return the scenarios
     */
    public List<Scenario> getScenarios()
    {
        return _scenarios;
    }
    
    /**
     * Sets the etl config.
     *
     * @param value the new etl config
     */
    public void setEtlConfig(EtlConfig value)
    {
        _config = value;
    }
    
    /**
     * Sets the log level.
     *
     * @param value the new log level
     */
    public void setLogLevel(int value)
    {
        _logLevel = value;
    }
    
    /**
     * Sets the scenarios.
     *
     * @param value the new scenarios
     */
    public void setScenarios(List<Scenario> value)
    {
        _scenarios = value;
    }
    
}
