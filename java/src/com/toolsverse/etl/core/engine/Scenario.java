/*
 * Scenario.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.etl.core.engine;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.toolsverse.etl.common.ConditionalExecution;
import com.toolsverse.etl.common.Variable;
import com.toolsverse.etl.core.config.EtlConfig;
import com.toolsverse.util.ListHashMap;
import com.toolsverse.util.Utils;

/**
 * This class defines an etl (extract, transfer, load) scenario. Typically contains multiple Sources (extract), Tasks (transformations) and Destinations (loads).The
 * ETL framework reads scenario from the xml file and executes it using <code>EtlProcess</code>. Generally speaking etl scenario is a program in the domain 
 * specific language. It can include code in other languages as well, for example SQL and JavaScript.        
 *
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class Scenario extends ConditionalExecution implements Serializable
{
    // domain values for on action events
    
    /** The ON_ACTION_PARENT - same behavior as parent scenario. */
    public static final int ON_ACTION_PARENT = -1;
    
    /** The ON_ACTION_SAVE - save file when event occurs. */
    public static final int ON_ACTION_SAVE = 0;
    
    /** The ON_ACTION_SKIP - do not save file when event occurs. */
    public static final int ON_ACTION_SKIP = 1;
    
    /** The ON_ACTION_SAVE_STR - the ON_ACTION_SAVE code. Save file when event occurs.  */
    public static final String ON_ACTION_SAVE_STR = "save";
    
    /** The ON_ACTION_SKIP_STR - the ON_ACTION_SKIP code. Do not save file when event occurs. */
    public static final String ON_ACTION_SKIP_STR = "skip";
    
    /** The ON_ACTION mapping. */
    public static final Map<String, Integer> ON_ACTION = new HashMap<String, Integer>();
    static
    {
        ON_ACTION.put(ON_ACTION_SAVE_STR, new Integer(ON_ACTION_SAVE));
        ON_ACTION.put(ON_ACTION_SKIP_STR, new Integer(ON_ACTION_SKIP));
    }
    
    /** The Constant REQUIRE_SOURCE_ATTR. */
    public static final String REQUIRE_SOURCE_ATTR = "requiresource";
    
    /** The Constant REQUIRE_DEST_ATTR. */
    public static final String REQUIRE_DEST_ATTR = "requiredest";
    
    /** The Constant NO_TEMP_TABLES_ATTR. */
    public static final String NO_TEMP_TABLES_ATTR = "notemp";
    
    /** The name. */
    private String _name;
    
    /** The script name. */
    private String _scriptName;
    
    /** The destination connection name. */
    private String _destConnectionName;
    
    /** The driver. */
    private String _driver;
    
    /** The code generator class. */
    private String _codeGenClass;
    
    /** The default function class. */
    private String _defaultFunctionClass;
    
    /** The sources. */
    private ListHashMap<String, Source> _sources;
    
    /** The mandatory sources. */
    private ListHashMap<String, Source> _mandatorySources;
    
    /** The destinations. */
    private ListHashMap<String, Destination> _destinations;
    
    /** The variables. */
    private ListHashMap<String, Variable> _variables;
    
    /** The on save action. */
    private int _onSave;
    
    /** The on persist data set action. */
    private int _onPersistDataSet;
    
    /** The on populate data set action. */
    private int _onPopulateDataSet;
    
    /** The action. */
    private int _action;
    
    /** The list of inner scenarios. */
    private List<Scenario> _execute;
    
    /** The "is inner" flag. */
    private boolean _isInner;
    
    /** The count of parallel sources. */
    private int _parallelSources;
    
    /** The count of parallel destinations. */
    private int _parallelDests;
    
    /** The _parallel inner scenarious. */
    private int _parallelInnerScenarious;
    
    /** The metadata extractor class. */
    private String _metadataExtractorClass;
    
    /** The "use metadata data types" flag. */
    private boolean _useMetadataDataTypes;
    
    /** The "has parallel connections" flag. */
    private boolean _parallelConnections;
    
    /** The "is parallel scenario" flag. */
    private boolean _parallelScenario;
    
    /** The is "parallel inner scenario" flag. */
    private boolean _parallelInnerScenario;
    
    /** The allowed actions. */
    private String _allowedActions;
    
    /** The description. */
    private String _description;
    
    /** The loop code. */
    private String _loopCode;
    
    /** The loop language. */
    private String _loopLang;
    
    /** The loop count. */
    private String _loopCount;
    
    /** The loop variable name. */
    private String _loopVarName;
    
    /** The loop variable pattern. */
    private String _loopVarPattern;
    
    /** The loop field. */
    private String _loopField;
    
    /** The loop connection name. */
    private String _loopConnectionName;
    
    /** The id. */
    private String _id;
    
    /** The attributes. */
    private Map<String, String> _attrs;
    
    /** The ready flag. */
    private boolean _ready;
    
    /** The _commit each block. */
    private boolean _commitEachBlock;
    
    /**
     * Instantiates a new etl scenario.
     */
    public Scenario()
    {
        _name = null;
        _scriptName = null;
        _destConnectionName = null;
        _driver = null;
        _codeGenClass = null;
        _defaultFunctionClass = null;
        _sources = null;
        _mandatorySources = null;
        _destinations = null;
        _variables = new ListHashMap<String, Variable>();
        _onSave = ON_ACTION_SKIP;
        _onPersistDataSet = ON_ACTION_SKIP;
        _onPopulateDataSet = ON_ACTION_SKIP;
        _commitEachBlock = false;
        _action = EtlConfig.DEFAULT_ACTION;
        
        _execute = null;
        _isInner = false;
        
        _parallelSources = 0;
        _parallelDests = 0;
        _parallelInnerScenarious = 0;
        
        _metadataExtractorClass = null;
        
        _useMetadataDataTypes = true;
        
        _parallelConnections = false;
        _parallelScenario = false;
        _parallelInnerScenario = false;
        
        _loopCode = null;
        _loopLang = Variable.DEFAULT_LANG;
        _loopCount = null;
        _loopVarName = null;
        _loopVarPattern = null;
        _loopField = null;
        _loopConnectionName = null;
        
        _allowedActions = null;
        _description = null;
        _id = "etl" + Utils.getUUIDName();
        
        _attrs = null;
        
        _ready = false;
    }
    
    /**
     * Adds the parallel inner scenario.
     */
    public void addParallelInnerScenario()
    {
        _parallelInnerScenarious++;
    }
    
    /**
     * Adds the variable.
     *
     * @param var the variable
     */
    public void addVariable(Variable var)
    {
        if (_variables == null)
            _variables = new ListHashMap<String, Variable>();
        
        _variables.put(var.getName(), var);
    }
    
    /**
     * Assign variables from this scenario to the <code>toScenario</code>.
     *
     * @param toScenario the scenario to assign variables from this 
     */
    public void assignVars(Scenario toScenario)
    {
        if (toScenario.getVariables() == null)
        {
            toScenario.setVariables(getVariables());
            
            return;
        }
        
        if (getVariables() != null && getVariables().size() > 0)
            for (int i = 0; i < getVariables().size(); i++)
            {
                Variable fromVar = getVariables().get(i);
                Variable toVar = toScenario.getVariable(fromVar.getName());
                
                if (toVar != null)
                    toVar.setValue(fromVar.getValue());
                else if (fromVar.isGlobal())
                    toScenario.getVariables().put(fromVar.getName(), fromVar);
            }
    }
    
    /**
     * Removes attribute.
     *
     * @param name the name
     */
    public void clearAttr(String name)
    {
        if (_attrs != null)
            _attrs.remove(name);
    }
    
    /**
     * Gets the action. Possible values: EtlConfig.NOTHING, EtlConfig.EXTRACT, EtlConfig.LOAD, EtlConfig.EXTRACT_LOAD.
     *
     * @return the action
     */
    public int getAction()
    {
        return _action;
    }
    
    /**
     * Gets the allowed actions.
     *
     * @return the allowed actions
     */
    public String getAllowedActions()
    {
        return _allowedActions;
    }
    
    /**
     * Gets the attributes.
     * 
     * @return the attributes
     */
    public Map<String, String> getAttrs()
    {
        return _attrs;
    }
    
    /**
     * Gets the attribute value by name.
     * 
     * @param name
     *            the attribute name.
     * @return the value
     */
    public String getAttrValue(String name)
    {
        return _attrs != null ? _attrs.get(name) : null;
    }
    
    /**
     * Gets the code generator class name.
     *
     * @return the code generator class name
     */
    public String getCodeGenClass()
    {
        return _codeGenClass;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.etl.common.ConditionalExecution#getDefaultConnectionName()
     */
    @Override
    public String getDefaultConnectionName()
    {
        return EtlConfig.SOURCE_CONNECTION_NAME;
    }
    
    /**
     * Gets the default function class name.
     *
     * @return the default function class name
     */
    public String getDefaultFunctionClass()
    {
        return _defaultFunctionClass;
    }
    
    /**
     * Gets the description.
     *
     * @return the description
     */
    public String getDescription()
    {
        return !Utils.isNothing(_description) ? _description : getName();
    }
    
    /**
     * Gets the default destination connection name.
     *
     * @return the destination connection name
     */
    public String getDestConnectionName()
    {
        return _destConnectionName;
    }
    
    /**
     * Gets the destination by source.
     *
     * @param source the source
     * @return the destination
     */
    public Destination getDestinationBySource(Source source)
    {
        if (source == null
                || (source.isIndependent() == null || source.isIndependent())
                || _destinations == null || _destinations.size() == 0)
            return null;
        
        for (Destination dest : _destinations.getList())
        {
            if (dest.getSource() == source)
                return dest;
        }
        
        return _destinations.get(source.getName());
    }
    
    /**
     * Gets the destinations.
     *
     * @return the destinations
     */
    public ListHashMap<String, Destination> getDestinations()
    {
        return _destinations;
    }
    
    /**
     * Gets the driver class name.
     *
     * @return the driver class name
     */
    public String getDriverClassName()
    {
        return _driver;
    }
    
    /**
     * Gets the list of inner scenarios.
     *
     * @return the list of inner scenarios
     */
    public List<Scenario> getExecute()
    {
        return _execute;
    }
    
    /**
     * Gets the unique id.
     *
     * @return the unique id
     */
    public String getId()
    {
        return _id;
    }
    
    /**
     * Gets the loop code. Loop code is program in one of the supported languages (SQL, JavaScript) for the loop condition: execute scenario over again until...
     *
     * @return the loop code
     */
    public String getLoopCode()
    {
        return _loopCode;
    }
    
    /**
     * Gets the loop connection name. This connection will be used to execute sql defined by <code>getLoopCode()</code>. 
     *
     * @return the loop connection name
     */
    public String getLoopConnectionName()
    {
        return _loopConnectionName;
    }
    
    /**
     * Gets the loop count. The loop count is way to split execution in chunks. For example there is very big driving table. The scenario can be configured 
     * to execute for every 1000 rows of this table.  
     *
     * @return the loop count
     */
    public String getLoopCount()
    {
        return _loopCount;
    }
    
    /**
     * Gets the loop field. The field in the driving table for the loop. 
     *
     * @return the loop field
     */
    public String getLoopField()
    {
        return _loopField;
    }
    
    /**
     * Gets the loop language. This is language for the code defined by <code>getLoopCode()</code>. Currently supported languages: SQL, JavaScript.
     *
     * @return the loop language
     */
    public String getLoopLang()
    {
        return _loopLang;
    }
    
    /**
     * Gets the loop variable name.
     *
     * @return the loop variable name
     */
    public String getLoopVarName()
    {
        return _loopVarName;
    }
    
    /**
     * Gets the loop variable pattern.
     *
     * @return the loop variable pattern
     */
    public String getLoopVarPattern()
    {
        return _loopVarPattern;
    }
    
    /**
     * Gets the list of mandatory sources. The mandatory sources must have a isMandatory() attribute set to true. The data from these sources will be extracted first
     * and they can not participate in the steaming from the source to destination.    
     *
     * @return the list of mandatory sources
     */
    public ListHashMap<String, Source> getMandatorySources()
    {
        return _mandatorySources;
    }
    
    /**
     * Gets the metadata extractor class name.
     *
     * @return the metadata extractor class name
     */
    public String getMetadataExtractorClass()
    {
        return _metadataExtractorClass;
    }
    
    /**
     * Gets the name.
     *
     * @return the name
     */
    public String getName()
    {
        return _name;
    }
    
    /**
     * Gets the action for "on persist data set" event. Possible values: ON_ACTION_SAVE - saves data set as an xml file, ON_ACTION_SKIP - does not save data set.    
     *
     * @return the action for "on persist data set" event
     */
    public int getOnPersistDataSet()
    {
        return _onPersistDataSet;
    }
    
    /**
     * Gets the action for "on persist data set" event from the string.
     *
     * @param action the action
     * @return the action for "on persist data set" event
     */
    public int getOnPersistDataSet(String action)
    {
        if (action == null)
            return ON_ACTION_SAVE;
        
        Integer value = ON_ACTION.get(action.toLowerCase());
        
        if (value == null)
            return ON_ACTION_SAVE;
        
        return value.intValue();
    }
    
    /**
     * Gets the action for "on populate data set" event. Possible values: ON_ACTION_SAVE - saves sql into the file in case of error, ON_ACTION_SKIP - does not save sql.
     *
     * @return the action for "on populate data set" event
     */
    public int getOnPopulateDataSet()
    {
        return _onPopulateDataSet;
    }
    
    /**
     * Gets the action for "on populate data set" event from the string.
     *
     * @param action the action
     * @return the action for "on populate data set" event
     */
    public int getOnPopulateDataSet(String action)
    {
        if (action == null)
            return ON_ACTION_SKIP;
        
        Integer value = ON_ACTION.get(action.toLowerCase());
        
        if (value == null)
            return ON_ACTION_SKIP;
        
        return value.intValue();
    }
    
    /**
     * Gets the action for "on save" event. Possible values: ON_ACTION_SAVE - save load sql into the file, ON_ACTION_SKIP - does not save load sql into the file.
     *
     * @return the action for "on save" event
     */
    public int getOnSave()
    {
        return _onSave;
    }
    
    /**
     * Gets the action for "on save" event.
     *
     * @param action the action
     * @return the action for "on save" event
     */
    public int getOnSave(String action)
    {
        if (action == null)
            return ON_ACTION_SAVE;
        
        Integer value = ON_ACTION.get(action.toLowerCase());
        
        if (value == null)
            return ON_ACTION_SAVE;
        
        return value.intValue();
    }
    
    /**
     * Gets the count of the destinations which loaded in parallel threads.
     *
     * @return the count of the destinations which loaded in parallel threads
     */
    public int getParallelDests()
    {
        return _parallelDests;
    }
    
    /**
     * Gets the count inner scenarios which executed in parallel threads.
     *
     * @return the count inner scenarios which executed in parallel threads
     */
    public int getParallelInnerScenarious()
    {
        return _parallelInnerScenarious;
    }
    
    /**
     * Gets the count of the sources which extracted in parallel threads.
     *
     * @return the count of the sources which extracted in parallel threads.
     */
    public int getParallelSources()
    {
        return _parallelSources;
    }
    
    /**
     * Gets the script name. 
     *
     * @return the script name
     */
    public String getScriptName()
    {
        return _scriptName;
    }
    
    /**
     * Gets the sources.
     *
     * @return the sources
     */
    public ListHashMap<String, Source> getSources()
    {
        return _sources;
    }
    
    /**
     * Gets the global "use metadata data types" flag. If true the destination database tables will be created (if needed) using fields data types from the source tables.    
     *
     * @return the global "use metadata data types" flag
     */
    public boolean getUseMetadataDataTypes()
    {
        return _useMetadataDataTypes;
    }
    
    /**
     * Gets the variable by name.
     *
     * @param name the name
     * @return the variable
     */
    public Variable getVariable(String name)
    {
        if (_variables == null)
            return null;
        
        return _variables.get(name);
    }
    
    /**
     * Gets the variables.
     *
     * @return the variables
     */
    public ListHashMap<String, Variable> getVariables()
    {
        return _variables;
    }
    
    /**
     * Checks if attribute is set and equals to true.
     *
     * @param name the name
     * @return true, if attribute is set and equals to true
     */
    public boolean isAttrSet(String name)
    {
        return Utils.str2Boolean(getAttrValue(name), false);
    }
    
    /**
     * Checks if engine should commit each block.
     *
     * @return true, if engine should commit each block
     */
    public boolean isCommitEachBlock()
    {
        return _commitEachBlock;
    }
    
    /**
     * Checks if this scenario is inner.
     *
     * @return true, if is inner
     */
    public boolean isInner()
    {
        return _isInner;
    }
    
    /**
     * Checks if scenario requires parallel connections.
     *
     * @return true, if scenario requires parallel connections
     */
    public boolean isParallelConnections()
    {
        return _parallelConnections;
    }
    
    /**
     * Checks if this is a parallel inner scenario.
     *
     * @return true, if this is a parallel inner scenario
     */
    public boolean isParallelInnerScenario()
    {
        return _parallelInnerScenario;
    }
    
    /**
     * Checks if this is a parallel scenario.
     *
     * @return true, if is parallel scenario
     */
    public boolean isParallelScenario()
    {
        return _parallelScenario;
    }
    
    /**
     * Checks if scenario is fully loaded and ready to be executed.
     * @return true if scenario is fully loaded and ready to be executed
     */
    public boolean isReady()
    {
        return _ready;
    }
    
    /**
     * Sets the action. Possible values: EtlConfig.NOTHING, EtlConfig.EXTRACT, EtlConfig.LOAD, EtlConfig.EXTRACT_LOAD.
     *
     * @param value the new action
     */
    public void setAction(int value)
    {
        _action = value;
    }
    
    /**
     * Sets the allowed actions. Example: EXTRACT|LOAD.
     *
     * @param value the new allowed actions
     */
    public void setAllowedActions(String value)
    {
        _allowedActions = value;
    }
    
    /**
     * Sets the attributes.
     * 
     * @param value
     *            the new attributes
     */
    public void setAttrs(Map<String, String> value)
    {
        _attrs = value;
    }
    
    /**
     * Sets the attribute value.
     *
     * @param name the name
     * @param value the value
     */
    public void setAttrValue(String name, String value)
    {
        if (_attrs == null)
            _attrs = new HashMap<String, String>();
        
        _attrs.put(name, value);
    }
    
    /**
     * Sets the code generator class name.
     *
     * @param value the new code generator class name
     */
    public void setCodeGenClass(String value)
    {
        _codeGenClass = value;
    }
    
    /**
     * Sets the "commit each block" flag. If true engine should commit each block.
     *
     * @param value the new value for the "commit each block" flag
     */
    public void setCommitEachBlock(boolean value)
    {
        _commitEachBlock = value;
    }
    
    /**
     * Sets the default function class name.
     *
     * @param value the new default function class name
     */
    public void setDefaultFunctionClass(String value)
    {
        _defaultFunctionClass = value;
    }
    
    /**
     * Sets the description.
     *
     * @param value the new description
     */
    public void setDescription(String value)
    {
        _description = value;
    }
    
    /**
     * Sets the destination connection name.
     *
     * @param value the new destination connection name
     */
    public void setDestConnectionName(String value)
    {
        _destConnectionName = value;
    }
    
    /**
     * Sets the destinations.
     *
     * @param value the destinations
     */
    public void setDestinations(ListHashMap<String, Destination> value)
    {
        _destinations = value;
    }
    
    /**
     * Sets the driver class name.
     *
     * @param value the new driver class name
     */
    public void setDriver(String value)
    {
        _driver = value;
    }
    
    /**
     * Sets the list of inner scenarios.
     *
     * @param value the list of inner scenarios
     */
    public void setExecute(List<Scenario> value)
    {
        _execute = value;
    }
    
    /**
     * Sets the unique id.
     *
     * @param value the new unique id
     */
    public void setId(String value)
    {
        _id = value;
    }
    
    /**
     * Sets "is inner" flag.
     *
     * @param value the new "is inner" flag
     */
    protected void setIsInner(boolean value)
    {
        _isInner = value;
    }
    
    /**
     * Sets the loop code.
     *
     * @param value the new loop code
     */
    public void setLoopCode(String value)
    {
        _loopCode = value;
    }
    
    /**
     * Sets the loop connection name.
     *
     * @param value the new loop connection name
     */
    public void setLoopConnectionName(String value)
    {
        _loopConnectionName = value;
    }
    
    /**
     * Sets the loop count.
     *
     * @param value the new loop count
     */
    public void setLoopCount(String value)
    {
        _loopCount = value;
    }
    
    /**
     * Sets the loop field.
     *
     * @param value the new loop field
     */
    public void setLoopField(String value)
    {
        _loopField = value;
    }
    
    /**
     * Sets the loop language.
     *
     * @param value the new loop language
     */
    public void setLoopLang(String value)
    {
        _loopLang = value;
    }
    
    /**
     * Sets the loop variable name.
     *
     * @param value the new loop variable name
     */
    public void setLoopVarName(String value)
    {
        _loopVarName = value;
    }
    
    /**
     * Sets the loop variable pattern.
     *
     * @param value the new loop variable pattern
     */
    public void setLoopVarPattern(String value)
    {
        _loopVarPattern = value;
    }
    
    /**
     * Sets the mandatory sources.
     *
     * @param value the mandatory sources
     */
    public void setMandatorySources(ListHashMap<String, Source> value)
    {
        _mandatorySources = value;
    }
    
    /**
     * Sets the metadata extractor class name.
     *
     * @param value the new metadata extractor class name
     */
    public void setMetadataExtractorClass(String value)
    {
        _metadataExtractorClass = value;
    }
    
    /**
     * Sets the name.
     *
     * @param value the new name
     */
    public void setName(String value)
    {
        _name = value;
    }
    
    /**
     * Sets the action for "on persist data set" event. Possible values: ON_ACTION_SAVE, ON_ACTION_SKIP
     *
     * @param value the action for "on persist data set" event
     */
    public void setOnPersistDataSet(int value)
    {
        _onPersistDataSet = value;
    }
    
    /**
     * Sets the action for "on persist data set" event from the string.
     *
     * @param action the new action for "on persist data set" event
     */
    public void setOnPersistDataSet(String action)
    {
        _onPersistDataSet = getOnPersistDataSet(action);
    }
    
    /**
     * Sets the action for "on populate data set" event. Possible values: ON_ACTION_SAVE, ON_ACTION_SKIP.
     *
     * @param value the new action for "on populate data set" event
     */
    public void setOnPopulateDataSet(int value)
    {
        _onPopulateDataSet = value;
    }
    
    /**
     * Sets the action for "on populate data set" event from the string.
     *
     * @param action the new action for "on populate data set" event
     */
    public void setOnPopulateDataSet(String action)
    {
        _onPopulateDataSet = getOnPopulateDataSet(action);
    }
    
    /**
     * Sets the action for "on save" event. Possible values: ON_ACTION_SAVE, ON_ACTION_SKIP.
     *
     * @param value the new action for "on save" event
     */
    public void setOnSave(int value)
    {
        _onSave = value;
    }
    
    /**
     * Sets the action for "on save" event from the string.
     *
     * @param action the new action for "on save" event
     */
    public void setOnSave(String action)
    {
        _onSave = getOnSave(action);
    }
    
    /**
     * Sets "has parallel connections" flag.
     *
     * @param value the "has parallel connections" flag
     */
    public void setParallelConnections(boolean value)
    {
        _parallelConnections = value;
    }
    
    /**
     * Sets the count of parallel destinations.
     *
     * @param value the count of parallel destinations
     */
    public void setParallelDests(int value)
    {
        _parallelDests = value;
    }
    
    /**
     * Sets the "is parallel inner scenario" flag.
     *
     * @param value the "is parallel inner scenario" flag
     */
    public void setParallelInnerScenario(boolean value)
    {
        _parallelInnerScenario = value;
    }
    
    /**
     * Sets the "is parallel scenario" flag.
     *
     * @param value the "is parallel scenario" flag
     */
    public void setParallelScenario(boolean value)
    {
        _parallelScenario = value;
    }
    
    /**
     * Sets the count of parallel sources.
     *
     * @param value the count of parallel sources.
     */
    public void setParallelSources(int value)
    {
        _parallelSources = value;
    }
    
    /**
     * Sets ready status for the scenario.
     *
     * @param value the ready status
     */
    public void setReady(boolean value)
    {
        _ready = true;
    }
    
    /**
     * Sets the script name.
     *
     * @param value the new script name
     */
    public void setScriptName(String value)
    {
        _scriptName = value;
    }
    
    /**
     * Sets the sources.
     *
     * @param value the sources
     */
    public void setSources(ListHashMap<String, Source> value)
    {
        _sources = value;
    }
    
    /**
     * Sets the "use metadata data types" flag.
     *
     * @param value the "use metadata data types" flag
     */
    public void setUseMetadataDataTypes(boolean value)
    {
        _useMetadataDataTypes = value;
    }
    
    /**
     * Sets the variables.
     *
     * @param value the variables
     */
    public void setVariables(ListHashMap<String, Variable> value)
    {
        _variables = value;
    }
}
