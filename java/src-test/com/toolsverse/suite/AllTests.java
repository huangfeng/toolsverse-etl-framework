/*
 * AllTests.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.suite;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.toolsverse.config.SystemConfigTest;
import com.toolsverse.etl.common.DataSetTest;
import com.toolsverse.etl.connector.excel.ExcelConnectorTest;
import com.toolsverse.etl.connector.sql.SqlConnectorTest;
import com.toolsverse.etl.connector.text.TextConnectorTest;
import com.toolsverse.etl.connector.xml.XmlConnectorTest;
import com.toolsverse.etl.core.config.EtlConfigTest;
import com.toolsverse.etl.core.connection.EtlConnectionFactoryTest;
import com.toolsverse.etl.core.engine.EtlFactoryTest;
import com.toolsverse.etl.core.engine.EtlProcessTest;
import com.toolsverse.etl.core.engine.ExtractorTest;
import com.toolsverse.etl.core.task.common.TasksTest;
import com.toolsverse.etl.core.util.EtlUtilsTest;
import com.toolsverse.etl.driver.DriverTest;
import com.toolsverse.etl.parser.GenericSqlParserTest;
import com.toolsverse.etl.sql.config.SqlConfigTest;
import com.toolsverse.etl.sql.connection.ConnectionFactoryTest;
import com.toolsverse.etl.sql.service.SqlServiceTest;
import com.toolsverse.etl.sql.util.SqlUtilsTest;
import com.toolsverse.io.IoProcessorTest;
import com.toolsverse.mvc.model.ModelTest;
import com.toolsverse.mvc.test.MvcTest;
import com.toolsverse.resource.ResourceTest;
import com.toolsverse.service.ServiceFactoryTest;
import com.toolsverse.util.AvgNumberTest;
import com.toolsverse.util.ClassUtilsTest;
import com.toolsverse.util.CollectionUtilsTest;
import com.toolsverse.util.DataStructureTest;
import com.toolsverse.util.FileUtilsTest;
import com.toolsverse.util.ScriptTest;
import com.toolsverse.util.TreeNodeTest;
import com.toolsverse.util.UrlUtilsTest;
import com.toolsverse.util.UtilsTest;
import com.toolsverse.util.aspect.AspectTest;
import com.toolsverse.util.collector.AppInfoCollectorTest;
import com.toolsverse.util.concurrent.ConcurrentExecutorTest;
import com.toolsverse.util.concurrent.FileLockerTest;
import com.toolsverse.util.concurrent.ParallelExecutorTest;
import com.toolsverse.util.encryption.SymmetricEncryptorTest;
import com.toolsverse.util.factory.ObjectFactoryTest;
import com.toolsverse.util.history.HistoryTest;
import com.toolsverse.util.log.LoggerTest;

/**
 * AllTests
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

@RunWith(Suite.class)
@Suite.SuiteClasses({ClassUtilsTest.class, LoggerTest.class,
        ModelTest.class, MvcTest.class, 
        ObjectFactoryTest.class, HistoryTest.class, DataStructureTest.class,
        UrlUtilsTest.class, FileUtilsTest.class, CollectionUtilsTest.class,
        UtilsTest.class, ServiceFactoryTest.class, TreeNodeTest.class,
        
        EtlFactoryTest.class, SqlUtilsTest.class, DataSetTest.class,
        EtlUtilsTest.class, TextConnectorTest.class, XmlConnectorTest.class,
        SqlConnectorTest.class, ExcelConnectorTest.class, EtlConfigTest.class,
        
        ParallelExecutorTest.class, ConcurrentExecutorTest.class,
        ExtractorTest.class, TasksTest.class, EtlConnectionFactoryTest.class,
        EtlProcessTest.class, 
        SystemConfigTest.class,
        AppInfoCollectorTest.class, 
        IoProcessorTest.class, 
        SqlConfigTest.class,
        ConnectionFactoryTest.class, SqlServiceTest.class,
        AspectTest.class, DriverTest.class,
        ResourceTest.class, 
        FileLockerTest.class, 
        GenericSqlParserTest.class, ScriptTest.class, AvgNumberTest.class})
public class AllTests
{
}
