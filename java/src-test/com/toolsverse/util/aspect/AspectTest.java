/*
 * AspectTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.util.aspect;

import static org.junit.Assert.assertTrue;

import java.lang.reflect.Method;

import net.sf.cglib.proxy.MethodProxy;

import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.Utils;

/**
 * AspectTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class AspectTest
{
    public class TestAspect1 extends Aspect<TestClass>
    {
        @Override
        public Object intercept(Object obj, Method method, Object[] args,
                MethodProxy proxy)
            throws Throwable
        {
            if ("doSomething".equals(method.getName()))
                return "value";
            else
                return super.intercept(obj, method, args, proxy);
        }
    }
    
    public class TestAspect2 extends Aspect<TestClass>
    {
        @Override
        public Object intercept(Object obj, Method method, Object[] args,
                MethodProxy proxy)
            throws Throwable
        {
            if ("getSomething".equals(method.getName()))
                return "nothing";
            else
                return super.intercept(obj, method, args, proxy);
        }
    }
    
    @BeforeClass
    public static void setUp()
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
    }
    
    @Test
    public void testAspect1()
    {
        TestAspect1 aspect1 = new TestAspect1();
        
        aspect1.init(new TestClass());
        
        TestClass testClass = aspect1.getAspect();
        
        assertTrue("value".equals(testClass.doSomething("abc")));
        
        testClass.setSomething("abc");
        
        assertTrue("abc".equals(testClass.getSomething()));
    }
    
    @Test
    public void testAspect2()
    {
        TestAspect2 aspect2 = new TestAspect2();
        
        aspect2.init(new TestClass());
        
        TestClass testClass = aspect2.getAspect();
        
        testClass.setSomething("abc");
        
        assertTrue("abc".equals(testClass._value));
        
        assertTrue("nothing".equals(testClass.getSomething()));
    }
    
}