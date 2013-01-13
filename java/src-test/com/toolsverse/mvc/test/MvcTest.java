/*
 * MvcTest.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.test;

import static org.junit.Assert.assertTrue;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.toolsverse.config.SystemConfig;
import com.toolsverse.mvc.controller.Controller;
import com.toolsverse.mvc.controller.ControllerImpl;
import com.toolsverse.mvc.controller.MockControllerAdapter;
import com.toolsverse.mvc.model.Level1Node;
import com.toolsverse.mvc.model.Level2Model;
import com.toolsverse.mvc.model.MasterModel;
import com.toolsverse.mvc.model.ProviderModel;
import com.toolsverse.mvc.model.TestData;
import com.toolsverse.mvc.pojo.PojoWrapper;
import com.toolsverse.mvc.view.MockTextComponent;
import com.toolsverse.mvc.view.MockViewImpl;
import com.toolsverse.mvc.view.View;
import com.toolsverse.resource.TestResource;
import com.toolsverse.util.TestAppender;
import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Log4jWriter;
import com.toolsverse.util.log.Logger;

/**
 * ModelTest
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MvcTest
{
    private static TestAppender _testAppender = new TestAppender();
    
    private static TestData _testData = null;
    private static View _view = null;
    private static Controller _controller = null;
    private static MockControllerAdapter _controllerAdapter = null;
    
    private static MockTextComponent _searchMockComponent = null;
    private static MockTextComponent _nodeName = null;
    private static MockTextComponent _level1Attr = null;
    private static MockTextComponent _level2Attr = null;
    
    @BeforeClass
    public static void setUp()
        throws Exception
    {
        System.setProperty(
                SystemConfig.HOME_PATH_PROPERTY,
                SystemConfig.WORKING_PATH
                        + TestResource.TEST_HOME_PATH.getValue());
        
        SystemConfig.instance().setSystemProperty(
                SystemConfig.DEPLOYMENT_PROPERTY, SystemConfig.TEST_DEPLOYMENT);
        
        Utils.callAnyMethod(SystemConfig.instance(), "init");
        
        ((Log4jWriter)Logger.getLogger()).getLogger(null).addAppender(
                _testAppender);
        
        Logger.getLogger().setLevel(null, Logger.INFO);
    }
    
    private void setUpMvc()
        throws Exception
    {
        _testData = new TestData();
        
        _view = new MockViewImpl();
        
        _controllerAdapter = new MockControllerAdapter();
        
        _searchMockComponent = new MockTextComponent();
        _searchMockComponent.setName(MasterModel.SEARCH_STR);
        
        _nodeName = new MockTextComponent();
        _nodeName.setName(Level1Node.NODE_NAME);
        
        _level1Attr = new MockTextComponent();
        _level1Attr.setName(Level1Node.LEVEL1_ATTR);
        
        _level2Attr = new MockTextComponent();
        _level2Attr.setName(Level2Model.LEVEL2_ATTR);
        
        _controllerAdapter.addComponent(_view, _searchMockComponent);
        _controllerAdapter.addComponent(_view, _nodeName);
        _controllerAdapter.addComponent(_view, _level1Attr);
        _controllerAdapter.addComponent(_view, _level2Attr);
        
        _controller = new ControllerImpl();
        
        _controller.setMasterModel(_testData.getModel());
        _controller.setMasterView(_view);
    }
    
    @AfterClass
    public static void tearDown()
    {
        ((Log4jWriter)Logger.getLogger()).getLogger(null).removeAppender(
                _testAppender);
    }
    
    @Test
    public void testChangeView()
        throws Exception
    {
        setUpMvc();
        
        _testAppender.clear();
        
        _testData.getModel().getRoot()
                .setCurrentNode(_testData.getLevel1Node1());
        
        _controller.init(_testData.getModel(), _controllerAdapter);
        
        _searchMockComponent.setText("new search");
        _nodeName.setText("new node name");
        _level1Attr.setText("new level1 attr");
        _level2Attr.setText("new level2 attr");
        
        assertTrue(_testData.getModel().getSearchStr()
                .equals(_searchMockComponent.getText()));
        assertTrue(_testData.getLevel1Node1().getName()
                .equals(_nodeName.getText()));
        assertTrue(_testData.getLevel1Node1().getLevel1Attr()
                .equals(_level1Attr.getText()));
        assertTrue(_testData.getLevel2Model().getLevel2Attr()
                .equals(_level2Attr.getText()));
    }
    
    @Test
    public void testInit()
        throws Exception
    {
        setUpMvc();
        
        _testAppender.clear();
        
        _testData.getModel().getRoot().setCurrentNode(null);
        
        _controller.init();
        
        assertTrue(_testData.getModel().getSearchStr()
                .equals(_searchMockComponent.getText()));
        assertTrue(Utils.isNothing(_nodeName.getText()));
    }
    
    @Test
    public void testModelProvider()
        throws Exception
    {
        setUpMvc();
        
        _testAppender.clear();
        
        View view = new MockViewImpl();
        
        MockControllerAdapter controllerAdapter = new MockControllerAdapter();
        
        MockTextComponent mockComponent = new MockTextComponent();
        mockComponent.setName(ProviderModel.SOME_ATTR);
        
        controllerAdapter.addComponent(view, mockComponent);
        
        PojoWrapper<ProviderModel> model = new PojoWrapper<ProviderModel>(
                ProviderModel.class);
        ProviderModel providerModel = model.getPojo();
        providerModel.setSomeAttr("init mvc");
        
        Controller controller = new ControllerImpl();
        
        controller.setMasterModel(model);
        controller.setMasterView(view);
        controller.init();
        
        // init test
        assertTrue(providerModel.getSomeAttr().equals(mockComponent.getText()));
        
        // model changed test
        providerModel.setSomeAttr("model changed");
        assertTrue(providerModel.getSomeAttr().equals(mockComponent.getText()));
        
        // view changed test
        mockComponent.setText("view changed");
        assertTrue(providerModel.getSomeAttr().equals(mockComponent.getText()));
    }
    
    @Test
    public void testSetCurrentModel()
        throws Exception
    {
        setUpMvc();
        
        _testAppender.clear();
        
        _testData.getModel().getRoot()
                .setCurrentNode(_testData.getLevel1Node1());
        
        _controller.init(_testData.getModel(), _controllerAdapter);
        
        assertTrue(_testData.getModel().getSearchStr()
                .equals(_searchMockComponent.getText()));
        
        assertTrue(_testData.getLevel1Node1().getLevel1Attr()
                .equals(_level1Attr.getText()));
        assertTrue(_testData.getLevel2Model().getLevel2Attr()
                .equals(_level2Attr.getText()));
        
        assertTrue(_testData.getLevel1Node1().getName()
                .equals(_nodeName.getText()));
    }
    
}