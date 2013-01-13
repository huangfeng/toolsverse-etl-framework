/*
 * TestData.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.util.ArrayList;
import java.util.List;

/**
 * TestData
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class TestData
{
    public static final String SEARCH_STR = "search";
    
    public static final String LEVEL1_NAME1 = "level1name1";
    public static final String LEVEL1_NAME2 = "level1name2";
    
    public static final String LEVEL1_ATTR1 = "level1attr1";
    public static final String LEVEL1_ATTR2 = "level1attr2";
    
    public static final String LEVEL2_ATTR = "level2attr";
    
    private MasterModel _model = null;
    
    public TestData()
    {
        _model = new MasterModel();
        
        _model.setSearchStr(SEARCH_STR);
        
        Root root = new Root();
        
        Level1Node level1Node1 = new Level1Node();
        level1Node1.setParent(root);
        level1Node1.setName(LEVEL1_NAME1);
        level1Node1.setLevel1Attr(LEVEL1_ATTR1);
        
        Level1Node level1Node2 = new Level1Node();
        level1Node2.setParent(root);
        level1Node2.setName(LEVEL1_NAME2);
        level1Node2.setLevel1Attr(LEVEL1_ATTR2);
        
        Level2Model level2Model = new Level2Model();
        level2Model.setLevel2Attr(LEVEL2_ATTR);
        
        level1Node1.setLevel2Node(level2Model);
        
        List<Node> level1Nodes = new ArrayList<Node>();
        
        level1Nodes.add(level1Node1);
        level1Nodes.add(level1Node2);
        
        root.setChildren(level1Nodes);
        
        _model.setRoot(root);
    }
    
    public Level1Node getLevel1Node1()
    {
        return (Level1Node)_model.getRoot().getChildren().get(0);
    }
    
    public Level1Node getLevel1Node2()
    {
        return (Level1Node)_model.getRoot().getChildren().get(1);
    }
    
    public Level2Model getLevel2Model()
    {
        return getLevel1Node1().getLevel2Model();
    }
    
    public MasterModel getModel()
    {
        return _model;
    }
    
    public Root getRoot()
    {
        return _model.getRoot();
    }
    
}
