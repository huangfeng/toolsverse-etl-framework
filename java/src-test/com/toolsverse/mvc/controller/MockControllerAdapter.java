/*
 * MockControllerAdapter.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.controller;

import com.toolsverse.util.Utils;
import com.toolsverse.util.log.Logger;

/**
 * MockControllerAdapter
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockControllerAdapter extends ControllerAdapterImpl
{
    @Override
    public Object convertForDisplay(Controller controller,
            String attributeName, Object input)
    {
        Logger.log(Logger.INFO, this, "convertForDisplay:" + attributeName
                + ":" + Utils.makeString(input));
        
        return input;
    }
    
    @Override
    public Object convertForStorage(Controller controller,
            String attributeName, Object input)
    {
        Logger.log(Logger.INFO, this, "convertForStorage:" + attributeName
                + ":" + Utils.makeString(input));
        
        return input;
    }
    
    @Override
    public boolean viewAction(Controller controller, Object source,
            String attributeName)
    {
        return false;
    }
    
}
