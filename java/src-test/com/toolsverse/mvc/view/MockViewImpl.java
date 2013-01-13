/*
 * MockViewImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse 
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.view;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

/**
 * MockViewImpl
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 1.0
 */

public class MockViewImpl extends ViewImpl
{
    public class MockBooleanComponentAdapter extends MockComponentAdapter
            implements ActionListener
    {
        public MockBooleanComponentAdapter(View view,
                MockBooleanComponent component)
        {
            super(view, component);
            
            component.addActionListener(this);
        }
        
        public void actionPerformed(ActionEvent event)
        {
            valueChanging(Boolean
                    .valueOf(((MockBooleanComponent)getComponent())
                            .isSelected()));
        }
        
        @Override
        public void dettach()
        {
            ((MockBooleanComponent)getComponent()).removeActionListener(this);
            
            super.dettach();
        }
        
        @Override
        public void display(Object newValue)
        {
            ((MockBooleanComponent)getComponent())
                    .setSelected((Boolean)newValue);
            
            invalidate();
        }
    }
    
    public class MockTextComponentAdapter extends MockComponentAdapter
            implements DocumentListener
    {
        private boolean changing = false;
        
        public MockTextComponentAdapter(View view, MockTextComponent component)
        {
            super(view, component);
            
            ((MockTextComponent)getComponent()).getDocument()
                    .addDocumentListener(this);
        }
        
        public void actionPerformed(ActionEvent e)
        {
            sendChange();
        }
        
        public void changedUpdate(DocumentEvent e)
        {
            sendChange();
        }
        
        @Override
        public void dettach()
        {
            ((MockTextComponent)getComponent()).getDocument()
                    .removeDocumentListener(this);
            
            super.dettach();
        }
        
        @Override
        public void display(Object newValue)
        {
            if (changing)
                return;
            
            if (newValue != null)
                ((MockTextComponent)getComponent())
                        .setText(newValue.toString());
            else
                ((MockTextComponent)getComponent()).setText(null);
            
            invalidate();
        }
        
        public void insertUpdate(DocumentEvent e)
        {
            sendChange();
        }
        
        public void removeUpdate(DocumentEvent e)
        {
            sendChange();
        }
        
        private void sendChange()
        {
            changing = true;
            
            valueChanging(((MockTextComponent)getComponent()).getText());
            actionOccured();
            
            changing = false;
        }
        
    }
    
    public String addComponent(Object component, long behavior)
    {
        MockComponent comp = (MockComponent)component;
        
        if (comp == null || comp.getName() == null)
            return null;
        
        MockComponentAdapter adapter = null;
        
        if (comp instanceof MockTextComponent)
            adapter = new MockTextComponentAdapter(this,
                    (MockTextComponent)comp);
        else if (comp instanceof MockBooleanComponent)
            adapter = new MockBooleanComponentAdapter(this,
                    (MockBooleanComponent)comp);
        
        else
            return null;
        
        addAdapter(adapter);
        
        return comp.getName();
    }
    
    public String removeComponent(Object component)
    {
        if (component == null || ((MockComponent)component).getName() == null)
            return null;
        
        MockComponentAdapter adapter = (MockComponentAdapter)getComponents()
                .get(((MockComponent)component).getName());
        
        if (adapter == null)
            return null;
        
        adapter.dettach();
        
        getComponents().remove(((MockComponent)component).getName());
        
        return ((MockComponent)component).getName();
    }
    
}
