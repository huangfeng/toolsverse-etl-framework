/*
 * Model.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.Serializable;
import java.util.Map;

import com.toolsverse.util.Mutable;

/**
 * This interface defines the methods required of objects filling the role of Model in the Toolsverse Model/View/Controller implementation.
 * Model supports sub models, so if attribute is not found in the model itself the MVC framework will continue looking in sub models.
 * 
 * @see com.toolsverse.mvc.controller.Controller
 * @see com.toolsverse.mvc.view.View
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public interface Model extends PropertyChangeListener, Cloneable, Mutable,
        Serializable
{
    
    /**
     * The Class Attribute. The default model implementation uses map of the Attributes to store name\value pairs.
     */
    public class Attribute implements Serializable
    {
        
        /** getter. */
        private String _getter;
        
        /** The setter. */
        private String _setter;
        
        /** The reader. */
        private String _reader;
        
        /** The writer. */
        private String _writer;
        
        /** The class. */
        private Class<?> _class;
        
        /** The value. */
        private Object _value;
        
        /** The params. */
        private Object _params;
        
        /** The persistable flag. */
        private boolean _persistable;
        
        /**
         * Instantiates a new Attribute.
         */
        public Attribute()
        {
            this(null, null, null, null);
        }
        
        /**
         * Instantiates a new Attribute.
         *
         * @param elementClass the element class
         * @param getter the name of the getter method
         * @param setter the name of the setter method
         */
        public Attribute(Class<?> elementClass, String getter, String setter)
        {
            this(elementClass, getter, setter, null);
        }
        
        /**
         * Instantiates a new Attribute.
         *
         * @param elementClass the element class
         * @param getter the name of the getter method
         * @param setter the name of the setter method
         * @param params the parameters
         */
        public Attribute(Class<?> elementClass, String getter, String setter,
                Object params)
        {
            this(elementClass, getter, setter, null, null, null);
        }
        
        /**
         * Instantiates a new Attribute.
         *
         * @param elementClass the element class
         * @param getter the name of the getter method
         * @param setter the name of the setter method
         * @param params the parameters
         * @param reader the name of the reader method. Reader method is used to read attribute from the storage (xml, database, etc)
         * @param writer the name of the writer method. Writer method is used to save attribute to the storage (xml, database, etc)
         */
        public Attribute(Class<?> elementClass, String getter, String setter,
                Object params, String reader, String writer)
        {
            _class = elementClass;
            _getter = getter;
            _setter = setter;
            _params = params;
            _reader = reader;
            _writer = writer;
            _persistable = true;
        }
        
        /**
         * Gets the attribute class.
         *
         * @return the attribute class
         */
        public Class<?> getAttributeClass()
        {
            return _class;
        }
        
        /**
         * Gets the name of the getter method.
         *
         * @return the name of the getter method
         */
        public String getGetter()
        {
            return _getter;
        }
        
        /**
         * Gets the parameters.
         *
         * @return the parameters
         */
        public Object getParams()
        {
            return _params;
        }
        
        /**
         * Gets the name of the reader method. Reader method is used to read attribute from the storage (xml, database, etc)
         *
         * @return the name of the reader method
         */
        public String getReader()
        {
            return _reader;
        }
        
        /**
         * Gets the name of the setter method
         *
         * @return the name of the setter method
         */
        public String getSetter()
        {
            return _setter;
        }
        
        /**
         * Gets the value of the attribute.
         *
         * @return the value
         */
        public Object getValue()
        {
            return _value;
        }
        
        /**
         * Gets the name of the writer method. Writer method is used to save attribute to the storage (xml, database, etc)
         *
         * @return the name of the writer method
         */
        public String getWriter()
        {
            return _writer;
        }
        
        /**
         * Checks if attribute is persistable - cab be save to the storage such as xml, database, etc.
         *
         * @return true, if it is persistable
         */
        public boolean isPersistable()
        {
            return _persistable;
        }
        
        /**
         * Sets the attribute class.
         *
         * @param value the new attribute class
         */
        public void setAttributeClass(Class<?> value)
        {
            _class = value;
        }
        
        /**
         * Sets the name of the getter method.
         *
         * @param value the new name of the getter method
         */
        public void setGetter(String value)
        {
            _getter = value;
        }
        
        /**
         * Sets the parameters.
         *
         * @param value the new parameters
         */
        public void setParams(Object value)
        {
            _params = value;
        }
        
        /**
         * Sets the persistable flag.
         *
         * @param value the new persistable flag
         */
        public void setPersistable(boolean value)
        {
            _persistable = value;
        }
        
        /**
         * Sets the name of the reader method
         *
         * @param value the new name of the reader method
         */
        public void setReader(String value)
        {
            _reader = value;
        }
        
        /**
         * Sets the name of the setter method
         *
         * @param value the new name of the setter method
         */
        public void setSetter(String value)
        {
            _setter = value;
        }
        
        /**
         * Sets the value of the attribute.
         *
         * @param value the new value
         */
        public void setValue(Object value)
        {
            _value = value;
        }
        
        /**
         * Sets the name of the writer method.
         *
         * @param value the new name of the writer method
         */
        public void setWriter(String value)
        {
            _writer = value;
        }
        
    }
    
    /**
     * Returns the value of the attribute.
     *
     * @param attributeName the attribute name
     * @return the object
     */
    Object access(String attributeName);
    
    /**
     * Adds the property change listener.
     *
     * @param listener the listener
     */
    void addPropertyChangeListener(PropertyChangeListener listener);
    
    /**
     * Adds the sub model. If attribute is not found in the parent the MVC framework will continue looking in sub models.
     *
     * @param model the model
     */
    void addSubModel(Model model);
    
    /**
     * Checks if attribute exists in the model or sub-models.
     *
     * @param attributeName the attribute name
     * @return true, if successful
     */
    boolean attributeExists(String attributeName);
    
    /**
     * Clones model
     *
     * @return the object
     * @throws CloneNotSupportedException the clone not supported exception
     */
    Object clone()
        throws CloneNotSupportedException;
    
    /**
     * Copies model from another model.
     *
     * @param model the model to copy
     * @throws Exception in case of any error
     */
    void copyFrom(Model model)
        throws Exception;
    
    /**
     * Gets the map of all attributes, including sub-models. The keys are names and the values are instances of the Attribute class.
     *
     * @return the the map of all attributes, including sub-models
     */
    Map<String, Attribute> getAllAttributes();
    
    /**
     * Gets the map of attributes for this model only, excluding sub-models. The keys are names and the values are instances of the Attribute class.
     *
     * @return the map of attributes for this model only
     */
    Map<String, Attribute> getAttributes();
    
    /**
     * Gets the model by attribute name. 
     *
     * @param attributeName the attribute name
     * @return the model by attribute name
     */
    Model getModelByAttributeName(String attributeName);
    
    /**
     * Gets the owner model. I this model is a sub model returns parent model.
     *
     * @return the owner
     */
    Model getOwner();
    
    /**
     * Sets the value for the attribute.
     *
     * @param attributeName the attribute name
     * @param newValue the new value
     */
    void populate(String attributeName, Object newValue);
    
    /*
     * (non-Javadoc)
     * 
     * @see java.beans.PropertyChangeListener#propertyChange(java.beans.
     * PropertyChangeEvent)
     */
    void propertyChange(PropertyChangeEvent event);
    
    /**
     * Reads the attribute from the storage such as xml, database, etc
     *
     * @param attributeName the attribute name
     * @param value the value
     */
    void read(String attributeName, Object value);
    
    /**
     * Removes the property change listener.
     *
     * @param listener the listener
     */
    void removePropertyChangeListener(PropertyChangeListener listener);
    
    /**
     * Removes the sub model.
     *
     * @param model the model
     */
    void removeSubModel(Model model);
    
    /**
     * Sets the owner.
     *
     * @param value the new owner
     */
    void setOwner(Model value);
    
    /**
     * Writes the attribute to the storage such as xml, database, etc
     *
     * @param attributeName the attribute name
     * @return the object
     */
    Object write(String attributeName);
    
}