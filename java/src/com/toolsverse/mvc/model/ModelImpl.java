/*
 * ModelImpl.java
 * 
 * Copyright 2010-2012 Toolsverse. All rights reserved. Toolsverse
 * PROPRIETARY/CONFIDENTIAL. Use is subject to license terms.
 */

package com.toolsverse.mvc.model;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.beans.PropertyChangeSupport;
import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.commons.beanutils.BeanUtils;

import com.toolsverse.resource.Resource;
import com.toolsverse.util.ClassUtils;
import com.toolsverse.util.Null;
import com.toolsverse.util.TypedKeyValue;
import com.toolsverse.util.Utils;
import com.toolsverse.util.factory.ObjectFactory;
import com.toolsverse.util.log.Logger;

/**
 * Abstract implementation of the Model interface.
 * 
 * @author Maksym Sherbinin
 * @version 2.0
 * @since 2.0
 */

public abstract class ModelImpl implements Model
{
    
    /** The attributes. */
    protected Map<String, Attribute> _attributes;
    
    /** The property change support. */
    private PropertyChangeSupport _propertyChangeSupport;
    
    /** The owner. */
    private Model _owner = null;
    
    /** The sub models. */
    private List<Model> _subModels;
    
    /** The name suffix. */
    private String _suffix;
    
    /** The exclude flag. */
    private boolean _exclude;
    
    /** The replacement map. If name is found in the keySet it will be substituted on the value */
    private Map<String, String> _replacementMap;
    
    /** The silent update flag. */
    private boolean _silentUpdate;
    
    /** The is dirty attrubute. */
    private boolean _isDirty;
    
    /** The update dirty on change flag. */
    private boolean _updateDirtyOnChange;
    
    /**
     * Instantiates a new ModelImpl. The constructor reads the annotations and sets the values for the getter\setter\reader\writer\params attributes.
     */
    public ModelImpl()
    {
        _updateDirtyOnChange = false;
        
        _silentUpdate = false;
        
        _isDirty = false;
        
        _suffix = null;
        
        _attributes = new LinkedHashMap<String, Attribute>();
        
        _propertyChangeSupport = new PropertyChangeSupport(this);
        
        _subModels = new ArrayList<Model>();
        
        Method[] methods = getClass().getMethods();
        
        List<TypedKeyValue<Method, Annotation>> readers = new ArrayList<TypedKeyValue<Method, Annotation>>();
        List<TypedKeyValue<Method, Annotation>> writers = new ArrayList<TypedKeyValue<Method, Annotation>>();
        
        for (Method method : methods)
        {
            Annotation[] annotations = method.getDeclaredAnnotations();
            if (annotations != null)
                for (Annotation annotation : annotations)
                {
                    if (annotation instanceof Getter && isGetterMethod(method))
                    {
                        Attribute attr = getAttribute(((Getter)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Getter)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getReturnType(),
                                    method.getName(), null, attrParams);
                            
                            _attributes.put(((Getter)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setGetter(method.getName());
                            attr.setAttributeClass(method.getReturnType());
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    else if (annotation instanceof Setter
                            && isSetterMethod(method))
                    {
                        Attribute attr = getAttribute(((Setter)annotation)
                                .name());
                        
                        Object attrParams = getAttrParams(((Setter)annotation)
                                .paramsClass());
                        
                        if (attr == null)
                        {
                            attr = new Attribute(method.getParameterTypes()[0],
                                    null, method.getName(), attrParams);
                            
                            _attributes.put(((Setter)annotation).name(), attr);
                        }
                        else
                        {
                            attr.setSetter(method.getName());
                            
                            if (attr.getAttributeClass() == null)
                                attr.setAttributeClass(method
                                        .getParameterTypes()[0]);
                            if (attr.getParams() == null && attrParams != null)
                                attr.setParams(attrParams);
                        }
                    }
                    else if (annotation instanceof Reader
                            && isReaderMethod(method))
                    {
                        readers.add(new TypedKeyValue<Method, Annotation>(
                                method, annotation));
                    }
                    else if (annotation instanceof Writer
                            && isWriterMethod(method))
                    {
                        writers.add(new TypedKeyValue<Method, Annotation>(
                                method, annotation));
                    }
                }
        }
        
        for (TypedKeyValue<Method, Annotation> keyValue : readers)
        {
            Attribute attr = getAttribute(((Reader)keyValue.getValue()).name());
            
            Object attrParams = getAttrParams(((Reader)keyValue.getValue())
                    .paramsClass());
            
            if (attr == null)
            {
                attr = new Attribute(keyValue.getKey().getParameterTypes()[0],
                        null, null, attrParams, keyValue.getKey().getName(),
                        null);
                
                _attributes.put(((Reader)keyValue.getValue()).name(), attr);
            }
            else
            {
                attr.setReader(keyValue.getKey().getName());
                
                if (attr.getAttributeClass() == null)
                    attr.setAttributeClass(keyValue.getKey()
                            .getParameterTypes()[0]);
                if (attr.getParams() == null && attrParams != null)
                    attr.setParams(attrParams);
            }
            
        }
        
        for (TypedKeyValue<Method, Annotation> keyValue : writers)
        {
            Attribute attr = getAttribute(((Writer)keyValue.getValue()).name());
            
            Object attrParams = getAttrParams(((Writer)keyValue.getValue())
                    .paramsClass());
            
            if (attr == null)
            {
                attr = new Attribute(keyValue.getKey().getReturnType(),
                        keyValue.getKey().getName(), null, attrParams);
                
                _attributes.put(((Writer)keyValue.getValue()).name(), attr);
            }
            else
            {
                attr.setWriter(keyValue.getKey().getName());
                
                if (attr.getAttributeClass() == null)
                    attr.setAttributeClass(keyValue.getKey()
                            .getParameterTypes()[0]);
                if (attr.getParams() == null && attrParams != null)
                    attr.setParams(attrParams);
            }
            
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#access(java.lang.String)
     */
    public Object access(String attributeName)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return Null.NULL;
            else
                return model.access(attributeName);
        }
        
        if (Utils.isNothing(attr.getGetter()))
            return Null.NULL;
        
        Method invokeMethod = null;
        
        try
        {
            invokeMethod = getClass()
                    .getMethod(attr.getGetter(), (Class[])null);
            
            return invokeMethod.invoke(this, (Object[])null);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, getClass(), "access: "
                    + Resource.ERROR_ACCESS_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
        
    }
    
    /**
     * Creates an Attribute and adds it to the model.
     *
     * @param attributeName the attribute name
     * @param elementClass the element class
     * @param getter the name of the getter method
     * @param setter the name of the setter method
     */
    public void addAttribute(String attributeName, Class<?> elementClass,
            String getter, String setter)
    {
        addAttribute(attributeName, elementClass, getter, setter, null);
    }
    
    /**
     * Creates an Attribute and adds it to the model.
     *
     * @param attributeName the attribute name
     * @param elementClass the element class
     * @param getter the name of the getter method
     * @param setter the name of the setter method
     * @param params the parameters of the attribute
     */
    public void addAttribute(String attributeName, Class<?> elementClass,
            String getter, String setter, Object params)
    {
        addAttribute(attributeName, elementClass, getter, setter, params, null,
                null);
    }
    
    /**
     * Creates an Attribute and adds it to the model.
     *
     * @param attributeName the attribute name
     * @param elementClass the element class
     * @param getter the name of the getter method
     * @param setter the name of the setter method
     * @param params the parameters of the attribute
     * @param reader the name of the reader method
     * @param writer the name of the writer method
     */
    public void addAttribute(String attributeName, Class<?> elementClass,
            String getter, String setter, Object params, String reader,
            String writer)
    {
        _attributes.put(attributeName, new Attribute(elementClass, getter,
                setter, params, reader, writer));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#addPropertyChangeListener(java.beans.
     * PropertyChangeListener)
     */
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        _propertyChangeSupport.addPropertyChangeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#addSubModel(com.toolsverse.mvc.model.Model
     * )
     */
    public void addSubModel(Model model)
    {
        if (!_subModels.contains(model))
        {
            model.setOwner(this);
            
            _subModels.add(model);
        }
    }
    
    /**
     * Executed when attribute value has changed.
     *
     * @param attributeName the attribute name
     * @param newValue the new value
     */
    public void attributeChanged(String attributeName, Object newValue)
    {
        propertyChange(new PropertyChangeEvent(this, attributeName, null,
                newValue));
    }
    
    public boolean attributeExists(String attributeName)
    {
        return _attributes.containsKey(getRealName(attributeName));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#baseline()
     */
    public void baseline()
    {
        setDirty(false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see java.lang.Object#clone()
     */
    @Override
    public Object clone()
        throws CloneNotSupportedException
    {
        try
        {
            return ClassUtils.clone(this);
        }
        catch (Exception ex)
        {
            throw new CloneNotSupportedException(
                    Utils.getStackTraceAsString(ex));
        }
        
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#copyFrom(com.toolsverse.mvc.model.Model)
     */
    public void copyFrom(Model model)
        throws Exception
    {
        BeanUtils.copyProperties(this, model);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#getAllAttributes()
     */
    public Map<String, Attribute> getAllAttributes()
    {
        Map<String, Attribute> attrs = getAttributes();
        
        if (_subModels.size() == 0)
            return attrs;
        
        for (Model model : _subModels)
        {
            Map<String, Model.Attribute> subAttrs = model.getAllAttributes();
            
            if (subAttrs != null && subAttrs.size() > 0)
                attrs.putAll(subAttrs);
        }
        
        return attrs;
    }
    
    /**
     * Gets the Attribute by name
     *
     * @param attributeName the attribute name
     * @return the attribute
     */
    public Attribute getAttribute(String attributeName)
    {
        Attribute attr = _attributes.get(attributeName);
        
        return attr;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#getAttributes()
     */
    public Map<String, Attribute> getAttributes()
    {
        Set<String> keys = _attributes.keySet();
        
        Map<String, Attribute> attrs = new LinkedHashMap<String, Attribute>();
        
        if (keys != null && keys.size() > 0)
        {
            Map<String, Attribute> innerAttrs = new LinkedHashMap<String, Attribute>();
            
            for (String key : _attributes.keySet())
            {
                attrs.put(key, _attributes.get(key));
                
                Attribute attr = _attributes.get(key);
                
                if (attr.getValue() instanceof Model)
                    innerAttrs.putAll(((Model)attr.getValue()).getAttributes());
            }
            
            if (innerAttrs.size() > 0)
                attrs.putAll(innerAttrs);
        }
        
        return attrs;
    }
    
    /**
     * Gets the attribute value by attributeName
     *
     * @param attributeName the attribute name
     * @return the attribute value
     */
    public Object getAttributeValue(String attributeName)
    {
        Attribute attribute = getAttribute(getRealName(attributeName));
        
        if (attribute == null)
            return null;
        
        return attribute.getValue();
    }
    
    /**
     * Gets the name of the attribute using substitution map. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param name the original name
     * @param suffix the suffix
     * @param map the substitution map
     * @param exclude if == true the new name is map.containsKey(name) ? name : name + suffix otherwise map.containsKey(name) ? name + suffix : name
     * @return the name of the attribute 
     */
    private String getAttrName(String name, String suffix,
            Map<String, String> map, boolean exclude)
    {
        if (map == null)
            return name + suffix;
        
        if (exclude)
            return map.containsKey(name) ? name : name + suffix;
        else
            return map.containsKey(name) ? name + suffix : name;
    }
    
    /**
     * Creates the attribute parameter object.
     *
     * @param paramsClass the parameter class
     * @return the the attribute parameter object
     */
    protected Object getAttrParams(Class<?> paramsClass)
    {
        if (paramsClass == null || paramsClass.isAssignableFrom(Null.class))
            return null;
        
        return ObjectFactory.instance().get(paramsClass.getName(), null,
                paramsClass, false);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#getModelByAttributeName(java.lang.String)
     */
    public Model getModelByAttributeName(String attributeName)
    {
        if (_subModels.size() == 0)
            return null;
        
        for (Model model : _subModels)
        {
            Map<String, Model.Attribute> attrs = model.getAttributes();
            
            if (attrs.containsKey(attributeName))
                return model;
            else
            {
                Model ret = model.getModelByAttributeName(attributeName);
                
                if (ret != null)
                    return ret;
            }
        }
        
        return null;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#getOwner()
     */
    public Model getOwner()
    {
        return _owner;
    }
    
    /**
     * Gets the name of the attribute using replacement map and exclude property.
     * 
     * <p>
     * <p><pre class="brush: java">
     *  return _replacementMap == null
     *           || (_exclude && !_replacementMap.containsKey(attributeName))
     *           || (!_exclude && _replacementMap.containsKey(attributeName)) ? attributeName
     *           + _suffix
     *           : attributeName;
     * </pre>
     * 
     * @param attributeName the attribute name
     * @return the real name
     */
    public String getRealName(String attributeName)
    {
        if (Utils.isNothing(_suffix))
            return attributeName;
        
        if (attributeName.lastIndexOf(_suffix) > 0)
            return attributeName;
        
        return _replacementMap == null
                || (_exclude && !_replacementMap.containsKey(attributeName))
                || (!_exclude && _replacementMap.containsKey(attributeName)) ? attributeName
                + _suffix
                : attributeName;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#isDirty()
     */
    public boolean isDirty()
    {
        return _isDirty;
    }
    
    /**
     * Checks if it is a getter method.
     *
     * @param method the method
     * @return true, if it is a getter method
     */
    protected boolean isGetterMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return (paramsTypes == null || paramsTypes.length == 0)
                && retType != null && !retType.toString().equals("void");
    }
    
    /**
     * Checks if it is a reader method.
     *
     * @param method the method
     * @return true, if it is a reader method
     */
    protected boolean isReaderMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return paramsTypes != null && paramsTypes.length == 1
                && retType != null && !retType.toString().equals("void");
    }
    
    /**
     * Checks if it is a setter method.
     *
     * @param method the method
     * @return true, if it is a setter method
     */
    protected boolean isSetterMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return paramsTypes != null && paramsTypes.length == 1
                && retType != null && retType.toString().equals("void");
    }
    
    /**
     * Checks if "silent update" flag is set. When it is true the view will not be notified when model is changing.
     *
     * @return true, if is silent update
     */
    public boolean isSilentUpdate()
    {
        return _silentUpdate;
    }
    
    /**
     * Checks if "is update dirty on change" flag is set. When it is true any update of the model will change isDirty attribute o the true. 
     * To set it back to false baseline() method must be called. 
     *
     * @return true, if is update dirty on change
     */
    public boolean isUpdateDirtyOnChange()
    {
        return _updateDirtyOnChange;
    }
    
    /**
     * Checks if it is a writer method.
     *
     * @param method the method
     * @return true, if it is a writer method
     */
    protected boolean isWriterMethod(Method method)
    {
        Class<?>[] paramsTypes = method.getParameterTypes();
        Class<?> retType = method.getReturnType();
        
        return (paramsTypes == null || paramsTypes.length == 0)
                && retType != null && !retType.toString().equals("void");
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#populate(java.lang.String,
     * java.lang.Object)
     */
    public void populate(String attributeName, Object newValue)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return;
            else
            {
                model.populate(attributeName, newValue);
                
                return;
            }
        }
        
        if (Utils.isNothing(attr.getSetter()) || Null.NULL.equals(newValue))
            return;
        
        Method invokeMethod = null;
        
        Class<?>[] parametertypes = new Class[] {attr.getAttributeClass()};
        
        try
        {
            invokeMethod = getClass().getMethod(attr.getSetter(),
                    parametertypes);
            
            invokeMethod.invoke(this, new Object[] {newValue});
            
            if (_updateDirtyOnChange)
                setDirty(true);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, getClass(), "populate: "
                    + Resource.ERROR_POPULATE_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#propertyChange(java.beans.PropertyChangeEvent
     * )
     */
    public void propertyChange(PropertyChangeEvent event)
    {
        if (_silentUpdate)
            return;
        
        PropertyChangeListener[] listeners = _propertyChangeSupport
                .getPropertyChangeListeners();
        
        if (listeners == null || listeners.length == 0)
        {
            if (getOwner() != null)
                getOwner().propertyChange(event);
            
            return;
        }
        
        for (PropertyChangeListener listener : listeners)
            listener.propertyChange(event);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#read(java.lang.String,
     * java.lang.Object)
     */
    public void read(String attributeName, Object value)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return;
            else
            {
                model.read(attributeName, value);
                
                return;
            }
        }
        
        if (!attr.isPersistable())
            return;
        
        if (Utils.isNothing(attr.getReader()))
        {
            populate(attributeName, value);
            
            return;
        }
        
        Method invokeMethod = null;
        
        Class<?>[] parametertypes = new Class[] {Object.class};
        
        try
        {
            invokeMethod = getClass().getMethod(attr.getReader(),
                    parametertypes);
            
            value = invokeMethod.invoke(this, new Object[] {value});
            
            if (Null.NULL != value)
                populate(attributeName, value);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, getClass(), "read: "
                    + Resource.ERROR_DESERIALIZE_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#removePropertyChangeListener(java.beans
     * .PropertyChangeListener)
     */
    public void removePropertyChangeListener(PropertyChangeListener listener)
    {
        _propertyChangeSupport.removePropertyChangeListener(listener);
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#removeSubModel(com.toolsverse.mvc.model
     * .Model)
     */
    public void removeSubModel(Model model)
    {
        if (model != null)
        {
            model.setOwner(null);
            
            _subModels.remove(model);
        }
    }
    
    /**
     * Replaces attribute names using model's substitution map and a suffix. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param suffix the suffix
     */
    public void replaceAttrNames(String suffix)
    {
        replaceAttrNames(suffix, null, false);
    }
    
    /**
     * Replaces attribute names using given substitution map, suffix and a exclude flag. Used when model is cloned or copied but it is not possible to reuse original names of the 
     * attributes. For example in the web mode where each UI element in the html must have a unique name. 
     *
     * @param suffix the suffix
     * @param map the substitution map
     * @param exclude the exclude flag
     */
    public void replaceAttrNames(String suffix, Map<String, String> map,
            boolean exclude)
    {
        Map<String, Attribute> attributes = new LinkedHashMap<String, Attribute>();
        
        _replacementMap = map;
        _exclude = exclude;
        
        for (String name : _attributes.keySet())
        {
            attributes.put(getAttrName(name, suffix, map, exclude),
                    _attributes.get(name));
        }
        
        _attributes = attributes;
        
        _suffix = suffix;
    }
    
    /**
     * Sets the new attribute value. Notifies view about the change. 
     *
     * @param attributeName the attribute name
     * @param newValue the new value
     */
    public void setAttributeValue(String attributeName, Object newValue)
    {
        Attribute attribute = getAttribute(getRealName(attributeName));
        
        if (attribute == null)
            return;
        
        Object oldValue = attribute.getValue();
        
        if (Utils.equals(oldValue, newValue))
            return;
        
        if (oldValue instanceof Model)
        {
            removeSubModel((Model)oldValue);
        }
        
        if (newValue instanceof Model)
        {
            addSubModel((Model)newValue);
        }
        
        attribute.setValue(newValue);
        
        propertyChange(new PropertyChangeEvent(this, attributeName, oldValue,
                newValue));
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.util.Mutable#setDirty(boolean)
     */
    public void setDirty(boolean value)
    {
        _isDirty = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see
     * com.toolsverse.mvc.model.Model#setOwner(com.toolsverse.mvc.model.Model)
     */
    public void setOwner(Model value)
    {
        _owner = value;
    }
    
    /**
     * Sets the "silent update" flag. When it is true the view will not be notified when model is changing.
     *
     * @param value the new value for "silent update" flag
     */
    public void setSilentUpdate(boolean value)
    {
        _silentUpdate = value;
    }
    
    /**
     * Sets the "update dirty on change" flag. When it is true any update of the model will change isDirty attribute o the true. 
     *
     * @param value the new value for the "update dirty on change" flag
     */
    public void setUpdateDirtyOnChange(boolean value)
    {
        _updateDirtyOnChange = value;
    }
    
    /*
     * (non-Javadoc)
     * 
     * @see com.toolsverse.mvc.model.Model#write(java.lang.String)
     */
    public Object write(String attributeName)
    {
        Attribute attr = getAttribute(attributeName);
        
        if (attr == null)
        {
            Model model = getModelByAttributeName(attributeName);
            
            if (model == null)
                return Null.NULL;
            else
                return model.write(attributeName);
        }
        
        if (!attr.isPersistable())
            return Null.NULL;
        
        if (Utils.isNothing(attr.getWriter()))
            return access(attributeName);
        
        Method invokeMethod = null;
        
        try
        {
            invokeMethod = getClass()
                    .getMethod(attr.getWriter(), (Class[])null);
            
            return invokeMethod.invoke(this, (Object[])null);
        }
        catch (Exception ex)
        {
            Logger.log(Logger.FATAL, getClass(), "access: "
                    + Resource.ERROR_ACCESS_MODEL.getValue(), ex);
            
            throw new IllegalArgumentException(ex);
        }
        
    }
    
}