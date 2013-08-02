package com.ebuddy.cassandra.property;

/**
 *  Utilities for working with PropertyValues.
 *  
 * @author Eric Zoerner <a href="mailto:ezoerner@ebuddy.com">ezoerner@ebuddy.com</a>
 */
@SuppressWarnings({"UtilityClass", "UnusedDeclaration"})
@Deprecated
public class PropertyValueUtils {

    private PropertyValueUtils() {
    }

    /**
     * Return if the property value is null or is empty.
     * @param property the property value
     * @return true if null or empty
     */
	public static boolean isEmpty( PropertyValue<?> property ) {
		return property == null || property.isEmpty();
	}

    /**
     * Return the value of the PropertyValue parameter or
     * null if the parameter is null.
     */
    public static <T> T getValueOrNull(PropertyValue<T> propertyValue) {
        return propertyValue == null ? null : propertyValue.getValue();
    }
}
