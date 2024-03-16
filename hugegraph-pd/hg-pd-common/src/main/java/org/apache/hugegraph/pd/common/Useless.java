package org.apache.hugegraph.pd.common;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * The "Useless" annotation indicates that the annotated object can be safely removed without
 * affecting existing functionality, including objects that are only referenced in tests.
 */
@Target({ElementType.FIELD, ElementType.METHOD, ElementType.CONSTRUCTOR, ElementType.TYPE})
@Retention(RetentionPolicy.SOURCE)
public @interface Useless {}
