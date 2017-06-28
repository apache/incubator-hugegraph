package com.baidu.hugegraph.config;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.baidu.hugegraph.util.E;
import com.google.common.base.Joiner;
import com.google.common.base.Preconditions;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableSet;

/**
 * Created by liningrui on 2017/3/23.
 */
public class ConfigOption<T> {

    private static final Logger logger = LoggerFactory.getLogger(ConfigOption.class);

    private static final Set<Class<?>> ACCEPTED_DATATYPES;
    private static final String ACCEPTED_DATATYPES_STRING;

    static {
        ACCEPTED_DATATYPES = ImmutableSet.of(
                Boolean.class,
                Short.class,
                Integer.class,
                Byte.class,
                Long.class,
                Float.class,
                Double.class,
                String.class,
                String[].class
        );

        ACCEPTED_DATATYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATATYPES);
    }

    private final String name;
    private final String desc;
    private final Boolean rewritable;
    private final Class<T> dataType;
    private T value;
    private final Predicate<T> checkFunc;

    public ConfigOption(String name, T value, Boolean rewritable, String desc,
                        Predicate<T> verifyFunc) {
        this(name, (Class<T>) value.getClass(), value, rewritable, desc, verifyFunc);
    }

    public ConfigOption(String name, Class<T> dataType, T value, Boolean
            rewritable, String desc, Predicate<T> checkFunc) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(rewritable);

        if (!ACCEPTED_DATATYPES.contains(dataType)) {
            String msg = String.format("Input datatype: '%s' doesn't belong "
                            + "to acceptable type set: [%s]",
                    dataType, ACCEPTED_DATATYPES_STRING);
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.name = name;
        this.dataType = dataType;
        this.value = value;
        this.rewritable = rewritable;
        this.desc = desc;
        this.checkFunc = checkFunc;

        if (this.checkFunc != null) {
            check(this.value);
        }
    }

    public String name() {
        return this.name;
    }

    public Class<T> dataType() {
        return this.dataType;
    }

    public String desc() {
        return this.desc;
    }

    public T value() {
        return this.value;
    }

    public void value(T value) {
        check(value);
        E.checkArgument(this.rewritable, "Not allowed to modify option: '%s' "
                + "which is unrewritable", this.name);
        this.value = value;
    }

    public void check(Object value) {
        E.checkNotNull(value, "value", this.name);
        E.checkArgument(this.dataType.isInstance(value),
                "Invalid class for option '%s'. Expected '%s' but given '%s'",
                this.name, this.dataType, value.getClass());
        T result = (T) value;
        E.checkArgument(this.checkFunc.apply(result),
                "Invalid option value for [%s]: %s", this.name, value);
    }

}
