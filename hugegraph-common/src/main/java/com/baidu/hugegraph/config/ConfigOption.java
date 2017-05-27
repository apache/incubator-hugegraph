package com.baidu.hugegraph.config;

import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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

        //        StandardSerializer ss = new StandardSerializer();
        //        for (Class<?> c : ACCEPTED_DATATYPES) {
        //            if (!ss.validDataType(c)) {
        //                String msg = String.format("%s datatype %s is not accepted by %s",
        //                        ConfigOption.class.getSimpleName(), c, StandardSerializer.class.getSimpleName());
        //                log.error(msg);
        //                throw new IllegalStateException(msg);
        //            }
        //        }

        ACCEPTED_DATATYPES_STRING = Joiner.on(", ").join(ACCEPTED_DATATYPES);
    }

    private final String name;
    private final String desc;
    private final Boolean rewritable;
    private final Class<T> dataType;
    private T value;
    private final Predicate<T> verifyFunc;

    public ConfigOption(String name, T value, Boolean rewritable, String desc, Predicate<T> verifyFunc) {
        this(name, (Class<T>) value.getClass(), value, rewritable, desc, verifyFunc);
    }

    public ConfigOption(String name, Class<T> dataType, T value, Boolean rewritable, String desc,
                        Predicate<T> verifyFunc) {
        Preconditions.checkNotNull(name);
        Preconditions.checkNotNull(dataType);
        Preconditions.checkNotNull(rewritable);

        if (!ACCEPTED_DATATYPES.contains(dataType)) {
            String msg = String.format("Datatype %s is not one of %s", dataType, ACCEPTED_DATATYPES_STRING);
            logger.error(msg);
            throw new IllegalArgumentException(msg);
        }

        this.name = name;
        this.dataType = dataType;
        this.value = value;
        this.rewritable = rewritable;
        this.desc = desc;
        this.verifyFunc = verifyFunc;

        if (this.verifyFunc != null) {
            verify(this.value);
        }
    }

    public String name() {
        return name;
    }

    public Class<T> dataType() {
        return dataType;
    }

    /**
     * @return
     */
    public T value() {
        return value;
    }

    /**
     * @param value
     */
    public void value(T value) {
        verify(value);
        Preconditions.checkArgument(rewritable);
        this.value = value;
    }

    public T verify(Object input) {
        Preconditions.checkNotNull(input);
        Preconditions.checkArgument(dataType.isInstance(input),
                "Invalid class for configuration value [%s]. Expected [%s] but given [%s]", this.toString(), dataType,
                input.getClass());
        T result = (T) input;
        Preconditions.checkArgument(verifyFunc.apply(result), "Invalid configuration value for [%s]: %s",
                this.toString(), input);
        return result;
    }

    //    public static final<E extends Enum> E getEnumValue(String str, Class<E> enumClass) {
    //        str = str.trim();
    //        if (StringUtils.isBlank(str)) return null;
    //        for (E e : enumClass.getEnumConstants()) {
    //            if (e.toString().equalsIgnoreCase(str)) return e;
    //        }
    //        throw new IllegalArgumentException("Invalid enum string provided for ["+enumClass+"]: " + str);
    //    }

}
