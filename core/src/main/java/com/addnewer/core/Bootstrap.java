package com.addnewer.core;


import com.addnewer.api.*;
import com.addnewer.config.ConfigurationUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.*;
import java.util.*;

/**
 * @author pinru
 * @version 1.0
 */
public class Bootstrap {
    private static final Logger logger = LoggerFactory.getLogger(Bootstrap.class);


    private static final Key<ParameterTool> ARGS = Key.of(ParameterTool.class, "args");
    private static final Key<ParameterTool> SYSTEM = Key.of(ParameterTool.class, "system");
    private static final Key<ParameterTool> FILE = Key.of(ParameterTool.class, "file");


    private final GraphContext context = new GraphContext();


    public Bootstrap(String[] args) {
        initArgs(args);
    }

    private void initArgs(final String[] args) {
        context.injectBean(ARGS, container -> args);
        context.injectBean(SYSTEM, container -> ParameterTool.fromSystemProperties());
        context.injectBean(FILE, container -> ConfigurationUtil.loadConfigurations(args));
    }


    public void run(List<Class<?>> components, List<Class<?>> defaultComponents) throws Exception {
        logger.info("init application.");
        logger.info("load  application components: {} .", components);
        final List<Class<? extends FlinkJob>> jobs = new ArrayList<>();
        for (Class<?> clazz : components) {
            injectComponent(clazz);
            if (FlinkJob.class.isAssignableFrom(clazz)) {
                context.addEntryPoint(clazz);
                jobs.add((Class<? extends FlinkJob>) clazz);
            }
        }

        logger.info("load default components: {} .", defaultComponents);
        injectDefaultComponents(defaultComponents);

        context.generateContainer();

        logger.info("run flink jobs: {} .", jobs);
        StreamExecutionEnvironment env = context.getInstance(StreamExecutionEnvironment.class);
        if (jobs.isEmpty()) {
            throw new BootstrapException("No FlinkJob found.");
        }
        for (final Class<? extends FlinkJob> job : jobs) {
            FlinkJob flinkJob = context.getInstance(job);
            flinkJob.run();
        }
        env.execute();
    }


    private void injectionBeans(Class<?> configuration, boolean ignore) {

        Key<?> configKey = Key.of(configuration);
        Arrays
                .stream(configuration.getMethods())
                .filter(method -> method.getAnnotation(Bean.class) != null)
                .forEach(method -> {
                    final Class<?> bind = getBind(method);
                    final String name = getName(method);
                    final Key key = new Key(bind, name);
                    if (isKeyExist(key, ignore)) return;

                    final List<Key> paramKeys = getParamKeys(method);
                    final InjectFunction<?> injectFunction = container -> {
                        try {
                            Object config = container.getBean(configKey);
                            return method.invoke(config, paramKeys
                                    .stream()
                                    .map(container::getBean)
                                    .toArray());
                        } catch (InvocationTargetException | IllegalAccessException e) {
                            throw new BootstrapException("注入Bean: {} 失败.", key, e);
                        }

                    };
                    ArrayList<Key> keys = new ArrayList<>();
                    keys.addAll(paramKeys);
                    keys.add(configKey);
                    context.injectBean(key, injectFunction, keys);
                });
    }


    private void injectComponent(Class<?> clazz) {
        logger.debug("inject component: {}", clazz);

        injectComponent(clazz, false);
    }

    private void injectDefaultComponents(List<Class<?>> components) {
        logger.debug("inject default components: {}", components);
        for (final Class<?> component : components) {
            injectComponent(component, true);
        }
    }

    private void injectComponent(Class<?> clazz, boolean ignore) {
        final Class<?> bind = getBind(clazz);
        final String name = getName(clazz);
        final Key key = new Key(bind, name);
        // 如果key存在，就不用再继续了
        if (isKeyExist(key, ignore)) return;

        // 获得构造函数
        final Constructor<?>[] constructors = clazz.getConstructors();
        Constructor<?> constructor;
        if (constructors.length == 0) {
            throw new BootstrapException("{} 构造函数不可见,检查是否有public构造函数.", clazz.getName());
        }
        if (constructors.length == 1) {
            constructor = constructors[0];
        } else {
            final List<Constructor<?>> constructorWithInject = Arrays
                    .stream(constructors)
                    .filter(con -> con.getAnnotation(Inject.class) != null)
                    .toList();

            if (constructorWithInject.isEmpty()) {
                throw new BootstrapException("{} 构造函数不唯一,请用@{}指定要用的构造函数.", clazz.getName(), Inject.class.getName());
            }
            if (constructorWithInject.size() > 1) {
                throw new BootstrapException("{} 有多个@{} 指定的构造函数,只能指定一个.", Inject.class.getSimpleName());
            }
            constructor = constructorWithInject.get(0);
        }

        // 构造对象并注入
        final List<Key> paramKeys = getParamKeys(constructor);
        Map<Key<String>, Field> values = getValuesFromClass(clazz);
        final InjectFunction<?> injectFunction = container -> {
            try {
                Object c = constructor.newInstance(paramKeys
                        .stream()
                        .map(container::getBean)
                        .toArray());
                values.forEach((k, f) -> {
                    f.setAccessible(true);
                    try {
                        String value = container.getBean(k);
                        f.set(c, translate(f.getType(), value));
                    } catch (IllegalAccessException | RuntimeException e) {
                        throw new BootstrapException("注入@Value:{} 失败.", e, k);
                    }
                });
                return c;
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new BootstrapException("构造Component: {} 失败.", e, clazz.getName());
            }
        };
        if (clazz.isAnnotationPresent(Configuration.class)) {
            injectionBeans(clazz, ignore);
        }
        if (values.isEmpty()) {
            context.injectBean(key, injectFunction, paramKeys);
        } else {
            ArrayList<Key> keys = new ArrayList<>();
            keys.addAll(paramKeys);
            keys.addAll(values.keySet());
            context.injectBean(key, injectFunction, keys);
        }
    }


    private Map<Key<String>, Field> getValuesFromClass(Class<?> clazz) {
        Map<Key<String>, Field> values = new HashMap<>();
        Field[] fields = clazz.getDeclaredFields();
        for (final Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(Value.class)) {
                final Value value = field.getAnnotation(Value.class);
                values.put(Key.of(String.class, value.value()), field);
                injectValue(value);
            }
        }
        return values;
    }

    private static Object translate(Class<?> clazz, String value) {
        if (String.class.equals(clazz)) {
            return value;
        } else if (Integer.class.equals(clazz) || int.class.equals(clazz)) {
            return Integer.parseInt(value);
        } else if (Long.class.equals(clazz) || long.class.equals(clazz)) {
            return Long.parseLong(value);
        } else if (Double.class.equals(clazz) || double.class.equals(clazz)) {
            return Double.parseDouble(value);
        } else if (Float.class.equals(clazz) || float.class.equals(clazz)) {
            return Float.parseFloat(value);
        } else if (Boolean.class.equals(clazz) || boolean.class.equals(clazz)) {
            return Boolean.parseBoolean(value);
        } else if (Byte.class.equals(clazz) || byte.class.equals(clazz)) {
            return Byte.parseByte(value);
        } else if (Short.class.equals(clazz) || short.class.equals(clazz)) {
            return Short.parseShort(value);
            // } else if (List.class.isAssignableFrom(clazz)) {
            //     return List.of(value.split(","));
        } else {
            throw new BootstrapException("不支持的@Value类型: {}.", clazz.getName());
        }
    }


    private boolean isKeyExist(Key key, boolean ignore) {
        if (context.isKeyExist(key)) {
            if (ignore) {
                return true;
            } else {
                throw new BootstrapException("依赖冲突,已经存在相同的Bean: {}.", key);
            }
        }
        return false;
    }


    private List<Key> getParamKeys(Executable executable) {
        final ArrayList<Key> paramsKey = new ArrayList<>();
        for (final Parameter parameter : executable.getParameters()) {
            final String paramName = getName(parameter);
            paramsKey.add(Key.of(parameter.getType(), paramName));
        }
        return paramsKey;
    }


    private void injectValue(Value annotation) {
        String propertyName = annotation.value();
        Key<String> key = Key.of(String.class, propertyName);
        context.injectBean(
                key,
                container -> {
                    ParameterTool params = container.getBean(FILE);
                    return params.get(propertyName);
                },
                List.of(FILE));

    }


    private static Class<?> getBind(Class<?> clazz) {
        final Bind annotation = clazz.getAnnotation(Bind.class);
        return getBind(clazz, annotation);
    }

    private static Class<?> getBind(Method method) {
        final Bind annotation = method.getAnnotation(Bind.class);
        final Class<?> returnType = method.getReturnType();
        return getBind(returnType, annotation);
    }

    private static Class<?> getBind(Class<?> clazz, Bind annotation) {
        final Class<?> bind;
        if (clazz.isInterface()) {
            bind = clazz;
        } else {
            final Class<?>[] interfaces = clazz.getInterfaces();
            bind = interfaces.length == 1 && clazz.getPackageName().startsWith(interfaces[0].getPackageName()) ? interfaces[0] : clazz;
        }
        return bindOrDefault(bind, annotation);
    }


    private static Class<?> bindOrDefault(Class<?> returnType, Bind annotation) {
        return annotation == null ? returnType : annotation.value();
    }

    private static String getName(Parameter parameter) {
        final Name annotation = parameter.getAnnotation(Name.class);
        return nameOrDefault(annotation);
    }

    private static String getName(Class<?> clazz) {
        final Name annotation = clazz.getAnnotation(Name.class);
        return nameOrDefault(annotation);
    }

    private static String getName(Executable method) {
        final Name annotation = method.getAnnotation(Name.class);
        return nameOrDefault(annotation);
    }

    private static String nameOrDefault(Name annotation) {
        return annotation == null ? "" : annotation.value();
    }

}
