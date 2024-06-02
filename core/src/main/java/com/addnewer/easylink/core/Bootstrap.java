package com.addnewer.easylink.core;


import com.addnewer.easylink.api.*;
import com.addnewer.easylink.auto.AutoUtil;
import com.addnewer.easylink.config.ConfigurationUtil;
import com.addnewer.easylink.utils.PojoUtil;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
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


    public static final Key<ParameterTool> ARGS = Key.of(ParameterTool.class, "args");
    public static final Key<ParameterTool> SYSTEM = Key.of(ParameterTool.class, "system");
    public static final Key<ParameterTool> CONFIG = Key.of(ParameterTool.class, "config");

    private ParameterTool config;

    private final GraphContext context = new GraphContext();
    List<AutoInject> list = ServiceLoader
            .load(AutoInject.class)
            .stream()
            .map(ServiceLoader.Provider::get)
            .toList();


    public Bootstrap(String[] args) {
        initArgs(args);
    }

    public Bootstrap() {
    }

    public void initArgs(final String[] args) {
        injectBean(ARGS, ParameterTool.fromArgs(args));
        injectBean(SYSTEM, ParameterTool.fromSystemProperties());
        initConfig(ConfigurationUtil.loadConfigurations(args));
    }

    public void initConfig(ParameterTool config) {
        this.config = config;
        injectBean(CONFIG, config);
    }

    private void injectBean(Key<?> key, Object object) {
        injectBean(key, container -> object);
    }

    private void injectBean(Key<?> key, InjectFunction<?> injectFunction) {
        injectBean(key, injectFunction, Set.of());
    }


    private void injectBean(Key<?> key, InjectFunction<?> injectFunction, Set<Key> paramKeys) {
        context.injectBean(key, injectFunction, paramKeys);
    }

    private void autoInjectPojo(Class<?> clazz, String name) {
        Key<?> key = Key.of(clazz, name);
        if (!context.isKeyExist(key)) {
            list.forEach(
                    autoInject -> {
                        if (autoInject.isAutoInject(clazz)) {
                            Object bean = autoInject.auto(name, config);
                            injectBean(key, bean);
                        }
                    }
            );
        }

    }

    public void run(List<Class<? extends FlinkJob>> jobs) throws Exception {
        logger.info("run flink jobs: {} .", jobs);
        jobs.forEach(context::addEntryPoint);
        context.generateContainer();

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

    public void injectBeansOfConfiguration(Class<?> configuration, boolean ignore) {

        Key<?> configKey = Key.of(configuration);

        Arrays
                .stream(configuration.getDeclaredMethods())
                .forEach(method -> {
                    method.setAccessible(true);
                    final String name = getName(method);
                    final List<Key> paramKeys = getParamKeys(method);
                    final InjectFunction<?> injectFunction;
                    final Key key;

                    if (method.isAnnotationPresent(Source.class)) {
                        Class<?> returnType = method.getReturnType();
                        if (DataStream.class.isAssignableFrom(returnType)) {
                            key = new Key(AppSource.class, name);
                            if (isKeyExist(key, ignore)) return;

                            injectFunction = container -> {
                                Object config = container.getBean(configKey);
                                return (AppSource) () -> (DataStream) method.invoke(config, paramKeys
                                        .stream()
                                        .map(container::getBean)
                                        .toArray());

                            };

                        } else {
                            throw new BootstrapException("{} 里Source的返回类型必须是DataStream！", configuration.getName());
                        }
                    } else if (method.isAnnotationPresent(Bean.class)) {
                        final Class<?> bind = getBind(method);
                        key = new Key(bind, name);
                        injectFunction = container -> {
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
                    } else {
                        return;
                    }
                    Set<Key> keys = new HashSet<>();
                    keys.addAll(paramKeys);
                    keys.add(configKey);
                    injectBean(key, injectFunction, keys);

                });

    }


    public void injectComponent(Class<?> clazz) {
        logger.debug("inject component: {}", clazz);

        injectComponent(clazz, false);
    }

    public void injectDefaultComponents(Set<Class<?>> components) {
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
                    try {
                        String value = container.getBean(k);
                        PojoUtil.transform(c,f,value);
                    } catch (IllegalAccessException e) {
                        throw new BootstrapException("{} 注入@Value:{} 失败.", e, clazz.getName(), k);
                    }
                });
                return c;
            } catch (InvocationTargetException | InstantiationException | IllegalAccessException e) {
                throw new BootstrapException("构造Component: {} 失败.", e, clazz.getName());
            }
        };
        if (clazz.isAnnotationPresent(Configuration.class)) {
            injectBeansOfConfiguration(clazz, ignore);
        }
        Set<Key> keys = new HashSet<>();
        keys.addAll(paramKeys);
        if (!values.isEmpty()) {
            keys.addAll(values.keySet());
        }
        injectBean(key, injectFunction, keys);
    }


    private Map<Key<String>, Field> getValuesFromClass(Class<?> clazz) {
        Map<Key<String>, Field> values = new HashMap<>();
        Field[] fields = clazz.getDeclaredFields();
        for (final Field field : fields) {
            field.setAccessible(true);
            if (field.isAnnotationPresent(Value.class)) {
                final Value value = field.getAnnotation(Value.class);
                if (value.value().isEmpty()) {
                    throw new BootstrapException("Value 值不可为空！");
                }
                values.put(Key.of(String.class, value.value()), field);
                injectValue(value);
            }
        }
        return values;
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
            autoInjectPojo(parameter.getType(), paramName);
            paramsKey.add(Key.of(parameter.getType(), paramName));
        }
        return paramsKey;
    }


    private void injectValue(Value annotation) {
        String propertyName = annotation.value();
        Key<String> key = Key.of(String.class, propertyName);
        injectBean(
                key,
                container -> {
                    ParameterTool params = container.getBean(CONFIG);
                    return params.get(propertyName);
                },
                Set.of(CONFIG));

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
