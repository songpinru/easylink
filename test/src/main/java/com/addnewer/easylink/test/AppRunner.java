package com.addnewer.easylink.test;

import com.addnewer.easylink.DefaultConfiguration;
import com.addnewer.easylink.api.Bean;
import com.addnewer.easylink.api.Configuration;
import com.addnewer.easylink.api.FlinkJob;
import com.addnewer.easylink.config.ConfigurationException;
import com.addnewer.easylink.core.Bootstrap;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration;
import org.apache.flink.test.util.MiniClusterWithClientResource;
import org.junit.runner.Description;
import org.junit.runner.manipulation.Filter;
import org.junit.runner.manipulation.NoTestsRemainException;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.ParentRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * 集成测试的Runner,类似{@link Configuration},可以注入{@link Bean}来替换source，sink做集成测试
 *
 * @author pinru
 * @version 1.0
 */
public class AppRunner extends ParentRunner<FrameworkMethod> {
    private final MiniClusterWithClientResource flinkCluster =
            new MiniClusterWithClientResource(
                    new MiniClusterResourceConfiguration
                            .Builder()
                            .setNumberSlotsPerTaskManager(2)
                            .setNumberTaskManagers(1)
                            .build());

    public AppRunner(Class<?> testClass) throws InitializationError {
        super(testClass);
    }

    @Override
    protected List<FrameworkMethod> getChildren() {
        return List.of();
    }

    @Override
    protected Description describeChild(FrameworkMethod child) {
        return null;
    }

    @Override
    protected void runChild(FrameworkMethod method, RunNotifier notifier) {

    }

    @Override
    public void filter(Filter filter) throws NoTestsRemainException {
    }

    @Override
    public void run(RunNotifier notifier) {
        try {
            flinkCluster.before();
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        String active = "test";
        Profile profile = getTestClass().getAnnotation(Profile.class);
        if (profile != null) {
            active = profile.value();
        }
        String fileName = "application-%s.properties".formatted(active);

        InputStream configInputStream = Thread.currentThread().getContextClassLoader().getResourceAsStream(fileName);
        if (configInputStream == null) {
            throw new ConfigurationException("配置文件: {} 不存在.", fileName);
        }
        final ParameterTool config;
        try {

            config = ParameterTool.fromPropertiesFile(configInputStream);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        Bootstrap bootstrap = new Bootstrap();
        bootstrap.initConfig(config);
        bootstrap.injectComponent(getTestClass().getJavaClass());
        if (getTestClass().getAnnotation(Configuration.class) == null) {
            bootstrap.injectBeansOfConfiguration(getTestClass().getJavaClass(), false);
        }

        bootstrap.injectComponent(DefaultConfiguration.class);
        final List<Class<? extends FlinkJob>> jobs = new ArrayList<>();
        Set<Class<?>> defaultComponents;
        Components components = getTestClass().getAnnotation(Components.class);
        if (components != null) {
            defaultComponents = Arrays
                    .stream(components.value())
                    .map(Component::value)
                    .flatMap(Arrays::stream)
                    .collect(Collectors.toSet());
        } else {
            Component annotation = getTestClass().getAnnotation(Component.class);
            if (annotation != null) {
                defaultComponents = Set.of(annotation.value());
            } else {
                throw new RuntimeException("需要指定Component.");
            }
        }
        defaultComponents.forEach(c -> {
            if (FlinkJob.class.isAssignableFrom(c)) {
                jobs.add((Class<? extends FlinkJob>) c);
            }

        });
        bootstrap.injectDefaultComponents(defaultComponents);

        try {
            bootstrap.run(jobs);
        } catch (Exception e) {
            throw new RuntimeException(e);
        }

        flinkCluster.after();
    }
}
