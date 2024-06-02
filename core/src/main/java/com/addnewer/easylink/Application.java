package com.addnewer.easylink;


import com.addnewer.easylink.api.FlinkJob;
import com.addnewer.easylink.core.Bootstrap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * @author pinru
 * @version 1.0
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final Set<Class<?>> components = new HashSet<>();
    private final Bootstrap bootstrap;

    public static Application app(String[] args) {
        return new Application(args);
    }

    public Application(String[] args) {
        logger.info("Application args: {}", Arrays.toString(args));
        bootstrap = new Bootstrap(args);
    }

    public Application addComponent(Class<?> clazz) {
        components.add(clazz);
        return this;
    }

    public void run() throws Exception {

        logger.info("init application.");
        logger.info("load  application components: {} .", components);
        final List<Class<? extends FlinkJob>> jobs = new ArrayList<>();
        for (Class<?> clazz : components) {
            bootstrap.injectComponent(clazz);
            if (FlinkJob.class.isAssignableFrom(clazz)) {
                jobs.add((Class<? extends FlinkJob>) clazz);
            }
        }

        Set<Class<?>> defaultComponents = new HashSet<>();
        defaultComponents.add(DefaultConfiguration.class);

        logger.info("load default components: {} .", defaultComponents);
        bootstrap.injectDefaultComponents(defaultComponents);
        bootstrap.run(jobs);
    }


}
