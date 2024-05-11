package com.addnewer;


import com.addnewer.core.Bootstrap;
import com.addnewer.core.ComponentsProvider;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.ServiceLoader;

/**
 * @author pinru
 * @version 1.0
 */
public class Application {
    private static final Logger logger = LoggerFactory.getLogger(Application.class);

    private final List<Class<?>> components = new ArrayList<>();
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
        ArrayList<Class<?>> defaultComponents = new ArrayList<>();
        defaultComponents.add(DefaultConfiguration.class);
        ServiceLoader
                .load(ComponentsProvider.class)
                .stream()
                .map(ServiceLoader.Provider::get)
                .flatMap(cs -> cs.get().stream())
                .forEach(defaultComponents::add);
        bootstrap.run(components, defaultComponents);
    }


}
