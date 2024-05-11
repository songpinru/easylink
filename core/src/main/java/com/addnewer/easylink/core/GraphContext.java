package com.addnewer.easylink.core;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author pinru
 * @version 1.0
 * @date 2024/5/6
 */
class GraphContext {
    private static final Logger logger = LoggerFactory.getLogger(GraphContext.class);

    private final Map<Key, InjectFunction<?>> injectMap;
    private final Map<Key, List<Key>> dependency;
    private final Map<Key, List<Key>> dependOn;
    private final Container singleton = new Container();
    private final Set<Key> entryPoints = new HashSet<>();

    public GraphContext() {
        injectMap = new HashMap<>();
        dependency = new HashMap<>();
        dependOn = new HashMap<>();
    }

    public void injectBean(Key key, InjectFunction<?> injectFunction) {
        injectBean(key, injectFunction, List.of());
    }

    public void injectBean(Key key, InjectFunction<?> injectFunction, List<Key> paramKeys) {
        logger.debug("inject bean {}", key);
        injectMap.put(key, injectFunction);
        edgeRelation(key, paramKeys);
    }

    public boolean isKeyExist(Key key) {
        return injectMap.containsKey(key);
    }

    public <T> T getInstance(Class<T> clazz, String name) {
        return getInstance(Key.of(clazz, name));
    }

    public <T> T getInstance(Class<T> clazz) {
        return getInstance(clazz, "");
    }

    private <T> T getInstance(Key<T> key) {
        return singleton.getBean(key);
    }

    public <T> void addEntryPoint(Class<T> clazz, String name) {
        addEntryPoint(Key.of(clazz, name));
    }

    public <T> void addEntryPoint(Class<T> clazz) {
        addEntryPoint(clazz, "");
    }

    private <T> void addEntryPoint(Key<T> key) {
        entryPoints.add(key);
    }

    public void generateContainer() {
        Map<Key, Integer> topologyMap = generateTopologyMap(entryPoints.toArray(new Key[0]));
        fillSingleContainer(topologyMap);
    }


    @SafeVarargs
    private <T> Map<Key, Integer> generateTopologyMap(Key<T>... keys) {
        final Map<Key, Integer> topologyMap = new ConcurrentHashMap<>();
        for (final Key<T> key : keys) {
            topology(topologyMap, key);
        }
        return topologyMap;
    }

    private void fillSingleContainer(Map<Key, Integer> topologyMap) {
        logger.info("generate container, topologyMap: {}", topologyMap);

        // 把入度为0的项加入容器，并把相关的项的计数-1，直至完成
        while (!topologyMap.isEmpty()) {
            logger.debug("the rest of topologyMap: {}", topologyMap);
            topologyMap.forEach((k, c) -> {
                if (c == 0) {
                    final InjectFunction<?> injectFunction = injectMap.get(k);
                    singleton.addBean(k, injectFunction.provide(singleton));
                    computeTopology(topologyMap, k);
                    topologyMap.remove(k);
                }
            });
            logger.debug("the current container: {}", singleton);
        }
    }

    private void computeTopology(Map<Key, Integer> topologyMap, Key k) {
        dependOn
                .getOrDefault(k, List.of())
                .forEach(in -> {
                    topologyMap.computeIfPresent(in, (i, v) -> v - 1);
                    computeTopology(topologyMap, in);
                });
    }

    private int topology(Map<Key, Integer> map, Key key) {
        logger.debug("topology bean:{}", key);
        final List<Key> dependencies = dependency.get(key);
        if (dependencies == null) {
            throw new GraphException("{} 依赖的Bean:{} 不存在!",dependOn.get(key),key);
        }
        int count = dependencies.size();
        for (final Key dependency : dependencies) {
            if (map.containsKey(dependency)) {
                count += map.get(dependency);
            } else {
                count += topology(map, dependency);
            }
        }
        map.put(key, count);
        return count;
    }

    private void edgeRelation(Key key, List<Key> paramKeys) {
        dependency.put(key, paramKeys);
        paramKeys.forEach(p -> dependOn.computeIfAbsent(p, k -> new ArrayList<>()).add(key));
    }


}
