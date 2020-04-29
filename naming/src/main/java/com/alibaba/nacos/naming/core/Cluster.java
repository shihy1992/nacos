/*
 * Copyright 1999-2018 Alibaba Group Holding Ltd.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.alibaba.nacos.naming.core;

import com.alibaba.fastjson.annotation.JSONField;
import com.alibaba.nacos.naming.healthcheck.HealthCheckReactor;
import com.alibaba.nacos.naming.healthcheck.HealthCheckStatus;
import com.alibaba.nacos.naming.healthcheck.HealthCheckTask;
import com.alibaba.nacos.naming.misc.Loggers;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.util.Assert;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @author nkorange
 * @author jifengnan 2019-04-26
 */
public class Cluster extends com.alibaba.nacos.api.naming.pojo.Cluster implements Cloneable {

    private static final String CLUSTER_NAME_SYNTAX = "[0-9a-zA-Z-]+";
    /**
     * a addition for same site routing, can group multiple sites into a region, like Hangzhou, Shanghai, etc.
     */
    private String sitegroup = StringUtils.EMPTY;

    private int defCkport = 80;

    private int defIPPort = -1;

    @JSONField(serialize = false)
    private HealthCheckTask checkTask;

    //存放的持久化实例
    @JSONField(serialize = false)
    private Set<Instance> persistentInstances = new HashSet<>();

    //存放的非持久化实例
    @JSONField(serialize = false)
    private Set<Instance> ephemeralInstances = new HashSet<>();

    @JSONField(serialize = false)
    private Service service;

    @JSONField(serialize = false)
    private volatile boolean inited = false;

    private Map<String, String> metadata = new ConcurrentHashMap<>();

    public Cluster() {
    }

    /**
     * Create a cluster.
     * <p>the cluster name cannot be null, and only the arabic numerals, letters and endashes are allowed.
     *
     * @param clusterName the cluster name
     * @param service     the service to which the current cluster belongs
     * @throws IllegalArgumentException the service is null, or the cluster name is null, or the cluster name is illegal
     * @author jifengnan 2019-04-26
     * @since 1.0.1
     */
    public Cluster(String clusterName, Service service) {
        this.setName(clusterName);
        this.service = service;
        validate();
    }

    public int getDefIPPort() {
        // for compatibility with old entries
        return defIPPort == -1 ? defCkport : defIPPort;
    }

    public void setDefIPPort(int defIPPort) {
        if (defIPPort == 0) {
            throw new IllegalArgumentException("defIPPort can not be 0");
        }
        this.defIPPort = defIPPort;
    }

    public List<Instance> allIPs() {
        List<Instance> allInstances = new ArrayList<>();
        allInstances.addAll(persistentInstances);
        allInstances.addAll(ephemeralInstances);
        return allInstances;
    }

    public List<Instance> allIPs(boolean ephemeral) {
        return ephemeral ? new ArrayList<>(ephemeralInstances) : new ArrayList<>(persistentInstances);
    }

    public void init() {
        if (inited) {
            return;
        }
        checkTask = new HealthCheckTask(this);

        HealthCheckReactor.scheduleCheck(checkTask);
        inited = true;
    }

    public void destroy() {
        if (checkTask != null) {
            checkTask.setCancelled(true);
        }
    }

    public HealthCheckTask getHealthCheckTask() {
        return checkTask;
    }

    public Service getService() {
        return service;
    }

    /**
     * Replace the service for the current cluster.
     * <p>  the service shouldn't be replaced. so if the service is not empty will nothing to do.
     * (the service fields can be changed, but the service A shouldn't be replaced to service B).
     * If the service of a cluster is required to replace, actually, a new cluster is required.
     *
     * @param service the new service
     */
    public void setService(Service service) {
        if (this.service != null) {
            return;
        }
        this.service = service;
    }

    /**
     * this method has been deprecated, the service name shouldn't be changed.
     *
     * @param serviceName the service name
     * @author jifengnan  2019-04-26
     * @since 1.0.1
     */
    @Deprecated
    @Override
    public void setServiceName(String serviceName) {
        super.setServiceName(serviceName);
    }

    /**
     * Get the service name of the current cluster.
     * <p>Note that the returned service name is not the name which set by {@link #setServiceName(String)},
     * but the name of the service to which the current cluster belongs.
     *
     * @return the service name of the current cluster.
     */
    @Override
    public String getServiceName() {
        if (service != null) {
            return service.getName();
        } else {
            return super.getServiceName();
        }
    }

    @Override
    public Cluster clone() throws CloneNotSupportedException {
        super.clone();
        Cluster cluster = new Cluster(this.getName(), service);
        cluster.setHealthChecker(getHealthChecker().clone());
        cluster.persistentInstances = new HashSet<>();
        cluster.checkTask = null;
        cluster.metadata = new HashMap<>(metadata);
        return cluster;
    }

    public void updateIPs(List<Instance> ips, boolean ephemeral) {
        //获取存放临时性实例的set或者是持久化的set
        Set<Instance> toUpdateInstances = ephemeral ? ephemeralInstances : persistentInstances;
        //存放的数据是，key-->ip+端口号+unknown+clusterName,value--->instance实例
        HashMap<String, Instance> oldIPMap = new HashMap<>(toUpdateInstances.size());

        for (Instance ip : toUpdateInstances) {
            //getDatumKey()方法返回的值是，ip+端口号+unknown+clusterName
            oldIPMap.put(ip.getDatumKey(), ip);
        }
        //入参是所有的instance和以前所有的旧的instance
        //返回值是需要更新的instance实例
        List<Instance> updatedIPs = updatedIPs(ips, oldIPMap.values());
        if (updatedIPs.size() > 0) {
            for (Instance ip : updatedIPs) {
                //根据需要更新的instance里面获取ip和端口号，从oldIPMap获取旧的instance实例
                Instance oldIP = oldIPMap.get(ip.getDatumKey());

                // do not update the ip validation status of updated ips
                // because the checker has the most precise result
                // Only when ip is not marked, don't we update the health status of IP:
                //从旧的instance实例中获取健康标识给新的instance实例
                if (!ip.isMarked()) {
                    ip.setHealthy(oldIP.isHealthy());
                }

                if (ip.isHealthy() != oldIP.isHealthy()) {
                    // ip validation status updated
                    Loggers.EVT_LOG.info("{} {SYNC} IP-{} {}:{}@{}",
                        getService().getName(), (ip.isHealthy() ? "ENABLED" : "DISABLED"), ip.getIp(), ip.getPort(), getName());
                }

                if (ip.getWeight() != oldIP.getWeight()) {
                    // ip validation status updated
                    Loggers.EVT_LOG.info("{} {SYNC} {IP-UPDATED} {}->{}", getService().getName(), oldIP.toString(), ip.toString());
                }
            }
        }

        //获取新增的instance实例
        List<Instance> newIPs = subtract(ips, oldIPMap.values());
        if (newIPs.size() > 0) {
            Loggers.EVT_LOG.info("{} {SYNC} {IP-NEW} cluster: {}, new ips size: {}, content: {}",
                getService().getName(), getName(), newIPs.size(), newIPs.toString());

            for (Instance ip : newIPs) {
                HealthCheckStatus.reset(ip);
            }
        }
        //获取老的里面存着而新的里面不存的instance实例，即死亡的instance的实例
        List<Instance> deadIPs = subtract(oldIPMap.values(), ips);

        if (deadIPs.size() > 0) {
            Loggers.EVT_LOG.info("{} {SYNC} {IP-DEAD} cluster: {}, dead ips size: {}, content: {}",
                getService().getName(), getName(), deadIPs.size(), deadIPs.toString());

            for (Instance ip : deadIPs) {
                HealthCheckStatus.remv(ip);
            }
        }

        toUpdateInstances = new HashSet<>(ips);

        if (ephemeral) {
            //将最新的注册表更新到内存
            ephemeralInstances = toUpdateInstances;
        } else {
            persistentInstances = toUpdateInstances;
        }
    }

    //返回需要更新的instance
    public List<Instance> updatedIPs(Collection<Instance> a, Collection<Instance> b) {

        //计算a和b的交集,这个代码的计算逻辑是按照内存地址来计算交集，即如果两个对象的所有字段都一样，在不同的集合中，但是这两个集合中这两个对象不是交集。同一个对象，在不同集合，则是会是交集对象。
        //即，计算出来a和b中包含的地址相同的对象。
        List<Instance> intersects = (List<Instance>) CollectionUtils.intersection(a, b);
        //新建一个并发map用来存放交集数据，key是ip:port,value是instance。
        Map<String, Instance> stringIPAddressMap = new ConcurrentHashMap<>(intersects.size());

        for (Instance instance : intersects) {
            stringIPAddressMap.put(instance.getIp() + ":" + instance.getPort(), instance);
        }
        //可以存放所有数据长度的map，key是instance的字符串形式，value是标记。里面放的是a和b中的所有交集数据。
        Map<String, Integer> intersectMap = new ConcurrentHashMap<>(a.size() + b.size());
        //放的是所有非交集的instance，key是instance的字符串形式，value是instance对象的引用
        Map<String, Instance> instanceMap = new ConcurrentHashMap<>(a.size());
        //instanceMap1中存放所有a中的数据即新数据，key是instance的字符串形式，value是instance对象的引用
        Map<String, Instance> instanceMap1 = new ConcurrentHashMap<>(a.size());
        //b是旧数据
        for (Instance instance : b) {
            //如果b中包含了交集中的数据，即，旧的服务实例中有相同的ip和端口的服务
            if (stringIPAddressMap.containsKey(instance.getIp() + ":" + instance.getPort())) {
                //这一步，intersectMap里面已经包含了所有的交集数据，并且都打上了标记1.
                intersectMap.put(instance.toString(), 1);
            }
        }

        //a是新数据
        for (Instance instance : a) {
            //a中包含了相同的ip和端口的instance对象，但是有可能是一个全新的来自同一个服务的新的服务实例，即，新的服务实例中有来自相同的ip和端口的服务。那么接下来就要判断是否是同一个instance对象，是否需要更新
            if (stringIPAddressMap.containsKey(instance.getIp() + ":" + instance.getPort())) {
                //紧接着判断交集数据中是否有需要更新的数据。即，拿a中数据和b中的数据进行比较。如果intersectMap已经包含了instance的字符串形式(上一步b中的操作)，且instance的内容没有改变，不需要更新。则标记为2
                if (intersectMap.containsKey(instance.toString())) {
                    intersectMap.put(instance.toString(), 2);
                } else {//如果intersectMap未包含了instance的字符串形式，说明instance的内容有改变，需要更新
                    intersectMap.put(instance.toString(), 1);
                }
            }
            //将a中的新数据放入到instanceMap1
            instanceMap1.put(instance.toString(), instance);

        }
        //遍历交集数据，将value为1的放到instanceMap中。
        //这个时候intersectMap里面包含所有的交集数据，其中value有的为2，有的为1，为1的数据可能来自a也可能来自b，2是肯定不需要更新的。1中有一部分来自于b，有一部分来自于a,来自于b的不需要更新，来自于a的需要更新。
        for (Map.Entry<String, Integer> entry : intersectMap.entrySet()) {
            String key = entry.getKey();
            Integer value = entry.getValue();

            //表示是需要更新的，则放入到instanceMap中
            if (value == 1) {
                //且是在新数据中包含的，则放入instanceMap
                if (instanceMap1.containsKey(key)) {
                    instanceMap.put(key, instanceMap1.get(key));
                }
            }
        }

        return new ArrayList<>(instanceMap.values());
    }

    public List<Instance> subtract(Collection<Instance> a, Collection<Instance> b) {
        Map<String, Instance> mapa = new HashMap<>(b.size());
        for (Instance o : b) {
            mapa.put(o.getIp() + ":" + o.getPort(), o);
        }

        List<Instance> result = new ArrayList<>();

        for (Instance o : a) {
            if (!mapa.containsKey(o.getIp() + ":" + o.getPort())) {
                result.add(o);
            }
        }

        return result;
    }

    @Override
    public int hashCode() {
        return Objects.hash(getName());
    }

    @Override
    public boolean equals(Object obj) {
        if (!(obj instanceof Cluster)) {
            return false;
        }

        return getName().equals(((Cluster) obj).getName());
    }

    public int getDefCkport() {
        return defCkport;
    }

    public void setDefCkport(int defCkport) {
        this.defCkport = defCkport;
    }

    public void update(Cluster cluster) {

        if (!getHealthChecker().equals(cluster.getHealthChecker())) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}:, healthChecker: {} -> {}",
                getService().getName(), getName(), getHealthChecker().toString(), cluster.getHealthChecker().toString());
            setHealthChecker(cluster.getHealthChecker());
        }

        if (defCkport != cluster.getDefCkport()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, defCkport: {} -> {}",
                getService().getName(), getName(), defCkport, cluster.getDefCkport());
            defCkport = cluster.getDefCkport();
        }

        if (defIPPort != cluster.getDefIPPort()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, defIPPort: {} -> {}",
                getService().getName(), getName(), defIPPort, cluster.getDefIPPort());
            defIPPort = cluster.getDefIPPort();
        }

        if (!StringUtils.equals(sitegroup, cluster.getSitegroup())) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, sitegroup: {} -> {}",
                getService().getName(), getName(), sitegroup, cluster.getSitegroup());
            sitegroup = cluster.getSitegroup();
        }

        if (isUseIPPort4Check() != cluster.isUseIPPort4Check()) {
            Loggers.SRV_LOG.info("[CLUSTER-UPDATE] {}:{}, useIPPort4Check: {} -> {}",
                getService().getName(), getName(), isUseIPPort4Check(), cluster.isUseIPPort4Check());
            setUseIPPort4Check(cluster.isUseIPPort4Check());
        }

        metadata = cluster.getMetadata();
    }

    public String getSitegroup() {
        return sitegroup;
    }

    public void setSitegroup(String sitegroup) {
        this.sitegroup = sitegroup;
    }

    public boolean contains(Instance ip) {
        return persistentInstances.contains(ip) || ephemeralInstances.contains(ip);
    }

    /**
     * validate the current cluster.
     * <p>the cluster name cannot be null, and only the arabic numerals, letters and endashes are allowed.
     *
     * @throws IllegalArgumentException the service is null, or the cluster name is null, or the cluster name is illegal
     */
    public void validate() {
        Assert.notNull(getName(), "cluster name cannot be null");
        Assert.notNull(service, "service cannot be null");
        if (!getName().matches(CLUSTER_NAME_SYNTAX)) {
            throw new IllegalArgumentException("cluster name can only have these characters: 0-9a-zA-Z-, current: " + getName());
        }
    }
}
