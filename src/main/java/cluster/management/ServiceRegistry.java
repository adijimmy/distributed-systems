package cluster.management;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public class ServiceRegistry implements Watcher {

    private static final String REGISTRY_NODE = "/service_registry";
    private final ZooKeeper zooKeeper;
    private String currentZNode;
    private List<String> allServiceAddresses;

    public ServiceRegistry(ZooKeeper zooKeeper) {
        this.zooKeeper = zooKeeper;
        createServiceRegistryNode();
    }

    public void registerToCluster(String metadata) throws KeeperException, InterruptedException {
        currentZNode = zooKeeper.create(REGISTRY_NODE + "/n_",
                metadata.getBytes(),
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("Registered to service registry");
    }

    private void createServiceRegistryNode() {
        try {
            if (zooKeeper.exists(REGISTRY_NODE, false) == null) {
                zooKeeper.create(REGISTRY_NODE,
                        new byte[]{},
                        ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            }
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public void registerForUpdates() {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }

    public synchronized List<String> getAllServiceAddresses() throws KeeperException, InterruptedException {
        if (allServiceAddresses == null) {
            updateAddresses();
        }
        return allServiceAddresses;
    }

    public void unregisterFromCluster() throws KeeperException, InterruptedException {
        if (currentZNode != null && zooKeeper.exists(currentZNode, false) != null) {
            zooKeeper.delete(currentZNode, -1);
        }
    }

    private synchronized void updateAddresses() throws KeeperException, InterruptedException {
        List<String> workerZNodes = zooKeeper.getChildren(REGISTRY_NODE, this);

        List<String> addresses = new ArrayList<>(workerZNodes.size());

        for (String workerZNode : workerZNodes) {
            String workerZNodeFullPath = REGISTRY_NODE + "/" + workerZNode;
            Stat stat = zooKeeper.exists(workerZNodeFullPath, false);
            if (stat != null) {
                byte[] addressBytes = zooKeeper.getData(workerZNodeFullPath, false, stat);
                addresses.add(new String(addressBytes));
            }
        }

        allServiceAddresses = Collections.unmodifiableList(addresses);

        System.out.println("Cluster addresses are : " + allServiceAddresses);
    }

    @Override
    public void process(WatchedEvent event) {
        try {
            updateAddresses();
        } catch (KeeperException | InterruptedException e) {
            e.printStackTrace();
        }
    }
}
