package cluster.management;

import org.apache.commons.lang.StringUtils;
import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.util.Collections;
import java.util.List;

public class LeaderElection implements Watcher {

    private static final String ELECTION_NAMESPACE = "/election";
    private String currentZNodeName;
    private final ZooKeeper zooKeeper;
    private final OnElectionCallback onElectionCallback;

    public LeaderElection(ZooKeeper zooKeeper, OnElectionCallback onElectionCallback) {
        this.zooKeeper = zooKeeper;
        this.onElectionCallback = onElectionCallback;
    }

    public void volunteerForLeadership() throws KeeperException, InterruptedException {
        String zNodePrefix = ELECTION_NAMESPACE + "/c_";
        String zNodeFullPath = zooKeeper.create(zNodePrefix,
                new byte[]{},
                ZooDefs.Ids.OPEN_ACL_UNSAFE,
                CreateMode.EPHEMERAL_SEQUENTIAL);
        System.out.println("ephemeral node name: " + zNodeFullPath);
        currentZNodeName = zNodeFullPath.replace(ELECTION_NAMESPACE + "/", "");
    }

    public void reelectLeader() throws KeeperException, InterruptedException {
        Stat predecessorStat = null;
        String predecessorZNodeName = StringUtils.EMPTY;
        while (predecessorStat == null) {
            List<String> children = zooKeeper.getChildren(ELECTION_NAMESPACE, false);

            Collections.sort(children);
            String smallestChild = children.get(0);

            if (smallestChild.equalsIgnoreCase(currentZNodeName)) {
                System.out.println("Leader node is: " + currentZNodeName);
                onElectionCallback.onElectedToBeLeader();
                return;
            } else {
                System.out.println("Leader node is: " + smallestChild);
                int predecessorNode = Collections.binarySearch(children, currentZNodeName) - 1;
                predecessorZNodeName = children.get(predecessorNode);
                predecessorStat = zooKeeper.exists(ELECTION_NAMESPACE + "/" + predecessorZNodeName, this);
            }
        }
        onElectionCallback.onWorker();
        System.out.println("Watching node: " + predecessorZNodeName);
    }

    @Override
    public void process(WatchedEvent event) {
        switch (event.getType()) {
            case NodeDeleted:
                try {
                    reelectLeader();
                } catch (KeeperException | InterruptedException e) {
                    e.printStackTrace();
                }
        }
    }
}
