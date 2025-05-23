/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iotdb.consensus.iot;

import org.apache.iotdb.common.rpc.thrift.TEndPoint;
import org.apache.iotdb.common.rpc.thrift.TSStatus;
import org.apache.iotdb.commons.client.IClientManager;
import org.apache.iotdb.commons.concurrent.IoTDBThreadPoolFactory;
import org.apache.iotdb.commons.concurrent.ThreadName;
import org.apache.iotdb.commons.concurrent.threadpool.ScheduledExecutorUtil;
import org.apache.iotdb.commons.consensus.ConsensusGroupId;
import org.apache.iotdb.commons.exception.StartupException;
import org.apache.iotdb.commons.service.RegisterManager;
import org.apache.iotdb.commons.utils.FileUtils;
import org.apache.iotdb.commons.utils.KillPoint.DataNodeKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusDeleteLocalPeerKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.IoTConsensusRemovePeerCoordinatorKillPoints;
import org.apache.iotdb.commons.utils.KillPoint.KillPoint;
import org.apache.iotdb.commons.utils.StatusUtils;
import org.apache.iotdb.consensus.IConsensus;
import org.apache.iotdb.consensus.IStateMachine;
import org.apache.iotdb.consensus.IStateMachine.Registry;
import org.apache.iotdb.consensus.common.DataSet;
import org.apache.iotdb.consensus.common.Peer;
import org.apache.iotdb.consensus.common.request.IConsensusRequest;
import org.apache.iotdb.consensus.config.ConsensusConfig;
import org.apache.iotdb.consensus.config.IoTConsensusConfig;
import org.apache.iotdb.consensus.exception.ConsensusException;
import org.apache.iotdb.consensus.exception.ConsensusGroupAlreadyExistException;
import org.apache.iotdb.consensus.exception.ConsensusGroupModifyPeerException;
import org.apache.iotdb.consensus.exception.ConsensusGroupNotExistException;
import org.apache.iotdb.consensus.exception.IllegalPeerEndpointException;
import org.apache.iotdb.consensus.exception.IllegalPeerNumException;
import org.apache.iotdb.consensus.exception.PeerAlreadyInConsensusGroupException;
import org.apache.iotdb.consensus.exception.PeerNotInConsensusGroupException;
import org.apache.iotdb.consensus.iot.client.AsyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.AsyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.IoTConsensusClientPool.SyncIoTConsensusServiceClientPoolFactory;
import org.apache.iotdb.consensus.iot.client.SyncIoTConsensusServiceClient;
import org.apache.iotdb.consensus.iot.logdispatcher.IoTConsensusMemoryManager;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCService;
import org.apache.iotdb.consensus.iot.service.IoTConsensusRPCServiceProcessor;
import org.apache.iotdb.consensus.iot.snapshot.IoTConsensusRateLimiter;
import org.apache.iotdb.rpc.RpcUtils;
import org.apache.iotdb.rpc.TSStatusCode;

import com.google.common.collect.ImmutableList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiConsumer;
import java.util.stream.Collectors;

public class IoTConsensus implements IConsensus {

  private static final long READER_UPDATE_INTERVAL_IN_MINUTES = 3;
  private final Logger logger = LoggerFactory.getLogger(IoTConsensus.class);

  private final TEndPoint thisNode;
  private final int thisNodeId;
  private final File storageDir;
  private final IStateMachine.Registry registry;
  private final Map<ConsensusGroupId, IoTConsensusServerImpl> stateMachineMap =
      new ConcurrentHashMap<>();
  private final IoTConsensusRPCService service;
  private final RegisterManager registerManager = new RegisterManager();
  private IoTConsensusConfig config;
  private final IClientManager<TEndPoint, AsyncIoTConsensusServiceClient> clientManager;
  private final IClientManager<TEndPoint, SyncIoTConsensusServiceClient> syncClientManager;
  private final ScheduledExecutorService backgroundTaskService;
  private Future<?> updateReaderFuture;
  private Map<ConsensusGroupId, List<Peer>> correctPeerListBeforeStart = null;

  public IoTConsensus(ConsensusConfig config, Registry registry) {
    this.thisNode = config.getThisNodeEndPoint();
    this.thisNodeId = config.getThisNodeId();
    this.storageDir = new File(config.getStorageDir());
    this.config = config.getIotConsensusConfig();
    this.registry = registry;
    this.service = new IoTConsensusRPCService(thisNode, config.getIotConsensusConfig());
    this.clientManager =
        new IClientManager.Factory<TEndPoint, AsyncIoTConsensusServiceClient>()
            .createClientManager(
                new AsyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.syncClientManager =
        new IClientManager.Factory<TEndPoint, SyncIoTConsensusServiceClient>()
            .createClientManager(
                new SyncIoTConsensusServiceClientPoolFactory(config.getIotConsensusConfig()));
    this.backgroundTaskService =
        IoTDBThreadPoolFactory.newSingleThreadScheduledExecutor(
            ThreadName.IOT_CONSENSUS_BACKGROUND_TASK_EXECUTOR.getName());
    // init IoTConsensus memory manager
    IoTConsensusMemoryManager.getInstance()
        .init(
            config.getIotConsensusConfig().getReplication().getConsensusMemoryBlock(),
            config.getIotConsensusConfig().getReplication().getMaxMemoryRatioForQueue());
    // init IoTConsensus Rate Limiter
    IoTConsensusRateLimiter.getInstance()
        .init(
            config
                .getIotConsensusConfig()
                .getReplication()
                .getRegionMigrationSpeedLimitBytesPerSecond());
  }

  @Override
  public synchronized void start() throws IOException {
    initAndRecover();
    service.initSyncedServiceImpl(new IoTConsensusRPCServiceProcessor(this));
    try {
      registerManager.register(service);
    } catch (StartupException e) {
      throw new IOException(e);
    }
    if (updateReaderFuture == null) {
      updateReaderFuture =
          ScheduledExecutorUtil.safelyScheduleWithFixedDelay(
              backgroundTaskService,
              () ->
                  stateMachineMap
                      .values()
                      .forEach(impl -> impl.getLogDispatcher().checkAndFlushIndex()),
              READER_UPDATE_INTERVAL_IN_MINUTES,
              READER_UPDATE_INTERVAL_IN_MINUTES,
              TimeUnit.MINUTES);
    }
  }

  private void initAndRecover() throws IOException {
    if (!storageDir.exists()) {
      if (!storageDir.mkdirs()) {
        throw new IOException(String.format("Unable to create consensus dir at %s", storageDir));
      }
    } else {
      try (DirectoryStream<Path> stream = Files.newDirectoryStream(storageDir.toPath())) {
        for (Path path : stream) {
          String[] items = path.getFileName().toString().split("_");
          ConsensusGroupId consensusGroupId =
              ConsensusGroupId.Factory.create(
                  Integer.parseInt(items[0]), Integer.parseInt(items[1]));
          IoTConsensusServerImpl consensus =
              new IoTConsensusServerImpl(
                  path.toString(),
                  new Peer(consensusGroupId, thisNodeId, thisNode),
                  new TreeSet<>(),
                  registry.apply(consensusGroupId),
                  backgroundTaskService,
                  clientManager,
                  syncClientManager,
                  config);
          stateMachineMap.put(consensusGroupId, consensus);
        }
      }
    }
    if (correctPeerListBeforeStart != null) {
      BiConsumer<ConsensusGroupId, List<Peer>> resetPeerListWithoutThrow =
          (consensusGroupId, peers) -> {
            try {
              resetPeerListImpl(consensusGroupId, peers, false);
            } catch (ConsensusGroupNotExistException ignore) {

            } catch (Exception e) {
              logger.warn("Failed to reset peer list while start", e);
            }
          };
      // make peers which are in list correct
      correctPeerListBeforeStart.forEach(resetPeerListWithoutThrow);
      // clear peers which are not in the list
      stateMachineMap.keySet().stream()
          .filter(consensusGroupId -> !correctPeerListBeforeStart.containsKey(consensusGroupId))
          // copy to a new list to avoid concurrent modification
          .collect(Collectors.toList())
          .forEach(
              consensusGroupId ->
                  resetPeerListWithoutThrow.accept(consensusGroupId, Collections.emptyList()));
    }
    stateMachineMap.values().forEach(IoTConsensusServerImpl::start);
  }

  @Override
  public synchronized void stop() {
    Optional.ofNullable(updateReaderFuture).ifPresent(future -> future.cancel(false));
    stateMachineMap.values().parallelStream().forEach(IoTConsensusServerImpl::stop);
    clientManager.close();
    syncClientManager.close();
    registerManager.deregisterAll();
    backgroundTaskService.shutdown();
    try {
      backgroundTaskService.awaitTermination(5, TimeUnit.SECONDS);
    } catch (InterruptedException e) {
      logger.warn("{}: interrupted when shutting down add Executor with exception {}", this, e);
      Thread.currentThread().interrupt();
    }
  }

  @Override
  public TSStatus write(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    if (impl.isReadOnly()) {
      return StatusUtils.getStatus(TSStatusCode.SYSTEM_READ_ONLY);
    } else if (!impl.isActive()) {
      String message =
          String.format(
              "Peer is inactive and not ready to write request, %s, DataNode Id: %s",
              groupId.toString(), impl.getThisNode().getNodeId());
      return RpcUtils.getStatus(TSStatusCode.WRITE_PROCESS_REJECT, message);
    } else {
      return impl.write(request);
    }
  }

  @Override
  public DataSet read(ConsensusGroupId groupId, IConsensusRequest request)
      throws ConsensusException {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .orElseThrow(() -> new ConsensusGroupNotExistException(groupId))
        .read(request);
  }

  @SuppressWarnings("java:S2201")
  @Override
  public void createLocalPeer(ConsensusGroupId groupId, List<Peer> peers)
      throws ConsensusException {
    int consensusGroupSize = peers.size();
    if (consensusGroupSize == 0) {
      throw new IllegalPeerNumException(consensusGroupSize);
    }
    if (!peers.contains(new Peer(groupId, thisNodeId, thisNode))) {
      throw new IllegalPeerEndpointException(thisNode, peers);
    }
    AtomicBoolean exist = new AtomicBoolean(true);
    Optional.ofNullable(
            stateMachineMap.computeIfAbsent(
                groupId,
                k -> {
                  exist.set(false);

                  String path = buildPeerDir(storageDir, groupId);
                  File file = new File(path);
                  if (!file.mkdirs()) {
                    logger.warn("Unable to create consensus dir for group {} at {}", groupId, path);
                    return null;
                  }

                  IoTConsensusServerImpl impl =
                      new IoTConsensusServerImpl(
                          path,
                          new Peer(groupId, thisNodeId, thisNode),
                          new TreeSet<>(peers),
                          registry.apply(groupId),
                          backgroundTaskService,
                          clientManager,
                          syncClientManager,
                          config);
                  impl.start();
                  return impl;
                }))
        .orElseThrow(
            () ->
                new ConsensusException(
                    String.format("Unable to create consensus dir for group %s", groupId)));
    KillPoint.setKillPoint(DataNodeKillPoints.DESTINATION_CREATE_LOCAL_PEER);
    if (exist.get()) {
      throw new ConsensusGroupAlreadyExistException(groupId);
    }
  }

  @Override
  public void deleteLocalPeer(ConsensusGroupId groupId) throws ConsensusException {
    KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.BEFORE_DELETE);
    AtomicBoolean exist = new AtomicBoolean(false);
    stateMachineMap.computeIfPresent(
        groupId,
        (k, v) -> {
          exist.set(true);
          v.stop();
          return null;
        });
    if (!exist.get()) {
      throw new ConsensusGroupNotExistException(groupId);
    }
    FileUtils.deleteFileOrDirectory(new File(buildPeerDir(storageDir, groupId)));
    KillPoint.setKillPoint(IoTConsensusDeleteLocalPeerKillPoints.AFTER_DELETE);
  }

  @Override
  public void addRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    synchronized (impl) {
      if (impl.getConfiguration().contains(peer)) {
        throw new PeerAlreadyInConsensusGroupException(groupId, peer);
      }
      try {
        // step 1: inactive new Peer to prepare for following steps
        logger.info("[IoTConsensus] inactivate new peer: {}", peer);
        impl.inactivatePeer(peer, false);

        // step 2: notify all the other Peers to build the sync connection to newPeer
        logger.info("[IoTConsensus] notify current peers to build sync log...");
        impl.notifyPeersToBuildSyncLogChannel(peer);

        // step 3: take snapshot
        logger.info("[IoTConsensus] start to take snapshot...");

        impl.takeSnapshot();

        // step 4: transit snapshot
        logger.info("[IoTConsensus] start to transmit snapshot...");
        impl.transmitSnapshot(peer);

        // step 5: let the new peer load snapshot
        logger.info("[IoTConsensus] trigger new peer to load snapshot...");
        impl.triggerSnapshotLoad(peer);
        KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_TRANSITION);

        // step 6: active new Peer
        logger.info("[IoTConsensus] activate new peer...");
        impl.activePeer(peer);

        // step 7: notify remote peer to clean up transferred snapshot
        logger.info("[IoTConsensus] clean up remote snapshot...");
        try {
          impl.cleanupRemoteSnapshot(peer);
        } catch (ConsensusGroupModifyPeerException e) {
          logger.warn("[IoTConsensus] failed to cleanup remote snapshot", e);
        }
        KillPoint.setKillPoint(DataNodeKillPoints.COORDINATOR_ADD_PEER_DONE);

      } catch (ConsensusGroupModifyPeerException e) {
        logger.info("[IoTConsensus] add remote peer failed, automatic cleanup side effects...");
        // try to clean up the sync log channel
        impl.notifyPeersToRemoveSyncLogChannel(peer);
        throw new ConsensusException(e);
      } finally {
        logger.info("[IoTConsensus] clean up local snapshot...");
        impl.cleanupLocalSnapshot();
      }
    }
  }

  @Override
  public void removeRemotePeer(ConsensusGroupId groupId, Peer peer) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    synchronized (impl) {
      if (!impl.getConfiguration().contains(peer)) {
        throw new PeerNotInConsensusGroupException(groupId, peer.toString());
      }

      KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.INIT);

      // let other peers remove the sync channel with target peer
      impl.notifyPeersToRemoveSyncLogChannel(peer);
      KillPoint.setKillPoint(
          IoTConsensusRemovePeerCoordinatorKillPoints
              .AFTER_NOTIFY_PEERS_TO_REMOVE_REPLICATE_CHANNEL);

      try {
        // let target peer reject new write
        impl.inactivatePeer(peer, true);
        KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.AFTER_INACTIVE_PEER);
        // wait its SyncLog to complete
        impl.waitTargetPeerUntilSyncLogCompleted(peer);
        // wait its region related resource to release
        impl.waitReleaseAllRegionRelatedResource(peer);
      } catch (ConsensusGroupModifyPeerException e) {
        throw new ConsensusException(e.getMessage());
      }
      KillPoint.setKillPoint(IoTConsensusRemovePeerCoordinatorKillPoints.FINISH);
    }
  }

  @Override
  public void transferLeader(ConsensusGroupId groupId, Peer newLeader) throws ConsensusException {
    throw new ConsensusException("IoTConsensus does not support leader transfer");
  }

  @Override
  public void triggerSnapshot(ConsensusGroupId groupId, boolean force) throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));
    try {
      impl.takeSnapshot();
    } catch (ConsensusGroupModifyPeerException e) {
      throw new ConsensusException(e.getMessage());
    }
  }

  @Override
  public boolean isLeader(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public boolean isLeaderReady(ConsensusGroupId groupId) {
    return true;
  }

  @Override
  public long getLogicalClock(ConsensusGroupId groupId) {
    return Optional.ofNullable(stateMachineMap.get(groupId))
        .map(IoTConsensusServerImpl::getSearchIndex)
        .orElse(0L);
  }

  @Override
  public Peer getLeader(ConsensusGroupId groupId) {
    if (!stateMachineMap.containsKey(groupId)) {
      return null;
    }
    return new Peer(groupId, thisNodeId, thisNode);
  }

  @Override
  public int getReplicationNum(ConsensusGroupId groupId) {
    IoTConsensusServerImpl impl = stateMachineMap.get(groupId);
    return impl != null ? impl.getConfiguration().size() : 0;
  }

  @Override
  public List<ConsensusGroupId> getAllConsensusGroupIds() {
    return new ArrayList<>(stateMachineMap.keySet());
  }

  @Override
  public String getRegionDirFromConsensusGroupId(ConsensusGroupId groupId) {
    return buildPeerDir(storageDir, groupId);
  }

  @Override
  public void reloadConsensusConfig(ConsensusConfig consensusConfig) {
    config = consensusConfig.getIotConsensusConfig();

    for (IoTConsensusServerImpl impl : stateMachineMap.values()) {
      impl.reloadConsensusConfig(config);
    }

    // update region migration speed limit
    IoTConsensusRateLimiter.getInstance()
        .init(config.getReplication().getRegionMigrationSpeedLimitBytesPerSecond());
  }

  @Override
  public void recordCorrectPeerListBeforeStarting(
      Map<ConsensusGroupId, List<Peer>> correctPeerList) {
    logger.info("Record correct peer list: {}", correctPeerList);
    this.correctPeerListBeforeStart = correctPeerList;
  }

  @Override
  public void resetPeerList(ConsensusGroupId groupId, List<Peer> correctPeers)
      throws ConsensusException {
    resetPeerListImpl(groupId, correctPeers, true);
  }

  private void resetPeerListImpl(
      ConsensusGroupId groupId, List<Peer> correctPeers, boolean startNow)
      throws ConsensusException {
    IoTConsensusServerImpl impl =
        Optional.ofNullable(stateMachineMap.get(groupId))
            .orElseThrow(() -> new ConsensusGroupNotExistException(groupId));

    Peer localPeer = new Peer(groupId, thisNodeId, thisNode);
    if (!correctPeers.contains(localPeer)) {
      logger.info(
          "[RESET PEER LIST] {} Local peer is not in the correct configuration, delete it.",
          groupId);
      deleteLocalPeer(groupId);
      return;
    }

    synchronized (impl) {
      // remove invalid peer
      ImmutableList<Peer> currentMembers = ImmutableList.copyOf(impl.getConfiguration());
      String previousPeerListStr = currentMembers.toString();
      for (Peer peer : currentMembers) {
        if (!correctPeers.contains(peer)) {
          if (!impl.removeSyncLogChannel(peer)) {
            logger.error(
                "[RESET PEER LIST] {} Failed to remove sync channel with: {}", groupId, peer);
          } else {
            logger.info("[RESET PEER LIST] {} Remove sync channel with: {}", groupId, peer);
          }
        }
      }
      // add correct peer
      for (Peer peer : correctPeers) {
        if (!impl.getConfiguration().contains(peer)) {
          impl.buildSyncLogChannel(peer, startNow);
          logger.info("[RESET PEER LIST] {} Build sync channel with: {}", groupId, peer);
        }
      }
      // show result
      String newPeerListStr = impl.getConfiguration().toString();
      if (!previousPeerListStr.equals(newPeerListStr)) {
        logger.info(
            "[RESET PEER LIST] {} Local peer list has been reset: {} -> {}",
            groupId,
            previousPeerListStr,
            newPeerListStr);
      } else {
        logger.info(
            "[RESET PEER LIST] {} The current peer list is correct, nothing need to be reset: {}",
            groupId,
            previousPeerListStr);
      }
    }
  }

  public IoTConsensusServerImpl getImpl(ConsensusGroupId groupId) {
    return stateMachineMap.get(groupId);
  }

  public static String buildPeerDir(File storageDir, ConsensusGroupId groupId) {
    return storageDir + File.separator + groupId.getType().getValue() + "_" + groupId.getId();
  }
}
