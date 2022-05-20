package stream.client;

import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import stream.client.models.lombok.ShardGroup;
import stream.models.proto.record.RecordOuterClass.Record;
import stream.models.proto.responses.PublishResponseOuterClass.PublishResponse;
import streamInfra.client.InfraClient;
import streamInfra.models.proto.responses.GetStateResponseOuterClass;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Collectors;

@Slf4j
public class Publisher {
    InfraClient infraClient;
    Map<String, ShardGroup> shardGroupMap;
    Map<String, GetStateResponseOuterClass.Topic> topicMap;
    ScheduledExecutorService stateRefresher;
    AtomicLong index;

    public Publisher(RaftClient raftClient) {
        index = new AtomicLong(0);
        infraClient = new InfraClient(raftClient);
        var res = infraClient.getState(0);
        var state = res.getState();
        index.set(state.getIndex());
        topicMap = new ConcurrentHashMap<>(state.getTopicsMap());
        shardGroupMap = new ConcurrentHashMap<>();
        state.getShardGroupsMap().forEach((k, v) -> {
            addShardGroup(v);
        });
        stateRefresher = new ScheduledThreadPoolExecutor(1);
        stateRefresher.scheduleAtFixedRate(() -> {
            var response = infraClient.getState(index.get());
            if (response.hasIndex()) {
                log.info("State is the same, no need to update");
                return;
            }
            //state has been updated
            var newState = response.getState();
            updateState(newState);
        }, 1, 1, TimeUnit.MINUTES);
    }

    private void updateState(GetStateResponseOuterClass.State newState) {
        var shardGroups = newState.getShardGroupsMap();
        shardGroups.forEach((k, v) -> {
            if (!shardGroupMap.containsKey(k)) {
                addShardGroup(v);
                return;
            }
            var curGroup = shardGroupMap.get(k);
            if (v.getLastIndex() > curGroup.getShardGroup().get().getLastIndex()) {
                curGroup.getShardGroup().set(v);
            }
        });
        var topics = newState.getTopicsMap();
        topics.forEach((k, v) -> {
            if (!topicMap.containsKey(k)) {
                topicMap.put(k, v);
                return;
            }
            var curTopic = topicMap.get(k);
            if (v.getLastIndex() > curTopic.getLastIndex()) {
                topicMap.put(k, v);
            }
        });
        index.set(newState.getIndex());
    }

    private void addShardGroup(GetStateResponseOuterClass.ShardGroup shardGroup) {
        var group = ShardGroup.builder();
        var peers = shardGroup.getPeersMap().entrySet()
                .stream().map((e) ->
                        RaftPeer.newBuilder()
                                .setId(e.getKey())
                                .setAddress(e.getValue())
                                .build()
                ).collect(Collectors.toList());
        group.shardGroup(new AtomicReference<>(shardGroup));
        group.producerClient(getClient(RaftGroupId.valueOf(shardGroup.getGroupIdBytes()), peers));
        shardGroupMap.put(shardGroup.getGroupId(), group.build());
    }

    protected ProducerClient getClient(RaftGroupId raftGroupId, List<RaftPeer> peers) {
        var raftProperties = getRaftProperties();
        RaftClient.Builder builder =
                RaftClient.newBuilder().setProperties(raftProperties);
        var raftGroup = RaftGroup.valueOf(raftGroupId, peers);
        builder.setRaftGroup(raftGroup);
        builder.setClientRpc(
                new GrpcFactory(new org.apache.ratis.conf.Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        RaftClient client = builder.build();
        return new ProducerClient(client);
    }

    private RaftProperties getRaftProperties() {
        int raftSegmentPreallocateSize = 1024 * 1024 * 1024;
        RaftProperties raftProperties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
        GrpcConfigKeys.setMessageSizeMax(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocateSize));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocateSize));
        RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocateSize));
        RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocateSize));
        RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
                SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
        RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

        RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

        RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
                TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);
        return raftProperties;
    }


    public List<PublishResponse> publish(List<Record> records, String topic) {
        var curTopic = topicMap.get(topic);
        if (curTopic == null) {
            return null;
        }
        var partitions = curTopic.getNumPartition();
        var list = ImmutableList.<PublishResponse>builder();
        records.forEach((v) -> {
            //todo: implement consistent hashing
            var shardKey = v.getKey().hashCode() % partitions;
            var node = curTopic.getDistributionsOrThrow(shardKey);
            var shard = shardGroupMap.get(node);
            try {
                list.add(shard.getProducerClient().publish(List.of(v), topic));
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
        return list.build();
    }
}
