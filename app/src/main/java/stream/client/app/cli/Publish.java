package stream.client.app.cli;

import com.beust.jcommander.Parameter;
import com.beust.jcommander.Parameters;
import com.google.common.collect.ImmutableList;
import lombok.extern.slf4j.Slf4j;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import stream.client.Publisher;
import stream.models.proto.record.RecordOuterClass;

import java.util.InputMismatchException;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * Subcommand to generate load in filestore state machine.
 */
@Parameters(commandDescription = "Producer cli for streams")
@Slf4j
public class Publish extends SubCommandBase {
    @Parameter(names = {"--messages", "--m"}, description = "messages to be published", required = true)
    private List<String> messages = null;

    @Parameter(names = {"--topic", "--t"}, description = "Topic to publish to", required = true)
    private String topic = null;

    @Override
    public void run() throws Exception {
        int raftSegmentPreallocatedSize = 1024 * 1024 * 1024;
        RaftProperties raftProperties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(raftProperties, SupportedRpcType.GRPC);
        GrpcConfigKeys.setMessageSizeMax(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setWriteBufferSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setPreallocatedSize(raftProperties,
                SizeInBytes.valueOf(raftSegmentPreallocatedSize));
        RaftServerConfigKeys.Log.setSegmentSizeMax(raftProperties,
                SizeInBytes.valueOf(1 * 1024 * 1024 * 1024L));
        RaftConfigKeys.DataStream.setType(raftProperties, SupportedDataStreamType.NETTY);

        RaftServerConfigKeys.Log.setSegmentCacheNumMax(raftProperties, 2);

        RaftClientConfigKeys.Rpc.setRequestTimeout(raftProperties,
                TimeDuration.valueOf(50000, TimeUnit.MILLISECONDS));
        RaftClientConfigKeys.Async.setOutstandingRequestsMax(raftProperties, 1000);

        operation(getClient(raftProperties));
    }

    protected void operation(Publisher client){
        client.publish(parseRecords(messages), topic);
    }

    private List<RecordOuterClass.Record> parseRecords(List<String> input) {
        var list = ImmutableList.<RecordOuterClass.Record>builder();
        input.forEach((val) -> {
            var keyVal = val.split(",");
            if(keyVal.length != 2){
                throw new InputMismatchException();
            }
            var record = RecordOuterClass.Record.newBuilder()
                    .setKey(keyVal[0])
                    .setPayload(ByteString.copyFromUtf8(keyVal[1]))
                    .build();
            list.add(record);
        });
        return list.build();
    }

    protected Publisher getClient(RaftProperties raftProperties) {
        final RaftGroup raftGroup = RaftGroup.valueOf(RaftGroupId.valueOf(ByteString.copyFromUtf8(getRaftGroupId())),
                getPeers());

        RaftClient.Builder builder =
                RaftClient.newBuilder().setProperties(raftProperties);
        builder.setRaftGroup(raftGroup);
        builder.setClientRpc(
                new GrpcFactory(new org.apache.ratis.conf.Parameters())
                        .newRaftClientRpc(ClientId.randomId(), raftProperties));
        RaftPeer[] peers = getPeers();
        builder.setPrimaryDataStreamServer(peers[0]);
        RaftClient client = builder.build();
        return new Publisher(client);
    }
}
