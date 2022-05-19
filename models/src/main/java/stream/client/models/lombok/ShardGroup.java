package stream.client.models.lombok;

import lombok.Builder;
import lombok.Data;
import lombok.NonNull;
import stream.client.ConsumerClient;
import stream.client.ProducerClient;
import streamInfra.models.proto.responses.GetStateResponseOuterClass;

import java.util.concurrent.atomic.AtomicReference;

@Data
@Builder
public class ShardGroup {
    @NonNull
    AtomicReference<GetStateResponseOuterClass.ShardGroup> shardGroup;
    ProducerClient producerClient;
    ConsumerClient consumerClient;
}
