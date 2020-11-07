package github.tcw.ibsen.client;

import com.google.protobuf.ByteString;
import github.com.tcw.ibsen.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Iterator;
import java.util.List;

public class IbsenClient {

    private final String host;
    private final int port;
    private final IbsenGrpc.IbsenBlockingStub blockingStub;
    private final IbsenGrpc.IbsenStub asyncStub;

    public IbsenClient(String host, int port) {
        this.host = host;
        this.port = port;
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = IbsenGrpc.newBlockingStub(channel);
        asyncStub = IbsenGrpc.newStub(channel);
    }

    public void createTopic(String topic) {
        Topic request = Topic.newBuilder().setName(topic).build();
        CreateStatus createStatus = blockingStub.create(request);
        if (!createStatus.getCreated()) {
            throw new IllegalStateException("Unable to create topic " + topic);
        }
    }

    public long write(String topic, List<ByteString> entries) {
        InputEntries inputEntries = InputEntries.newBuilder().setTopic(topic).addAllEntries(entries).build();
        WriteStatus writeStatus = blockingStub.write(inputEntries);
        return writeStatus.getWrote();
    }

    public Iterator<Entry> read(String topic, long offset, int batchSize) {
        ReadParams readParams = ReadParams.newBuilder().setTopic(topic)
                .setOffset(offset)
                .setBatchSize(batchSize)
                .build();

        return new EntryIterator<>(blockingStub.read(readParams));
    }

    public static class EntryIterator<Entry> implements Iterator<IbsenClient.Entry> {

        private final Iterator<OutputEntries> iterator;
        private Iterator<ByteString> batchIterator;
        private long currentOffset;

        EntryIterator(Iterator<OutputEntries> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() && batchIterator.hasNext();
        }

        @Override
        public IbsenClient.Entry next() {

            if (batchIterator == null || !batchIterator.hasNext()) {
                if (iterator.hasNext()) {
                    OutputEntries batch = iterator.next();
                    batchIterator = batch.getEntriesList().iterator();
                    long batchSize = batch.getEntriesList().size();
                    currentOffset = batch.getOffset() - batchSize;
                }
            }
            return new IbsenClient.Entry(currentOffset++, batchIterator.next());
        }
    }

    public static class Entry {
        private final long offset;
        private final ByteString entry;

        public Entry(long offset, ByteString entry) {
            this.offset = offset;
            this.entry = entry;
        }

        public long getOffset() {
            return offset;
        }

        public ByteString getEntry() {
            return entry;
        }
    }

}
