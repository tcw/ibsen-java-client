package github.tcw.ibsen.client;

import com.google.protobuf.ByteString;
import github.com.tcw.ibsen.*;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

import java.util.Collections;
import java.util.Iterator;
import java.util.List;

public class IbsenClient {

    private final IbsenGrpc.IbsenBlockingStub blockingStub;
    //private final IbsenGrpc.IbsenStub asyncStub;

    public IbsenClient(String host, int port) {
        ManagedChannelBuilder<?> channelBuilder = ManagedChannelBuilder
                .forAddress(host, port)
                .usePlaintext();
        ManagedChannel channel = channelBuilder.build();
        blockingStub = IbsenGrpc.newBlockingStub(channel);
        // asyncStub = IbsenGrpc.newStub(channel); Todo: for testing streaming write
    }

    public boolean create(String topic) {
        Topic request = Topic.newBuilder().setName(topic).build();
        return blockingStub.create(request).getCreated();
    }

    public boolean drop(String topic) {
        Topic request = Topic.newBuilder().setName(topic).build();
        return blockingStub.drop(request).getDropped();
    }

    public TopicsStatus status() {
        Empty empty = Empty.newBuilder().build();
        return blockingStub.status(empty);
    }

    public long write(String topic, List<ByteString> entries) {
        InputEntries inputEntries = InputEntries.newBuilder().setTopic(topic).addAllEntries(entries).build();
        WriteStatus writeStatus = blockingStub.write(inputEntries);
        return writeStatus.getWrote();
    }

    public Iterator<Entry> read(String topic) {
        return read(topic, 0, 1000);
    }

    public Iterator<Entry> read(String topic, long offset) {
        return read(topic, offset, 1000);
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
        private Iterator<github.com.tcw.ibsen.Entry> batchIterator = Collections.emptyIterator();

        EntryIterator(Iterator<OutputEntries> iterator) {
            this.iterator = iterator;
        }

        @Override
        public boolean hasNext() {
            return iterator.hasNext() || batchIterator.hasNext();
        }

        @Override
        public IbsenClient.Entry next() {
            if (!batchIterator.hasNext()) {
                if (iterator.hasNext()) {
                    OutputEntries batch = iterator.next();
                    batchIterator = batch.getEntriesList().iterator();
                }
            }
            github.com.tcw.ibsen.Entry entry = batchIterator.next();
            return new IbsenClient.Entry(entry.getOffset(),entry.getContent());
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
