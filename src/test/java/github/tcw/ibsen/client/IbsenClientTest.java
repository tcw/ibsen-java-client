package github.tcw.ibsen.client;

import github.com.tcw.ibsen.Entry;
import github.com.tcw.ibsen.OutputEntries;
import io.grpc.stub.StreamObserver;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.List;
import java.util.concurrent.SynchronousQueue;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.jupiter.api.Assertions.assertEquals;

@Testcontainers
class IbsenClientTest {

    public static final int BATCHES = 100;
    public static final int ENTRIES_IN_BATCH = 1000;

    private IbsenClient client;

    @Container
    GenericContainer<?> ibsenContainer = new GenericContainer<>(DockerImageName.parse("ibsen"))
            .withExposedPorts(50001)
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
            .withEnv("IBSEN_IN_MEMORY_ONLY", "true")
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Ibsen grpc server started.*"));

    @BeforeEach
    public void setUp() {
        String address = ibsenContainer.getHost();
        Integer port = ibsenContainer.getMappedPort(50001);

        client = new IbsenClient(address, port);
    }

    @Test
    void writeEntriesToTopic() {
        long totalBytesWritten = client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(78, totalBytesWritten);
    }

    @Test
    void readEntriesFromTopic() throws InterruptedException {
        client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        SynchronousQueue<List<Entry>> queue = new SynchronousQueue<>();
        client.read("test", new StreamObserver<>() {
            @Override
            public void onNext(OutputEntries value) {
                queue.offer(value.getEntriesList());
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });

        List<Entry> entries = queue.take();
        assertEquals(3, entries.size());
        assertEquals(copyFromUtf8("entry1"), entries.get(0).getContent());
        assertEquals(copyFromUtf8("entry2"), entries.get(1).getContent());
        assertEquals(copyFromUtf8("entry3"), entries.get(2).getContent());
    }

    @Test
    void statusFrom2Topics() {
        client.write("test1",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        client.write("test2",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2")));
        List<String> topics = client.topics();
        Assertions.assertEquals(2, topics.size());
        Assertions.assertTrue(topics.contains("test1"));
        Assertions.assertTrue(topics.contains("test2"));
    }

    @Test
    void readEntriesFromTopicWithOffsetNot0() throws InterruptedException {
        client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        SynchronousQueue<List<Entry>> queue = new SynchronousQueue<>();
        client.read("test", 1, new StreamObserver<>() {
            @Override
            public void onNext(OutputEntries value) {
                queue.offer(value.getEntriesList());
            }

            @Override
            public void onError(Throwable t) {
            }

            @Override
            public void onCompleted() {
            }
        });

        List<Entry> entries = queue.take();
        assertEquals(2, entries.size());
        assertEquals(copyFromUtf8("entry2"), entries.get(0).getContent());
        assertEquals(copyFromUtf8("entry3"), entries.get(1).getContent());

    }

}