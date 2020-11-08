package github.tcw.ibsen.client;

import com.google.protobuf.ByteString;
import github.com.tcw.ibsen.TopicStatus;
import github.com.tcw.ibsen.TopicsStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;

import static com.google.protobuf.ByteString.copyFromUtf8;
import static org.junit.jupiter.api.Assertions.*;

@Testcontainers
class IbsenClientTest {

    public static final int BATCHES = 100;
    public static final int ENTRIES_IN_BATCH = 1000;

    private IbsenClient client;

    @Container
    GenericContainer<?> ibsenContainer = new GenericContainer<>(DockerImageName.parse("ibsen"))
            .withExposedPorts(50001)
            .withLogConsumer(outputFrame -> System.out.print(outputFrame.getUtf8String()))
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Ibsen grpc server started.*"));

    @BeforeEach
    public void setUp() {
        String address = ibsenContainer.getHost();
        Integer port = ibsenContainer.getMappedPort(50001);

        client = new IbsenClient(address, port);
    }

    @Test
    void createTopic() {
        boolean isCreated = client.create("test");
        assertTrue(isCreated);
    }

    @Test
    void writeEntriesToTopic() {
        client.create("test");
        long wrote = client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(3, wrote);
    }

    @Test
    void readEntriesFromTopic() {
        client.create("test");
        long wrote = client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(3, wrote);
        Iterator<IbsenClient.Entry> iterator = client.read("test", 0, 10);
        IbsenClient.Entry firstEntry = iterator.next();
        assertEquals(1, firstEntry.getOffset());
        assertEquals(copyFromUtf8("entry1"), firstEntry.getEntry());

        IbsenClient.Entry secondEntry = iterator.next();
        assertEquals(2, secondEntry.getOffset());
        assertEquals(copyFromUtf8("entry2"), secondEntry.getEntry());

        IbsenClient.Entry thirdEntry = iterator.next();
        assertEquals(3, thirdEntry.getOffset());
        assertEquals(copyFromUtf8("entry3"), thirdEntry.getEntry());

        assertFalse(iterator.hasNext());
    }

    @Test
    void statusFrom2Topics() {
        assertTrue(client.create("test1"));
        long wrote = client.write("test1",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(wrote, 3L);
        assertTrue(client.create("test2"));
        wrote = client.write("test2",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2")));
        assertEquals(wrote, 2L);
        TopicsStatus status = client.status();
        List<TopicStatus> topicStatusList = status.getTopicStatusList();
        TopicStatus topicStatus = topicStatusList.get(0);
        assertEquals(topicStatus.getBlocks(), 1);
    }

    @Test
    void readEntriesFromTopicWithOffsetNot0() {
        client.create("test");
        long wrote = client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(3, wrote);
        Iterator<IbsenClient.Entry> iterator = client.read("test", 1, 10);

        IbsenClient.Entry secondEntry = iterator.next();
        assertEquals(2, secondEntry.getOffset());
        assertEquals(copyFromUtf8("entry2"), secondEntry.getEntry());

        IbsenClient.Entry thirdEntry = iterator.next();
        assertEquals(3, thirdEntry.getOffset());
        assertEquals(copyFromUtf8("entry3"), thirdEntry.getEntry());

        assertFalse(iterator.hasNext());
    }

    @Test
    void readMultipleBatchesOfEntriesFromTopic() {
        client.create("test");
        int entrySeq = 1;
        for (int i = 0; i < BATCHES; i++) {
            List<ByteString> batch = new ArrayList<>();
            for (int j = 0; j < ENTRIES_IN_BATCH; j++) {
                batch.add(copyFromUtf8("entry-" + entrySeq++));
            }
            client.write("test", batch);
        }
        Iterator<IbsenClient.Entry> iterator = client.read("test", 0, 1000);
        for (int i = 1; i < (BATCHES * ENTRIES_IN_BATCH) + 1; i++) {
            IbsenClient.Entry entry = iterator.next();
            assertEquals(entry.getOffset(), i);
            assertEquals(entry.getEntry(), copyFromUtf8("entry-" + i));
        }
    }


    @Test
    void dropTopic() {
        client.create("test");
        long wrote = client.write("test",
                List.of(copyFromUtf8("entry1"), copyFromUtf8("entry2"), copyFromUtf8("entry3")));
        assertEquals(wrote, 3L);
        Iterator<IbsenClient.Entry> iterator = client.read("test", 0, 10);
        assertTrue(iterator.hasNext());
        boolean isDropped = client.drop("test");
        assertTrue(isDropped);
    }
}