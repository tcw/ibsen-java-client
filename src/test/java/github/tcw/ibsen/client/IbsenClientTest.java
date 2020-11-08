package github.tcw.ibsen.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

@Testcontainers
class IbsenClientTest {

    private IbsenClient client;

    @Container
    GenericContainer<?> ibsenContainer = new GenericContainer<>(DockerImageName.parse("ibsen"))
            .withExposedPorts(50001)
            .waitingFor(new LogMessageWaitStrategy().withRegEx(".*Ibsen grpc server started.*"));

    @BeforeEach
    public void setUp() {
        String address = ibsenContainer.getHost();
        Integer port = ibsenContainer.getMappedPort(50001);

        client = new IbsenClient(address, port);
    }

    @AfterEach
    void tearDown() {
        ibsenContainer.stop();
    }

    @Test
    void createTopic() {
        client.createTopic("test");
    }
}