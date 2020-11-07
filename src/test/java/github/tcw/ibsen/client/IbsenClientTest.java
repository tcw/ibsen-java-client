package github.tcw.ibsen.client;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.output.OutputFrame;
import org.testcontainers.containers.wait.strategy.HostPortWaitStrategy;
import org.testcontainers.containers.wait.strategy.LogMessageWaitStrategy;
import org.testcontainers.containers.wait.strategy.WaitAllStrategy;
import org.testcontainers.containers.wait.strategy.WaitStrategy;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.util.function.Consumer;

@Testcontainers
class IbsenClientTest {

    private IbsenClient client;
    GenericContainer<?> ibsenContainer;


    @BeforeEach
    public void setUp() {
        ibsenContainer = new GenericContainer<>(DockerImageName.parse("ibsen"))
                .withExposedPorts(50001)
                .withLogConsumer(new Consumer<OutputFrame>() {
                    @Override
                    public void accept(OutputFrame outputFrame) {
                        System.out.println(outputFrame.getUtf8String());
                    }
                })
                .waitingFor(new HostPortWaitStrategy());
        ibsenContainer.start();
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