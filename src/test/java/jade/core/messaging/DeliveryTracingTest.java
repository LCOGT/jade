package jade.core.messaging;

import jade.core.AID;
import jade.core.ProfileImpl;
import jade.domain.FIPAAgentManagement.InternalError;
import org.junit.Test;

import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.logging.Handler;
import java.util.logging.LogRecord;
import java.util.logging.Logger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

public class DeliveryTracingTest {
    @Test
    public void slowDeliveryWarningsUseOnlyTheCurrentTraceAfterAFailure() throws Exception {
        var warnings = new LinkedBlockingQueue<String>();
        Logger logger = Logger.getLogger(MessageManager.class.getName());
        Handler handler = new Handler() {
            @Override
            public void publish(LogRecord record) {
                if (record.getMessage().contains("Delivery-time over threshold")) {
                    warnings.add(record.getMessage());
                }
            }

            @Override
            public void flush() {
            }

            @Override
            public void close() {
            }
        };
        logger.addHandler(handler);

        ProfileImpl profile = new ProfileImpl();
        profile.setParameter("jade_core_messaging_MessageManager_poolsize", "1");
        profile.setParameter("jade_core_messaging_MessageManager_deliverytimethreshold", "1");
        MessageManager manager = MessageManager.instance(profile);
        AtomicInteger failures = new AtomicInteger();
        MessageManager.Channel channel = new MessageManager.Channel() {
            @Override
            public void deliverNow(GenericMessage message, AID receiver) {
                try {
                    Thread.sleep(20);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
                if (receiver.getLocalName().equals("first")) {
                    DeliveryTracing.setTracingInfo("Target-node", "first-node");
                    throw new IllegalStateException("simulated failed delivery");
                }
                DeliveryTracing.setTracingInfo("Reused-connection", false);
            }

            @Override
            public void notifyFailureToSender(GenericMessage message, AID receiver, InternalError error) {
                failures.incrementAndGet();
            }
        };

        try {
            manager.deliver(new GenericMessage(), new AID("first@test", AID.ISGUID), channel);
            String first = warnings.poll(5, TimeUnit.SECONDS);
            assertNotNull(first);
            assertTrue(first, first.contains("Receiver = first,") && first.endsWith("Delivery details: {Target-node=first-node}"));
            assertEquals(1, failures.get());

            manager.deliver(new GenericMessage(), new AID("second@test", AID.ISGUID), channel);
            String second = warnings.poll(5, TimeUnit.SECONDS);
            assertNotNull(second);
            assertTrue(second, second.contains("Receiver = second,") && second.endsWith("Delivery details: {Reused-connection=false}"));
            assertEquals(1, failures.get());
        }
        finally {
            logger.removeHandler(handler);
            // Normally invoked through the runtime's last-container callback.
            var shutdown = MessageManager.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(manager);
            jade.core.Runtime.instance().resetTerminators();
            manager.getThreadPool()[0].join(TimeUnit.SECONDS.toMillis(5));
        }
    }
}
