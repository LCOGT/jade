package jade.core.messaging;

import jade.core.AID;
import jade.core.ProfileImpl;
import jade.domain.FIPAAgentManagement.InternalError;
import jade.core.messaging.MessageManager.PendingMsg;
import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;

public class OutBoxTest {
    private static final int TIMEOUT_SECONDS = 5;

    @Test
    public void servesAnotherRecipientWhileOneIsBusyAndResumesTheBusyRecipientInOrder() throws Exception {
        MessageManager manager = MessageManager.instance(profile());
        OutBox queue = new OutBox(10_000, 100_000, -1, true, manager);
        try {
            AID firstRecipient = recipient("first");
            AID secondRecipient = recipient("second");
            GenericMessage first = new GenericMessage();
            GenericMessage next = new GenericMessage();
            GenericMessage last = new GenericMessage();
            GenericMessage independent = new GenericMessage();
            queue.addLast(firstRecipient, first, null);
            assertSame(first, nextDelivery(queue).get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
            queue.addLast(firstRecipient, next, null);
            queue.addLast(firstRecipient, last, null);

            FutureTask<PendingMsg> otherDelivery = waitingDelivery(queue);
            queue.addLast(secondRecipient, independent, null);
            assertSame(independent, otherDelivery.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
            queue.handleServed(secondRecipient, 1);

            FutureTask<PendingMsg> resumed = waitingDelivery(queue);
            queue.handleServed(firstRecipient, 1);
            assertSame(next, resumed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
            FutureTask<PendingMsg> finalDelivery = waitingDelivery(queue);
            queue.handleServed(firstRecipient, 1);
            assertSame(last, finalDelivery.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
            queue.handleServed(firstRecipient, 1);
            assertEquals(0, queue.getPendingCnt());
        }
        finally {
            shutdown(manager);
        }
    }

    @Test
    public void shutsDownWhenAllWorkersAreWaiting() throws Exception {
        MessageManager manager = MessageManager.instance(profile());
        Thread[] workers = manager.getThreadPool();
        try {
            for (Thread worker : workers) {
                awaitWaiting(worker);
            }
        }
        finally {
            shutdown(manager);
        }
        assertStopped(workers);
    }

    @Test
    public void shutsDownWithAnActiveDeliveryAndAPendingMessage() throws Exception {
        MessageManager manager = MessageManager.instance(profile());
        Thread[] workers = manager.getThreadPool();
        CountDownLatch delivering = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        MessageManager.Channel channel = new MessageManager.Channel() {
            @Override
            public void deliverNow(GenericMessage message, AID receiver) {
                delivering.countDown();
                try {
                    release.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
                }
                catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }

            @Override
            public void notifyFailureToSender(GenericMessage message, AID receiver, InternalError error) {
                throw new AssertionError(error);
            }
        };
        try {
            AID receiver = recipient("blocked");
            manager.deliver(new GenericMessage(), receiver, channel);
            assertTrue(delivering.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
            manager.deliver(new GenericMessage(), receiver, channel);
            assertEquals(1, manager.getPendingCnt());
        }
        finally {
            try {
                shutdown(manager);
            }
            finally {
                release.countDown();
            }
        }
        assertStopped(workers);
    }

    private static FutureTask<PendingMsg> nextDelivery(OutBox queue) {
        FutureTask<PendingMsg> delivery = new FutureTask<>(queue::get);
        Thread.ofPlatform().daemon().start(delivery);
        return delivery;
    }

    private static FutureTask<PendingMsg> waitingDelivery(OutBox queue) throws InterruptedException {
        FutureTask<PendingMsg> delivery = new FutureTask<>(queue::get);
        awaitWaiting(Thread.ofPlatform().daemon().start(delivery));
        return delivery;
    }

    private static void awaitWaiting(Thread worker) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (worker.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(Thread.State.WAITING, worker.getState());
    }

    private static void assertStopped(Thread[] workers) throws InterruptedException {
        for (Thread worker : workers) {
            worker.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse(worker.getName() + " did not stop", worker.isAlive());
        }
    }

    private static ProfileImpl profile() {
        ProfileImpl profile = new ProfileImpl();
        profile.setParameter("jade_core_messaging_MessageManager_poolsize", "4");
        return profile;
    }

    private static AID recipient(String name) {
        return new AID(name + "@test", AID.ISGUID);
    }

    private static void shutdown(MessageManager manager) throws ReflectiveOperationException {
        // The runtime normally invokes this callback when its last container terminates.
        var shutdown = MessageManager.class.getDeclaredMethod("shutdown");
        shutdown.setAccessible(true);
        shutdown.invoke(manager);
        jade.core.Runtime.instance().resetTerminators();
    }
}
