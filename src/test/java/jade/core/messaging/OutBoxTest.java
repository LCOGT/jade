package jade.core.messaging;

import jade.core.AID;
import jade.core.messaging.MessageManager.PendingMsg;
import jade.core.ProfileImpl;
import jade.domain.FIPAAgentManagement.InternalError;
import org.junit.After;
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
    private final MessageManager manager = createManager();
    private final CountDownLatch releaseDelivery = new CountDownLatch(1);

    @Test(timeout = 15_000)
    public void servesAnotherRecipientWhileOneIsBusyAndResumesTheBusyRecipientInOrder() throws Exception {
        OutBox queue = new OutBox(10_000, 100_000, -1, true, manager);
        AID firstRecipient = recipient("first");
        AID secondRecipient = recipient("second");
        GenericMessage next = new GenericMessage();
        GenericMessage last = new GenericMessage();
        GenericMessage independent = new GenericMessage();
        queue.addLast(firstRecipient, new GenericMessage(), null);
        queue.get(); // A worker has taken the first message and still owns this recipient.
        queue.addLast(firstRecipient, next, null);
        queue.addLast(firstRecipient, last, null);

        FutureTask<PendingMsg> otherDelivery = waitingDelivery(queue);
        queue.addLast(secondRecipient, independent, null);
        assertSame(independent, otherDelivery.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
        queue.handleServed(secondRecipient, 1);

        FutureTask<PendingMsg> resumed = waitingDelivery(queue);
        queue.handleServed(firstRecipient, 1);
        assertSame(next, resumed.get(TIMEOUT_SECONDS, TimeUnit.SECONDS).getMessage());
        queue.handleServed(firstRecipient, 1);
        assertSame(last, queue.get().getMessage());
    }

    @Test
    public void shutsDownWhenAllWorkersAreWaiting() throws Exception {
        for (Thread worker : manager.getThreadPool()) {
            awaitWaiting(worker);
        }
    }

    @Test
    public void shutsDownWithAnActiveDeliveryAndAPendingMessage() throws Exception {
        CountDownLatch delivering = new CountDownLatch(1);
        MessageManager.Channel channel = new MessageManager.Channel() {
            @Override
            public void deliverNow(GenericMessage message, AID receiver) {
                delivering.countDown();
                try {
                    releaseDelivery.await(TIMEOUT_SECONDS, TimeUnit.SECONDS);
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
        AID receiver = recipient("blocked");
        manager.deliver(new GenericMessage(), receiver, channel);
        assertTrue(delivering.await(TIMEOUT_SECONDS, TimeUnit.SECONDS));
        manager.deliver(new GenericMessage(), receiver, channel);
        assertEquals(1, manager.getPendingCnt());
    }

    private static FutureTask<PendingMsg> waitingDelivery(OutBox queue) throws InterruptedException {
        FutureTask<PendingMsg> delivery = new FutureTask<>(queue::get);
        Thread worker = new Thread(delivery);
        worker.setDaemon(true);
        worker.start();
        awaitWaiting(worker);
        return delivery;
    }

    private static void awaitWaiting(Thread worker) throws InterruptedException {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(TIMEOUT_SECONDS);
        while (worker.getState() != Thread.State.WAITING && System.nanoTime() < deadline) {
            Thread.sleep(1);
        }
        assertEquals(Thread.State.WAITING, worker.getState());
    }

    @After
    public void shutDownAndCheckWorkersStopped() throws Exception {
        try {
            // The runtime normally invokes this callback when its last container terminates.
            var shutdown = MessageManager.class.getDeclaredMethod("shutdown");
            shutdown.setAccessible(true);
            shutdown.invoke(manager);
            jade.core.Runtime.instance().resetTerminators();
        }
        finally {
            releaseDelivery.countDown();
        }
        for (Thread worker : manager.getThreadPool()) {
            worker.join(TimeUnit.SECONDS.toMillis(TIMEOUT_SECONDS));
            assertFalse(worker.getName() + " did not stop", worker.isAlive());
        }
    }

    private static MessageManager createManager() {
        ProfileImpl profile = new ProfileImpl();
        profile.setParameter("jade_core_messaging_MessageManager_poolsize", "4");
        return MessageManager.instance(profile);
    }

    private static AID recipient(String name) {
        return new AID(name + "@test", AID.ISGUID);
    }
}
