import java.util.*;
import java.util.concurrent.*;

import co.paralleluniverse.fibers.*;
import co.paralleluniverse.strands.*;
import co.paralleluniverse.strands.channels.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class Main {
    private static final int RUNS = -1; // < 0 => unbounded
    private static final int CHANNEL_BUFFER = 0; // < 0 => unbounded
    private static final int MESSAGES_PER_PRODUCER = -1; // < 0 => unbounded

    private static final int PRODUCERS = 128;

    private static final long PROD_SLEEP_MS = 0;
    private static final long CONS_SLEEP_MS = 0;

    private static final long CONS_SELECT_TIMEOUT_MS = 1000;

    private final static Logger log = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for (int k = 0 ; RUNS < 0 || k < RUNS ; k++) {
            System.err.println();
            log.info("STARTING RUN {}", k+1);
            final Channel[] chs = new Channel[PRODUCERS];
            final Strand[] prod = new Strand[PRODUCERS];
            for (int i = 0; i < PRODUCERS; i++) {
                chs[i] = Channels.newChannel(CHANNEL_BUFFER, Channels.OverflowPolicy.BLOCK, true, true);
                final int iF = i;
                prod[i] = Strand.of(new Thread(new Runnable() {
                    @Override
                    @Suspendable
                    public void run() {
                        try {
                            for (int j = 0; MESSAGES_PER_PRODUCER <= 0 || j < MESSAGES_PER_PRODUCER; j++) {
                                //noinspection ConstantConditions
                                if (PROD_SLEEP_MS > 0) {
                                    log.debug("sleeping {}ms before send", PROD_SLEEP_MS);
                                    Strand.sleep(PROD_SLEEP_MS);
                                }
                                final String s = Strand.currentStrand().getName() + ": '" + Integer.toString(j) + "'";
                                log.debug("sending: \"{}\"", s);
                                //noinspection unchecked
                                chs[iF].send(s);
                                log.debug("sent: \"{}\"", s);
                            }
                        } catch (final SuspendExecution | InterruptedException e) {
                            log.error("!!! caught {} with msg '{}', retrowing as assert failure (trace follows)", e.getClass().getName(), e.getMessage());
                            e.printStackTrace(System.err);
                            throw new AssertionError(e);
                        } finally {
                            log.debug("closing channel");
                            chs[iF].close();
                            log.debug("exiting");
                        }
                    }
                }));
                final String n = "prod" + Integer.toString(i);
                log.debug("starting \"{}\"", n);
                prod[i].setName(n);
                prod[i].start();
            }

            final Strand dst = Strand.of(new Thread(new Runnable() {
                @Override
                @Suspendable
                public void run() {
                    try {
                        final List<Port> done = new ArrayList<>(PRODUCERS);
                        while (true) {
                            log.debug("building select with open channels");
                            final StringBuilder added = new StringBuilder();
                            final List<SelectAction<Object>> sas = new ArrayList<>(PRODUCERS);

                            boolean first = true;
                            for (int i = 0; i < PRODUCERS; i++) {
                                if (!done.contains(chs[i])) {
                                    if (!first) {
                                        added.append(",");
                                    }
                                    first = false;
                                    //noinspection unchecked
                                    sas.add(Selector.receive(chs[i]));
                                    added.append(Integer.toString(i));
                                }
                            }
                            log.debug("added channels {}", added.toString());

                            if (sas.size() == 0) {
                                log.info("all channels closed, exiting");
                                return;
                            }

                            //noinspection ConstantConditions
                            if (CONS_SLEEP_MS > 0) {
                                log.debug("sleeping {}ms before select", CONS_SLEEP_MS);
                                Strand.sleep(CONS_SLEEP_MS);
                            }
                            log.debug("selecting with {}ms timeout", CONS_SELECT_TIMEOUT_MS);
                            final SelectAction m = Selector.select(CONS_SELECT_TIMEOUT_MS, TimeUnit.MILLISECONDS, sas);
                            sas.clear();

                            if (m == null) {
                                log.info("select timed out, exiting");
                                return;
                            }

                            final Object msg = m.message();
                            if (msg != null) {
                                log.debug("select returned: \"{}s\"", msg);
                            } else {
                                if (m.port() != null) {
                                    final Port p = m.port();
                                    if (m.port() instanceof ReceivePort) {
                                        final ReceivePort rp = (ReceivePort) p;
                                        if (rp.isClosed()) {
                                            log.debug("select returned `null` from closed receive channel with index {} in the list, excluding", m.index());
                                            done.add(rp);
                                        } else
                                            log.error("!!!ERROR: select returned `null` from OPEN receive channel with index {} in the list!!!", m.index());
                                    } else {
                                        log.error("!!!ERROR: select returned `null` from non-receive channel with index {} in the list!!!", m.index());
                                    }
                                } else {
                                    log.error("!!!ERROR: select returned `null` from `null` channel with index {} in the list!!!", m.index());
                                }
                            }
                        }
                    } catch (final SuspendExecution | InterruptedException e) {
                        log.error("!!! caught {} with msg '{}', re-trowing as assert failure (trace follows)", e.getClass().getName(), e.getMessage());
                        e.printStackTrace(System.err);
                        throw new AssertionError(e);
                    }
                }
            }));

            final String n = "cons";
            log.debug("starting \"{}\"", n);
            dst.setName(n);
            dst.start();

            for (int i = 0; i < PRODUCERS; i++) {
                final Strand s = prod[i];
                final String np = s.getName();
                log.debug("joining \"{}\"", np);
                log.debug("joined \"{}\"", np);
                s.join();
            }
            log.debug("joining \"{}\"", n);
            dst.join();
            log.debug("joined \"{}\"", n);
        }
    }
}
