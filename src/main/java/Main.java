import java.io.PrintStream;
import java.util.*;
import java.util.concurrent.*;

import co.paralleluniverse.fibers.*;
import co.paralleluniverse.strands.*;
import co.paralleluniverse.strands.channels.*;
import co.paralleluniverse.strands.concurrent.*;

public final class Main {
    private static final int RUNS = -1; // < 0 => unbounded
    private static final int CHANNEL_BUFFER = 0; // < 0 => unbounded
    private static final int MESSAGES_PER_PRODUCER = -1; // < 0 => unbounded

    private static final int PRODUCERS = 128;

    private static final long PROD_SLEEP_MS = 0;
    private static final long CONS_SLEEP_MS = 0;

    private static final long CONS_SELECT_TIMEOUT_MS = 1000;

    private static final int DBG = 0, INF = 1, WRN = 2, ERR = 3;

    private static final int LOG_LEVEL = DBG;

    private static ReentrantLock l = new ReentrantLock();
    @Suspendable
    private static void l(int lvl, String format, Object... args) {
        if (lvl >= LOG_LEVEL) {
            l.lock();
            try {
                final PrintStream e = System.err;
                e.print("[" + Strand.currentStrand().getName() + "] ");
                e.printf(format, args);
                e.println();
                e.flush();
            } finally {
                l.unlock();
            }
        }
    }

    public static void main(String[] args) throws ExecutionException, InterruptedException {
        for (int k = 0 ; RUNS < 0 || k < RUNS ; k++) {
            System.err.println();
            l(INF, "STARTING RUN %d", k+1);
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
                                    l(DBG, "sleeping %dms before send", PROD_SLEEP_MS);
                                    Strand.sleep(PROD_SLEEP_MS);
                                }
                                final String s = Strand.currentStrand().getName() + ": '" + Integer.toString(j) + "'";
                                l(DBG, "sending: \"%s\"", s);
                                //noinspection unchecked
                                chs[iF].send(s);
                                l(DBG, "sent: \"%s\"", s);
                            }
                        } catch (final SuspendExecution | InterruptedException e) {
                            l(ERR, "!!! caught %s with msg '%s', retrowing as assert failure (trace follows)", e.getClass().getName(), e.getMessage());
                            e.printStackTrace(System.err);
                            throw new AssertionError(e);
                        } finally {
                            l(DBG, "closing channel");
                            chs[iF].close();
                            l(DBG, "exiting");
                        }
                    }
                }));
                final String n = "prod" + Integer.toString(i);
                l(DBG, "starting \"%s\"", n);
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
                            l(DBG, "building select with open channels");
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
                            l(DBG, "added channels %s", added.toString());

                            if (sas.size() == 0) {
                                l(INF, "all channels closed, exiting");
                                return;
                            }

                            //noinspection ConstantConditions
                            if (CONS_SLEEP_MS > 0) {
                                l(DBG, "sleeping %dms before select", CONS_SLEEP_MS);
                                Strand.sleep(CONS_SLEEP_MS);
                            }
                            l(DBG, "selecting with %dms timeout", CONS_SELECT_TIMEOUT_MS);
                            final SelectAction m = Selector.select(CONS_SELECT_TIMEOUT_MS, TimeUnit.MILLISECONDS, sas);
                            sas.clear();

                            if (m == null) {
                                l(INF, "select timed out, exiting");
                                return;
                            }

                            final Object msg = m.message();
                            if (msg != null) {
                                l(DBG, "select returned: \"%s\"", msg);
                            } else {
                                if (m.port() != null) {
                                    final Port p = m.port();
                                    if (m.port() instanceof ReceivePort) {
                                        final ReceivePort rp = (ReceivePort) p;
                                        if (rp.isClosed()) {
                                            l(DBG, "select returned `null` from closed receive channel with index %d in the list, excluding", m.index());
                                            done.add(rp);
                                        } else
                                            l(ERR, "!!!ERROR: select returned `null` from OPEN receive channel with index %d in the list!!!", m.index());
                                    } else {
                                        l(ERR, "!!!ERROR: select returned `null` from non-receive channel with index %d in the list!!!", m.index());
                                    }
                                } else {
                                    l(ERR, "!!!ERROR: select returned `null` from `null` channel with index %d in the list!!!", m.index());
                                }
                            }
                        }
                    } catch (final SuspendExecution | InterruptedException e) {
                        l(ERR, "!!! caught %s with msg '%s', re-trowing as assert failure (trace follows)", e.getClass().getName(), e.getMessage());
                        e.printStackTrace(System.err);
                        throw new AssertionError(e);
                    }
                }
            }));

            final String n = "cons";
            l(DBG, "starting \"%s\"", n);
            dst.setName(n);
            dst.start();

            for (int i = 0; i < PRODUCERS; i++) {
                final Strand s = prod[i];
                final String np = s.getName();
                l(DBG, "joining \"%s\"", np);
                l(DBG, "joined \"%s\"", np);
                s.join();
            }
            l(DBG, "joining \"%s\"", n);
            dst.join();
            l(DBG, "joined \"%s\"", n);
        }
    }
}
