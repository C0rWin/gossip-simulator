package sim.gossip.hrl.il.ibm.com;

import java.io.ByteArrayOutputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.DoubleStream;
import java.util.stream.IntStream;
import java.util.stream.Stream;

public class Simulation implements Runnable {

	private static final String csvPattern = "%d,%d,%d,%d,%f,%d,%d,%d";

	private int maxTTL;
	private int h;
	private int k;
	private int n;
	private double a;
	private int lastRound;
	

	private PeerList peers;

	private int infectedCount;
	private Random rnd = new Random(System.currentTimeMillis());
	private int msgCount;
	private PrintWriter w;

	private static AtomicInteger activeJobs = new AtomicInteger(0);

	public Simulation(int h, int k, int n, double a, int ttl, OutputStream out) {
		if (k > n) {
			throw new RuntimeException("k(" + k + ") must be less than n(" + n + ")");
		}
		w = new PrintWriter(out);
		this.h = h;
		this.k = k;
		this.n = n;
		this.a = a;
		this.maxTTL = ttl;
		this.peers = new PeerList(n);
	}

	public void run() {
		try {
			lastRound = (int) (n * Math.log(n));
			// Some peer is infected at first
			int p = (int) (Math.random() * n);
			peers.infect(p);
			peers.ttl[p] = maxTTL;
			for (int i = 0; i < lastRound; i++) {
				round(i);
			}
			w.flush();
			w.close();
			activeJobs.decrementAndGet();
		} catch (Exception e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	private void round(int r) {
		if (r <= h) {
			forwardingPhase();
		} else {
			pullPhase();
		}
		peers.incrementRound(r);
		log(r);
	} // round

	private void forwardingPhase() {
		// First, figure out which peers don't have the message:
		Set<Integer> attackedPeers = peers.randomNoneInfectedPeers(a);
		Set<Integer> peersToBeInfected = new HashSet<>();
		// We still have peers that forward the message again because
		// their TTL isn't 0.
		// Start with them as actors from the previous round(s), but omit the ones that
		// are denied of service in this round.
		// Note: the peers that were infected in the previous round, are also included
		// here implicitly since their TTL is > 0.
		Set<Integer> actingPeers = peers.withRemainingTTL().filter(p -> {
			return ! attackedPeers.contains(p);
		}).collect(Collectors.toSet());
		// Now, have each acting peer disseminate to a random set of peers.
		actingPeers.forEach(p -> {
			// peer p forwards to q
			selectRandomPeers(p).forEach(q -> {
				if (attackedPeers.contains(q)) {
					return;
				}
				// q has yet to be infected
				if (!peers.infected[q]) {
					// It will be marked as infected at the end of this round.
					peersToBeInfected.add(q);
					infectedCount++;
					peers.ttl[q] = maxTTL;
				}
				peers.ttl[p]--;
			});
			msgCount += k;
		});
		peers.infect(peersToBeInfected);
	} // forwardingPhase

	private void pullPhase() {
		Set<Integer> attackedPeers = peers.randomNoneInfectedPeers(a);
		Set<Integer> peersToBeInfected = new HashSet<>();
		// Select peers that are not infected and are not denied of service.
		peers.notInfected.stream().filter(p -> {
			return (! attackedPeers.contains(p));
		}).forEach(p -> {
			// Select some peers that are not denied of service, to pull the message from
			boolean pullSucceeded = selectRandomPeers(p).filter(q -> !attackedPeers.contains(q))
					.anyMatch(q -> peers.infected[q]);
			msgCount += k;
			if (pullSucceeded) {
				peersToBeInfected.add(p);
				infectedCount++;
			}
		});
		peers.infect(peersToBeInfected);
	} // pullPhase

	private Stream<Integer> selectRandomPeers(int self) {
		List<Integer> peers = new ArrayList<>();
		// N >> k so it's ok not to check corner cases

		while (peers.size() < k) {
			int p = rnd.nextInt(n);
			if (peers.contains(p) || p == self) {
				continue;
			}
			peers.add(p);
		}
		return peers.stream();
	}

	private void log(int r) {
		w.println(String.format(csvPattern, r, h, k, n, a, maxTTL, infectedCount, msgCount));
	}

	private static void sleep(int seconds) {
		try {
			Thread.sleep(1000 * seconds);
		} catch (InterruptedException e) {
		}
	}

	public static void main(String[] args) {
		AtomicInteger iterations = new AtomicInteger();

		Vector<ByteArrayOutputStream> simResults = new Vector<ByteArrayOutputStream>();
		IntStream.range(1, 13000).forEach(i -> {
			simResults.add(new ByteArrayOutputStream());
		});

		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(90);

		IntStream.of(10000).parallel().forEach(n -> {
			DoubleStream.iterate(0, i -> i + 0.1).limit(5).forEach(a -> {
				int maxH = (int) Math.log(n);
				IntStream.range(1, maxH).parallel().forEach(h -> {
					IntStream.iterate(1, i -> i + 1).limit(10).forEach(k -> {
						if (k > n) {
							return;
						}
						IntStream.range(1, 2).parallel().forEach(ttl -> {
							Simulation sim = new Simulation(h, k, n, a, ttl,
									simResults.get(iterations.getAndIncrement()));
							activeJobs.incrementAndGet();
							pool.execute(sim);
						});
					});
				});
			});
		});

		while (true) {
			sleep(1);
			System.out.println(iterations.get() + "/" + 12960);
			System.out.println("Active jobs:" + activeJobs.get());
			if (activeJobs.get() == 0) {
				pool.shutdown();
				break;
			}
		}

		try {
			FileOutputStream fos = new FileOutputStream("sim.csv");
			for (ByteArrayOutputStream simRes : simResults) {
				simRes.writeTo(fos);
			}
			fos.flush();
			fos.close();
			System.exit(0);
		} catch (IOException e) {
			e.printStackTrace();
			System.exit(1);
		}
	}

	public static class PeerList {
		private int[] ttl;
		private boolean[] infected;												// Current state of the peer
		private Set<Integer> notInfected = new HashSet<>();						// Set of peers that are currently not infected
		private Set<Integer> adversaryNotInfectedView = new HashSet<>();		// Set of peers that are not infected as viewed by the adversary
		private LinkedList<Set<Integer>> infectedByRounds = new LinkedList<Set<Integer>>();	// Sets of peers that were infected in each round
		private Random rnd = new Random(System.currentTimeMillis());

		public PeerList(int n) {
			ttl = new int[n];
			infected = new boolean[n];
			IntStream.range(0, n).forEach(i -> {
				notInfected.add(i);
				adversaryNotInfectedView.add(i);
			});
			
			infectedByRounds.add(new HashSet<>());
		}

		public Set<Integer> randomNoneInfectedPeers(double epsilon) {
			List<Integer> notInfectedList = new ArrayList<>(adversaryNotInfectedView);
			Collections.shuffle(notInfectedList);
			return notInfectedList.stream().filter(p -> {
				return rnd.nextDouble() < epsilon;
			}).collect(Collectors.toSet());
		}

		public Stream<Integer> withRemainingTTL() {
			return Arrays.stream(ttl).boxed().filter(c -> {
				return c > 0;
			});
		}
		
		public void incrementRound(int round) {
			// In end of round 0 the adversary doesn't know anything 
			if (round > 0) {
				Set<Integer> infectedInLastRound = infectedByRounds.removeFirst();
				adversaryNotInfectedView.removeAll(infectedInLastRound);
			}
			infectedByRounds.add(new HashSet<>());
		}
		
		public void infect(Collection<Integer> peers) {
			peers.stream().forEach(p -> {
				infected[p] = true;
			});
			infectedByRounds.getLast().addAll(peers);
			notInfected.removeAll(peers);
		}

		public void infect(int p) {
			infected[p] = true;
			infectedByRounds.getLast().add(p);
			notInfected.remove(p);
		}
	}

}
