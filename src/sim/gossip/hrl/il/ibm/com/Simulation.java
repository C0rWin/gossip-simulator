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
import java.util.stream.IntStream;

public class Simulation implements Runnable {

	private static final String csvPattern = "%d,%d,%d,%d,%d,%d,%d,%d";

	private int maxTTL;
	private int h;
	private int k;
	private int n;
	private int a;
	private int lastRound;
	private boolean[] peers;
	private int[] ttl;
	private Set<Integer> newInfections = new HashSet<Integer>();
	private LinkedList<Integer> infectedPeers = new LinkedList<Integer>();
	private int infectedCount;
	private Random rnd = new Random(System.currentTimeMillis());
	private int msgCount;
	private PrintWriter w;
	
	private static AtomicInteger activeJobs = new AtomicInteger(0);

	public Simulation(int h, int k, int n, int a, int ttl, OutputStream out) {
		if (k > n) {
			throw new RuntimeException("k(" + k + ") must be less than n(" + n + ")");
		}
		w =  new PrintWriter(out);
		this.h = h;
		this.k = k;
		this.n = n;
		this.a = a;
		this.ttl = new int[n];
		this.maxTTL = ttl;
		this.peers = new boolean[n];
	}

	public void run() {
		lastRound = (int) (n * Math.log(n));
		// Some peer is infected at first
		infectedPeers.add((int) (Math.random() * n));
		for (int i = 0; i < lastRound; i++) {
			round(i);
		}
		w.flush();
		w.close();
		activeJobs.decrementAndGet();
	}
	
	private void filterOutInfectedPeers(Set<Integer> in) {
		Iterator<Integer> i = in.iterator();
		while (i.hasNext()) {
			int p = i.next();
			// If p has already been infected, remove it
			if (peers[p]) {
				i.remove();
			}
		}
	}

	private void denialOfService() {
		if (newInfections.size() <= a) {
			newInfections.clear();
			return;
		}

		// Compute non infected peers
		List<Integer> nonInfected = new ArrayList<>();
		IntStream.range(0, n).filter(p -> ! peers[p]).forEach(p -> nonInfected.add(p));
		Collections.shuffle(nonInfected);
		// Adversary selects a specified peers out of the non infected peers
		List<Integer> blocked = nonInfected.subList(0, a);
		// And blocks the peers from being infected in the next round
		newInfections.removeAll(blocked);
	}

	private void round(int r) {
		if (r <= h) {
			forwardingPhase();
		} else {
			pullPhase();
		}
		log(r);
	} // round

	private void forwardingPhase() {
		while (!infectedPeers.isEmpty()) {
			int p = infectedPeers.removeFirst();
			// If it's the first infection of p:
			if (! peers[p]) {
				peers[p] = true;
				infectedCount++;
				ttl[p] = maxTTL;
			}
			// p selects K peers and sends them the message
			msgCount += k;
			ttl[p]--;
			newInfections.addAll(selectRandomPeers());
		} // while

		// Add all new infections to the infection queue
		filterOutInfectedPeers(newInfections);
		denialOfService();
		infectedPeers.addAll(newInfections);
		newInfections.clear();
		// Add all peers with TTL > 0 to the infectedPeers to send the message next round
		for (int p = 0; p < ttl.length; p++) {
			if (ttl[p] > 0) {
				infectedPeers.add(p);
			}
		}
	} // forwardingPhase

	private void pullPhase() {
		// Adversary tries to block a portion of infected peers that got the
		// message
		List<Integer> blockedPeers = selectRandomInfectedPeers();
		for (int p = 0; p < peers.length; p++) {
			// Already infected, nothing to do
			if (peers[p]) {
				continue;
			}
			// Else select some peers to pull the message from
			boolean pullSucceeded = selectRandomPeers().stream().filter(q -> !blockedPeers.contains(q))
					.anyMatch(q -> peers[q]);
			msgCount += k;
			if (pullSucceeded) {
				peers[p] = true;
				infectedCount++;
			}
		}
	} // pullPhase

	private List<Integer> selectRandomInfectedPeers() {
		List<Integer> peers = new ArrayList<>(infectedPeers);
		Collections.shuffle(peers);
		if (peers.size() <= a) {
			return peers;
		}
		return peers.subList(0, a);
	}

	private List<Integer> selectRandomPeers() {
		List<Integer> peers = new ArrayList<>();
		// N >> k so it's ok not to check corner cases

		while (peers.size() < k) {
			int p = rnd.nextInt(n);
			if (peers.contains(p)) {
				continue;
			}
			peers.add(p);
		}
		return peers;
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
		IntStream.range(1, 13000).forEach( i-> {
			simResults.add(new ByteArrayOutputStream());
		});
		
		ThreadPoolExecutor pool = (ThreadPoolExecutor) Executors.newFixedThreadPool(90);

		IntStream.of(1000, 2000, 16000).parallel().forEach(n -> {
			IntStream.iterate(0, i -> i + n / 10).limit(5).forEach(a -> {
				int maxH = (int) Math.log(n);
				IntStream.range(1, maxH).parallel().forEach(h -> {
					IntStream.iterate(1, i -> i + 1).limit(2 * (long) Math.log(n)).forEach(k -> {
						if (k > n) {
							return;
						}
						IntStream.range(1, 10).parallel().forEach(ttl -> {
							Simulation sim = new Simulation(h, k, n, a, ttl, simResults.get(iterations.getAndIncrement()));
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
}
