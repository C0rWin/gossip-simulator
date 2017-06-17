package sim.gossip.hrl.il.ibm.com;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.IntStream;

public class Simulation {

	private static final String csvPattern = "%d,%d,%d,%d,%d,%d,%d";

	private int h;
	private int k;
	private int n;
	private int a;
	private int lastRound;
	private boolean[] peers;
	private Set<Integer> newInfections = new HashSet<Integer>();
	private LinkedList<Integer> infectedPeers = new LinkedList<Integer>();
	private int infectedCount;
	private Random rnd = new Random(System.currentTimeMillis());
	private int msgCount;
	private static PrintWriter w;

	public Simulation(int h, int k, int n, int a) {
		if (k > n) {
			throw new RuntimeException("k(" + k + ") must be less than n(" + n + ")");
		}
		this.h = h;
		this.k = k;
		this.n = n;
		this.a = a;
		this.peers = new boolean[n];
	}

	public void run() {
		lastRound = (int) (n * Math.log(n));
		// Some peer is infected at first
		infectedPeers.add((int) (Math.random() * n));
		for (int i = 0; i < lastRound; i++) {
			round(i);
		}
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

		int removedCount = 0;
		for (Iterator<Integer> i = newInfections.iterator(); i.hasNext() && removedCount < a;) {
			i.next();
			removedCount++;
			i.remove();
		}
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
			peers[p] = true;
			infectedCount++;
			msgCount += k;
			newInfections.addAll(selectRandomPeers());
		} // while

		// Add all new infections to the infection queue
		filterOutInfectedPeers(newInfections);
		denialOfService();
		infectedPeers.addAll(newInfections);
		newInfections.clear();
	} // forwardingPhase

	private void pullPhase() {
		for (int p = 0; p < peers.length; p++) {
			// Already infected, nothing to do
			if (peers[p]) {
				continue;
			}
			// Adversary tries to block a portion of infected peers that got the
			// message
			List<Integer> blockedPeers = selectRandomInfectedPeers();
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
		List<Integer> peers = new ArrayList<>();
		peers.addAll(infectedPeers);
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
		w.println(String.format(csvPattern, r, h, k, n, a, infectedCount, msgCount));
	}

	public static void main(String[] args) {
		try {
			w = new PrintWriter("log.csv");
			w.println("r,h,k,n,a,infected,msgs");
		} catch (FileNotFoundException e) {
			e.printStackTrace();
			return;
		}
		IntStream.iterate(10, i -> i * 2).limit((long) Math.log(10000)).forEach(n -> {
			IntStream.iterate(0, i -> i + n / 10).limit(5).forEach(a -> {
				int maxH = (int) Math.log(n) * 2;
				IntStream.range(1, maxH).forEach(h -> {
					IntStream.iterate(1, i -> i * 2).limit((long) Math.log(n)).forEach(k -> {
						if (k > n) {
							return;
						}
						new Simulation(h, k, n, a).run();
					});
				});
			});
		});
		w.flush();
		w.close();
	}
}
