package sim.gossip.hrl.il.ibm.com;

import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Set;

public class Simulation {

	private int h;
	private int k;
	private int n;
	private int lastRound;
	private boolean[] peers;
	private Set<Integer> newInfections = new HashSet<Integer>();
	private LinkedList<Integer> infectedPeers = new LinkedList<Integer>();
	private int infectedCount;

	public Simulation(int h, int k, int n) {
		if (k > n) {
			throw new RuntimeException("k(" + k + ") must be less than n(" + n + ")");
		}
		this.h = h;
		this.k = k;
		this.n = n;
		this.peers = new boolean[n];
	}

	public void run() {
		lastRound = (int) (n * Math.log(n));
		// Some peer is infected at first
		infectedPeers.add((int) (Math.random() * n));
		for (int i = 0; i < lastRound && infectedCount < n; i++) {
			round(i);
		}
		System.out.println("Infected:" + infectedCount + " out of " + n);
	}

	private void round(int r) {
		System.out.println("Running simulation round " + r + " out of " + lastRound);
		if (r <= h) {
			forwardingPhase();
			return;
		}
		pullPhase();
	} // round

	private void forwardingPhase() {
		while (!infectedPeers.isEmpty()) {
			int p = infectedPeers.removeFirst();
			// Peer has already been infected
			if (peers[p]) {
				continue;
			}
			peers[p] = true;
			infectedCount++;
			for (int q : selectRandomPeers(k, n)) {
				// If current peer has not been infected yet
				newInfections.add(q);
			}
		} // while

		// Add all new infections to the infection queue
		newInfections.stream().forEach(p -> {
			infectedPeers.add(p);
		});
		newInfections.clear();
	} // forwardingPhase

	private void pullPhase() {
		for (int p = 0; p < peers.length; p++) {
			// Already infected, nothing to do
			if (peers[p]) {
				continue;
			}
			// Else select some peers to pull the message from
			boolean pullSucceeded = Arrays.stream(selectRandomPeers(k, n)).anyMatch(q -> peers[q]);
			if (pullSucceeded) {
				peers[p] = true;
				infectedCount++;
			}
		}
	} // pullPhase

	private static int[] selectRandomPeers(int k, int n) {
		int[] peers = new int[k];
		// N >> k so it's ok not to check corner cases
		for (int i = 0; i < k; i++) {
			while (true) {
				boolean exists = false;
				int p = (int) (Math.random() * n);
				for (int j = 0; j < i; j++) {
					if (peers[j] == p) {
						exists = true;
						break;
					} // if
				} // for j < i
				if (!exists) {
					peers[i] = p;
					break;
				}
			} // while
		} // for i < n
		return peers;
	}

	public static void main(String[] args) {
		new Simulation(70, 6, 1000).run();
	}
}
