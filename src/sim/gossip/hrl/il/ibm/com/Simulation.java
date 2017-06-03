package sim.gossip.hrl.il.ibm.com;

import java.io.FileNotFoundException;
import java.io.PrintWriter;
import java.util.*;
import java.util.stream.IntStream;

public class Simulation {

	private static final String csvPattern = "%d,%d,%d,%d,%d,%d";
	
    private int h;
    private int k;
    private int n;
    private int lastRound;
    private boolean[] peers;
    private Set<Integer> newInfections = new HashSet<Integer>();
    private LinkedList<Integer> infectedPeers = new LinkedList<Integer>();
    private int infectedCount;
    private Random rnd = new Random(System.currentTimeMillis());
    private int msgCount;
    private PrintWriter w;

    public Simulation(int h, int k, int n) {
        if (k > n) {
            throw new RuntimeException("k(" + k + ") must be less than n(" + n + ")");
        }
        this.h = h;
        this.k = k;
        this.n = n;
        this.peers = new boolean[n];
        try {
			w = new PrintWriter(String.format("n%d-k%d-h%d.csv", n, k, h));
			w.println("r,h,k,n,infected,msgs");
		} catch (FileNotFoundException e) {
			throw new RuntimeException(e);
		}
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
            // Peer has already been infected
            if (peers[p]) {
                continue;
            }
            peers[p] = true;
            infectedCount++;
            msgCount += k;
            newInfections.addAll(selectRandomPeers(k, n));
        } // while

        // Add all new infections to the infection queue
        infectedPeers.addAll(newInfections);
        newInfections.clear();
    } // forwardingPhase

    private void pullPhase() {
        for (int p = 0; p < peers.length; p++) {
            // Already infected, nothing to do
            if (peers[p]) {
                continue;
            }
            // Else select some peers to pull the message from
            boolean pullSucceeded = selectRandomPeers(k, n).stream().anyMatch(q -> peers[q]);
            msgCount += k;
            if (pullSucceeded) {
                peers[p] = true;
                infectedCount++;
            }
        }
    } // pullPhase

    private List<Integer> selectRandomPeers(int k, int n) {
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
    	w.println(String.format(csvPattern, r, h, k, n, infectedCount, msgCount));
    }
    
    

    public static void main(String[] args) {
    	IntStream.iterate(10, i -> i * 2).limit(10000).parallel().forEach(n -> {
    		int maxH = (int) Math.log(n) * 2;
    		IntStream.range(1, maxH).parallel().forEach(h -> {
    			IntStream.iterate(1, i -> i * 2).limit((long) Math.log(n)).forEach(k -> {
    				if (k > n) {
    					return;
    				}
    				new Simulation(h, k, n).run();
    			});
    		});
    	});
    }
}
