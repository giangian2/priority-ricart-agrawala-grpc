package smartfab.http.client;

import java.util.Date;
import java.util.List;
import java.util.Scanner;

import smartfab.http.contorller.ProductionLineController.PeerStatusResponse;


/**
 * @author Gianluca Bianchi
 *
 *      Interactive command line client for the analyst. It talks to the admin
 *      server through {@link smartfab.http.client.AdminRestClient}.
 *      The base url of the admin server can be passed as the first program
 *      argument (default: http://localhost:8080).
 */
public class AdminCLI {

    private static final String DEFAULT_BASE_URL = "http://localhost:8080";

    public static void main(String... args) {
        String baseUrl = (args.length > 0) ? args[0] : DEFAULT_BASE_URL;
        var client = new AdminRestClient(baseUrl);

        System.out.println(" Smartfab Admin CLI connected to " + baseUrl);
        printHelp();

        try (var scanner = new Scanner(System.in)) {
            boolean running = true;
            while (running) {
                System.out.print("\nsmartfab> ");
                if (!scanner.hasNextLine()) {
                    break;
                }

                String line = scanner.nextLine().trim();
                if (line.isEmpty()) {
                    continue;
                }

                String[] tokens = line.split("\\s+");
                String command = tokens[0].toLowerCase();

                try {
                    switch (command) {
                        case "list" -> handleList(client);
                        case "find" -> handleFind(client, tokens);
                        case "avgs" -> handleAverages(client, tokens);
                        case "help" -> printHelp();
                        case "exit", "quit" -> running = false;
                        default -> System.out.println("Unknown command: '" + command + "'. Type 'help'.");
                    }
                } catch (NumberFormatException e) {
                    System.out.println("Invalid number: " + e.getMessage());
                } catch (Exception e) {
                    System.out.println("Request failed: " + e.getMessage());
                }
            }
        }

        System.out.println("Bye.");
    }

    private static void handleList(AdminRestClient client) {
        List<PeerStatusResponse> peers = client.listPeers();
        if (peers == null || peers.isEmpty()) {
            System.out.println("No peers registered.");
            return;
        }

        System.out.printf("%-6s %-18s %-8s %-12s%n", "ID", "ADDRESS", "PORT", "STATUS");
        peers.forEach(AdminCLI::printPeer);
    }

    private static void handleFind(AdminRestClient client, String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("Usage: find <lineID>");
            return;
        }

        int lineID  = Integer.parseInt(tokens[1]);
        var peer    = client.findPeer(lineID);
        
        if (peer == null) {
            System.out.println("Peer " + lineID + " not found.");
            return;
        }

        System.out.printf("%-6s %-18s %-8s %-12s%n", "ID", "ADDRESS", "PORT", "STATUS");
        printPeer(peer);
    }

    private static void handleAverages(AdminRestClient client, String[] tokens) {
        if (tokens.length < 2) {
            System.out.println("Usage: avgs <lineID> [from] [to]");
            return;
        }

        int lineID  = Integer.parseInt(tokens[1]);
        Long from   = (tokens.length > 2) ? Long.parseLong(tokens[2]) : null;
        Long to     = (tokens.length > 3) ? Long.parseLong(tokens[3]) : null;

        var averages = client.findAverages(lineID, from, to);
        if (averages == null || averages.isEmpty()) {
            System.out.println("No averages for line " + lineID + " in the requested window.");
            return;
        }

        System.out.printf("%-6s %-14s %-24s%n", "LINE", "AVG", "TIMESTAMP");
        averages.forEach(a -> System.out.printf(
                "%-6d %-14.4f %-24s%n",
                a.getLineId(),
                a.getAvg(),
                new Date(a.getTimestamp())));
    }

    private static void printPeer(PeerStatusResponse peer) {
        System.out.printf("%-6d %-18s %-8d %-12s%n",
                peer.id(),
                peer.address(),
                peer.port(),
                peer.status());
    }

    private static void printHelp() {
        System.out.println("""
                Available commands:
                  list                        list peers with their status
                  find <lineID>               find a single peer with its status
                  avgs <lineID> [from] [to]   find averages of a line (from/to = epoch millis)
                  help                        show this help
                  exit                        quit""");
    }
}