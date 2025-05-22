package server;

public class SystemKeeper {

    private final FileManager fileManager;
    private final int fileThreshold = 50_000_000;  // 50 MB
    private final long compactionInterval = 60_000;  // 1 minute
    private final long CompactionCheckInterval = 1_000;  // 1 second
    private final long fileCheckInterval = 100;  // 0.1 second

    private volatile boolean running = true;

    private Thread fileCheckThread;
    private Thread compactionThread;

    public SystemKeeper(FileManager fileManager){
        this.fileManager = fileManager;
    }

    private void activeFileCheckLoop() {
        while (running) {
            try {
                if (fileManager.getActiveFileSize() >= fileThreshold) {
                    fileManager.createActiveFile();
                }
                Thread.sleep(fileCheckInterval);
            } catch (InterruptedException e) {
                System.out.println("Active file check interrupted.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.out.println("Error in activeFileCheck: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    private void compactionCheckLoop() {
        long lastCompactionTime = System.currentTimeMillis();
        while (running) {
            try {
                long now = System.currentTimeMillis();
                if (now - lastCompactionTime >= compactionInterval) {
                    System.out.println("Running compaction...");
                    fileManager.compact();
                    lastCompactionTime = System.currentTimeMillis();
                }
                Thread.sleep(CompactionCheckInterval);
            } catch (InterruptedException e) {
                System.out.println("Compaction check interrupted.");
                Thread.currentThread().interrupt();
            } catch (Exception e) {
                System.out.println("Error in compactionCheck: " + e.getMessage());
                e.printStackTrace();
            }
        }
    }

    public void startInBackground() {
        stop(); // Ensure old threads are stopped if restarting

        running = true;

        fileCheckThread = new Thread(this::activeFileCheckLoop);
        fileCheckThread.setDaemon(true);
        fileCheckThread.start();

        compactionThread = new Thread(this::compactionCheckLoop);
        compactionThread.setDaemon(true);
        compactionThread.start();
    }

    public void stop() {
        running = false;

        if (fileCheckThread != null) {
            fileCheckThread.interrupt();
        }

        if (compactionThread != null) {
            compactionThread.interrupt();
        }
    }
}
