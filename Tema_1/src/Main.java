import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

public class Main {
    public static void main(String[] args) throws InterruptedException, IOException {
        final String numberOfThreadsString = args[0];
        final String articlesFilePath = args[1];
        final String inputsFilePath = args[2];

        // Read inputs.
        Inputs inputs = readInputs(inputsFilePath);

        // Create the article database.
        Database database = new Database(inputs);

        // Create the thread pool and a database worker for each thread.
        final int numberOfThreads = Integer.parseInt(numberOfThreadsString);
        List<Thread> threadPool = new ArrayList<>();
        for (int threadIndex = 0; threadIndex < numberOfThreads; ++threadIndex) {
            DatabaseWorker worker = database.createWorker();
            threadPool.add(new Thread(() -> { worker.run(); }));
        }
        
        // Start the threads, effectively dispatching database workers.
        for (Thread thread : threadPool) {
            thread.start();
        }

        addMetadataFilePathsToDatabase(articlesFilePath, database);
        database.manageWorkers();

        // // Join the threads, effectively wait for all database workers to finish execution.
        for (Thread thread : threadPool) {
            thread.join();
        }

        database.writeAllProcessedArticles();
        database.writeLanguageIndexing();
        database.writeCategoryIndexing();
        database.writeKeywordsCount();
        database.writeReport();
    }

    private static void addMetadataFilePathsToDatabase(String articlesFilePath, Database database) throws IOException {
        HashSet<Path> filePaths = new HashSet<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(articlesFilePath))) {
            int numberOfMetadataFilePaths = Integer.parseInt(reader.readLine().trim());
            for (int i = 0; i < numberOfMetadataFilePaths; i++) {
                String relativeFilePath = reader.readLine();
                Path filePath = Paths.get(articlesFilePath).getParent().resolve(relativeFilePath);
                if (!filePaths.contains(filePath)) {
                    filePaths.add(filePath);
                    database.addMetadataFilePath(filePath.toString());
                }
            }
        }
    }

    private static Inputs readInputs(String inputsFilePath) throws IOException {
        List<String> filePaths = new ArrayList<>();
        try (BufferedReader reader = new BufferedReader(new FileReader(inputsFilePath))) {
            int numberOfFilePaths = Integer.parseInt(reader.readLine().trim());
            for (int i = 0; i < numberOfFilePaths; i++) {
                String relativeFilePath = reader.readLine();
                Path filePath = Paths.get(inputsFilePath).getParent().resolve(relativeFilePath);
                filePaths.add(filePath.toString());
            }
        }

        Inputs inputs = new Inputs();
        inputs.languages = readStructuredFile(filePaths.get(0));
        inputs.categories = readStructuredFile(filePaths.get(1));
        inputs.englishLinkingWords = readStructuredFile(filePaths.get(2));

        return inputs;
    }

    private static List<String> readStructuredFile(String filePath) throws IOException {
        List<String> entries;
        try (BufferedReader reader = new BufferedReader(new FileReader(filePath))) {
            int numberOfEntries = Integer.parseInt(reader.readLine().trim());
            entries = new ArrayList<>(numberOfEntries);
            for (int i = 0; i < numberOfEntries; i++)
                entries.add(reader.readLine().trim());
        }
        return entries;
    }
}
