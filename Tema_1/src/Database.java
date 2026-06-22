import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;

public class Database {
    private final List<DatabaseWorker> workers = new ArrayList<>();
    private final Inputs inputs;

    private final ConcurrentLinkedQueue<String> metadataFilePathsToProcess = new ConcurrentLinkedQueue<>();
    private final ConcurrentLinkedQueue<Article> articlesToIndex = new ConcurrentLinkedQueue<>();

    private CountDownLatch numberOfWorkersReading;
    private CountDownLatch numberOfWorkersIndexing;
    private final CountDownLatch startReadingArticles;
    private final CountDownLatch startIndexingArticles;

    private int numberOfArticlesProcessed = 0;
    private final ArrayList<Article> articles = new ArrayList<>();

    private final HashMap<String, Integer> authorPublishedCount = new HashMap<>();
    private final HashMap<String, Integer> numberOfArticlesContaingWord = new HashMap<>();
    private final HashMap<String, TreeSet<String>> articlesInLanguage = new HashMap<>();
    private final HashMap<String, TreeSet<String>> articlesOfCategory = new HashMap<>();

    public Database(Inputs inInputs) {
        inputs = inInputs;        
        startIndexingArticles = new CountDownLatch(1);
        startReadingArticles = new CountDownLatch(1);
    }

    public DatabaseWorker createWorker() {
        DatabaseWorker worker = new DatabaseWorker(this);
        workers.add(worker);
        return worker;
    }

    public void addMetadataFilePath(String filePath) {
        metadataFilePathsToProcess.add(filePath);
    }

    public void manageWorkers() throws InterruptedException {
        numberOfWorkersReading = new CountDownLatch(workers.size());
        numberOfWorkersIndexing = new CountDownLatch(workers.size());

        startReadingArticles.countDown();
        numberOfWorkersReading.await();
        bindReadingResultsFromWorkers();
        
        startIndexingArticles.countDown();
        // Wait for all workers to finish indexing articles.
        numberOfWorkersIndexing.await();
        bindIndexingResultsFromWorkers();
    }

    public String acquireMetadataFilePath() {
        // Returns null if no metadata file paths are in the collection.
        return metadataFilePathsToProcess.poll();
    }

    public Article acquireArticleToIndex() {
        // Returns null if no articles are in the collection.
        return articlesToIndex.poll();
    }

    public void decrementNumberOfWorkersReading() {
        numberOfWorkersReading.countDown();
    }

    public void decrementNumberOfWorkersIndexing() {
        numberOfWorkersIndexing.countDown();
    }

    public void waitForStartingToReadArticles() throws InterruptedException {
        startReadingArticles.await();
    }

    public void waitForStartingToIndexArticles() throws InterruptedException {
        startIndexingArticles.await();
    }

    public Inputs getInputs() {
        return inputs;
    }

    private void bindReadingResultsFromWorkers() {
        HashSet<String> duplicatedTitles = new HashSet<>();
        HashSet<String> duplicatedUUIDs = new HashSet<>();

        HashMap<String, Article> titleToArticle = new HashMap<>();
        HashMap<String, Article> uuidToArticle = new HashMap<>();

        for (DatabaseWorker worker : workers) {
            for (Article article : worker.processedArticles) {
                // NOTE: This algorithm removed the duplicates from the article list.
                //       Currently, it seems more complicated than it needs to be but it gets the job done.

                if (duplicatedUUIDs.contains(article.uuid))
                    continue;
                if (duplicatedTitles.contains(article.title))
                    continue;

                if (uuidToArticle.containsKey(article.uuid)) {
                    duplicatedUUIDs.add(article.uuid);
                    duplicatedTitles.add(article.title);

                    String title = uuidToArticle.get(article.uuid).title;
                    uuidToArticle.remove(article.uuid);
                    titleToArticle.remove(title);

                    continue;
                }

                if (titleToArticle.containsKey(article.title)) {
                    duplicatedUUIDs.add(article.uuid);
                    duplicatedTitles.add(article.title);

                    String uuid = titleToArticle.get(article.title).uuid;
                    uuidToArticle.remove(uuid);
                    titleToArticle.remove(article.title);

                    continue;
                }

                uuidToArticle.put(article.uuid, article);
                titleToArticle.put(article.title, article);
            }

            numberOfArticlesProcessed += worker.processedArticles.size();
            // Clear structures that will not be used anymore in order to save memory.
            worker.processedArticles.clear();
            worker.processedArticles = null;
        }

        duplicatedTitles.clear();
        duplicatedUUIDs.clear();
        titleToArticle.clear();
        
        for (var entry : uuidToArticle.entrySet())
            articles.add(entry.getValue());
        uuidToArticle.clear();

        // Queue all articles for indexing.
        articlesToIndex.addAll(articles);
    }

    private void bindIndexingResultsFromWorkers() {
        for (DatabaseWorker worker : workers) {
            // Update articles published in each language.
            for (var languageEntry : worker.languagePublishedArticles.entrySet()) {
                articlesInLanguage.putIfAbsent(languageEntry.getKey(), new TreeSet<>());
                articlesInLanguage.get(languageEntry.getKey()).addAll(languageEntry.getValue());
            }
            worker.languagePublishedArticles.clear();

            // Update articles published for each category.
            for (var categoryEntry : worker.categoryPublishedArticles.entrySet()) {
                articlesOfCategory.putIfAbsent(categoryEntry.getKey(), new TreeSet<>());
                articlesOfCategory.get(categoryEntry.getKey()).addAll(categoryEntry.getValue());
            }
            worker.categoryPublishedArticles.clear();

            // Update number of articles published by each author.
            for (var authorEntry : worker.authorPublishedCount.entrySet()) {
                int currentCount = authorPublishedCount.getOrDefault(authorEntry.getKey(), 0);
                authorPublishedCount.put(authorEntry.getKey(), currentCount += authorEntry.getValue());
            }
            worker.authorPublishedCount.clear();

            // Update number of articles each word appears in.
            for (var wordEntry : worker.englishWordCount.entrySet()) {
                int currentCount = numberOfArticlesContaingWord.getOrDefault(wordEntry.getKey(), 0);
                numberOfArticlesContaingWord.put(wordEntry.getKey(), currentCount += wordEntry.getValue());
            }
            worker.englishWordCount.clear();
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void writeAllProcessedArticles() {
        Comparator<Article> articleComparator = Comparator
            .comparing((Article article) -> article.published, Comparator.reverseOrder())
            .thenComparing(article -> article.uuid);
        articles.sort(articleComparator);

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("all_articles.txt"))) {
            for (Article article : articles) {
                writer.write(article.uuid);
                writer.write(' ');
                writer.write(article.published);
                writer.newLine();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void writeLanguageIndexing() {
        for (String language : inputs.languages) {
            TreeSet<String> articleUuuids = articlesInLanguage.get(language);
            if (articleUuuids != null && !articleUuuids.isEmpty()) {
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(language + ".txt"))) {
                    for (String uuid : articleUuuids) {
                        writer.write(uuid);
                        writer.newLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }

    @SuppressWarnings("CallToPrintStackTrace")
    public void writeCategoryIndexing() {
        for (String category : inputs.categories) {
            TreeSet<String> articleUuuids = articlesOfCategory.get(category);
            if (articleUuuids != null && !articleUuuids.isEmpty()) {
                String categoryName = category.replace(',', ' ').trim().replaceAll("\\s+", "_");
                try (BufferedWriter writer = new BufferedWriter(new FileWriter(categoryName + ".txt"))) {
                    for (String uuid : articleUuuids) {
                        writer.write(uuid);
                        writer.newLine();
                    }
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }
    }
    
    public void writeKeywordsCount() throws IOException {
        List<Map.Entry<String, Integer>> list = new ArrayList<>(numberOfArticlesContaingWord.entrySet());
        list.sort((a, b) -> {
            // Sort the words in descending order regarding number of articles containing them.
            int cmp = b.getValue().compareTo(a.getValue());
            if (cmp != 0)
                return cmp;
            // Sort the words lexicographically if they appear in the same number of articles.
            return a.getKey().compareTo(b.getKey());
        });

        try (BufferedWriter writer = new BufferedWriter(new FileWriter("keywords_count.txt"))) {
            for (var e : list) {
                writer.write(e.getKey() + " " + e.getValue());
                writer.newLine();
            }
        }
    }

    public void writeReport() throws IOException {
        DatabaseReport report = queryReport();
        try (BufferedWriter writer = new BufferedWriter(new FileWriter("reports.txt"))) {
            writer.write("duplicates_found - " + (numberOfArticlesProcessed - articles.size()) + "\n");
            writer.write("unique_articles - " + articles.size() + "\n");
            writer.write("best_author - " + report.bestAuthor + " " + report.bestAuthorPublishedCount + "\n");
            writer.write("top_language - " + report.topLanguage + " " + report.topLanguagePublishedCount + "\n");
            
            String category = report.topCategory.replace(',', ' ').trim().replaceAll("\\s+", "_");
            writer.write("top_category - " + category + " " + report.topCategoryPublishedCount + "\n");
            
            writer.write("most_recent_article - " + report.mostRecentArticle.published + " " + report.mostRecentArticle.url + "\n");
            writer.write("top_keyword_en - " + report.topEnglishKeyword + " " + report.topEnglishKeywordCount + "\n");
        }
    }

    private DatabaseReport queryReport() {
        DatabaseReport report = new DatabaseReport();

        // Find the best author.
        {
            String bestAuthor = new String();
            int bestAuthorPublishedCount = 0;
            for (var entry : authorPublishedCount.entrySet()) {
                if (entry.getValue() >= bestAuthorPublishedCount) {
                    if (entry.getValue() == bestAuthorPublishedCount) {
                        if (entry.getKey().compareTo(bestAuthor) < 0)
                            bestAuthor = entry.getKey();
                    }
                    else {
                        bestAuthor = entry.getKey();
                    }

                    bestAuthorPublishedCount = entry.getValue();
                }
            }

            report.bestAuthor = bestAuthor;
            report.bestAuthorPublishedCount = bestAuthorPublishedCount;
        }

        // Find the top language.
        {
            String topLanguage = new String();
            int topLanguagePublishedCount = 0;
            for (var entry : articlesInLanguage.entrySet()) {
                if (entry.getValue().size() >= topLanguagePublishedCount) {
                    if (entry.getValue().size() == topLanguagePublishedCount) {
                        if (entry.getKey().compareTo(topLanguage) < 0)
                            topLanguage = entry.getKey();
                    }
                    else {
                        topLanguage = entry.getKey();
                    }

                    topLanguagePublishedCount = entry.getValue().size();
                }
            }

            report.topLanguage = topLanguage;
            report.topLanguagePublishedCount = topLanguagePublishedCount;
        }

        // Find the top category.
        {
            String topCategory = new String();
            int topCategoryPublishedCount = 0;
            for (var entry : articlesOfCategory.entrySet()) {
                if (entry.getValue().size() >= topCategoryPublishedCount) {
                    if (entry.getValue().size() == topCategoryPublishedCount) {
                        if (entry.getKey().compareTo(topCategory) < 0)
                            topCategory = entry.getKey();
                    }
                    else {
                        topCategory = entry.getKey();
                    }

                    topCategoryPublishedCount = entry.getValue().size();
                }
            }

            report.topCategory = topCategory;
            report.topCategoryPublishedCount = topCategoryPublishedCount;
        }

        // Find the most recent article.
        {
            Article mostRecentArticle = null;
            for (Article article : articles) {
                String published = article.published;
                String uuid = article.uuid;
                if (mostRecentArticle == null || mostRecentArticle.published.compareTo(published) < 0) {
                    mostRecentArticle = article;
                }
                else if (mostRecentArticle.published.equals(published)) {
                    if (mostRecentArticle.uuid.compareTo(uuid) > 0)
                        mostRecentArticle = article;
                }
            }

            report.mostRecentArticle = mostRecentArticle;
        }

        // Find the top english word.
        {
            String topEnglishKeyword = new String();
            int topEnglishKeywordCount = 0;
            for (var entry : numberOfArticlesContaingWord.entrySet()) {
                if (entry.getValue() > topEnglishKeywordCount) {
                    topEnglishKeyword = entry.getKey();
                    topEnglishKeywordCount = entry.getValue();
                }
                else if (entry.getValue() == topEnglishKeywordCount) {
                    if (topEnglishKeyword.compareTo(entry.getKey()) > 0)
                        topEnglishKeyword = entry.getKey();
                }
            }
            
            report.topEnglishKeyword = topEnglishKeyword;
            report.topEnglishKeywordCount = topEnglishKeywordCount;
        }

        return report;
    }
}
