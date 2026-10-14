package com.streamingrag.vector;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import dev.langchain4j.data.document.Metadata;
import dev.langchain4j.data.embedding.Embedding;
import dev.langchain4j.data.segment.TextSegment;
import dev.langchain4j.store.embedding.EmbeddingMatch;
import dev.langchain4j.store.embedding.EmbeddingSearchRequest;
import dev.langchain4j.store.embedding.EmbeddingSearchResult;
import dev.langchain4j.store.embedding.EmbeddingStore;
import org.apache.lucene.analysis.standard.StandardAnalyzer;
import org.apache.lucene.document.Document;
import org.apache.lucene.document.Field;
import org.apache.lucene.document.KnnFloatVectorField;
import org.apache.lucene.document.StoredField;
import org.apache.lucene.document.StringField;
import org.apache.lucene.document.TextField;
import org.apache.lucene.index.IndexWriter;
import org.apache.lucene.index.IndexWriterConfig;
import org.apache.lucene.index.Term;
import org.apache.lucene.index.VectorSimilarityFunction;
import org.apache.lucene.search.IndexSearcher;
import org.apache.lucene.search.KnnFloatVectorQuery;
import org.apache.lucene.search.ScoreDoc;
import org.apache.lucene.search.SearcherFactory;
import org.apache.lucene.search.SearcherManager;
import org.apache.lucene.search.TopDocs;
import org.apache.lucene.store.Directory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * Enterprise Apache Lucene embedded implementation of LangChain4j {@link EmbeddingStore}.
 * Provides in-process, zero-external-dependency dense vector indexing and cosine k-NN similarity search
 * powered by Lucene's native HNSW graph structures ({@link KnnFloatVectorField} and {@link KnnFloatVectorQuery}).
 */
public class LuceneEmbeddingStore implements EmbeddingStore<TextSegment>, AutoCloseable {

    private static final Logger log = LoggerFactory.getLogger(LuceneEmbeddingStore.class);

    public static final String FIELD_ID = "id";
    public static final String FIELD_VECTOR = "vector";
    public static final String FIELD_TEXT = "text";
    public static final String FIELD_TEXT_CONTENT = "text_content";
    public static final String FIELD_METADATA = "metadata";

    private final Directory directory;
    private final StandardAnalyzer analyzer;
    private final IndexWriter indexWriter;
    private final SearcherManager searcherManager;
    private final ObjectMapper objectMapper;
    private final VectorSimilarityFunction similarityFunction;

    public LuceneEmbeddingStore(Directory directory, ObjectMapper objectMapper) throws IOException {
        this(directory, objectMapper, VectorSimilarityFunction.COSINE);
    }

    public LuceneEmbeddingStore(
            Directory directory,
            ObjectMapper objectMapper,
            VectorSimilarityFunction similarityFunction) throws IOException {
        this.directory = directory;
        this.objectMapper = (objectMapper != null) ? objectMapper : new ObjectMapper();
        this.similarityFunction = (similarityFunction != null) ? similarityFunction : VectorSimilarityFunction.COSINE;
        this.analyzer = new StandardAnalyzer();

        IndexWriterConfig config = new IndexWriterConfig(this.analyzer);
        config.setOpenMode(IndexWriterConfig.OpenMode.CREATE_OR_APPEND);
        this.indexWriter = new IndexWriter(this.directory, config);
        this.searcherManager = new SearcherManager(this.indexWriter, new SearcherFactory());
    }

    @Override
    public String add(Embedding embedding) {
        String id = UUID.randomUUID().toString();
        add(id, embedding, null);
        return id;
    }

    @Override
    public void add(String id, Embedding embedding) {
        add(id, embedding, null);
    }

    @Override
    public String add(Embedding embedding, TextSegment textSegment) {
        String id = UUID.randomUUID().toString();
        add(id, embedding, textSegment);
        return id;
    }

    public void add(String id, Embedding embedding, TextSegment textSegment) {
        try {
            Document doc = buildDocument(id, embedding, textSegment);
            indexWriter.addDocument(doc);
            refreshReader();
            log.debug("[LUCENE] Indexed vector document id={}", id);
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed to index document id={}: {}", id, e.getMessage(), e);
            throw new RuntimeException("Failed to add vector document to Lucene: " + e.getMessage(), e);
        }
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings) {
        return addAll(embeddings, null);
    }

    @Override
    public List<String> addAll(List<Embedding> embeddings, List<TextSegment> textSegments) {
        if (embeddings == null || embeddings.isEmpty()) {
            return Collections.emptyList();
        }
        List<String> ids = new ArrayList<>(embeddings.size());
        for (int i = 0; i < embeddings.size(); i++) {
            ids.add(UUID.randomUUID().toString());
        }
        addAll(ids, embeddings, textSegments);
        return ids;
    }

    public void addAll(List<String> ids, List<Embedding> embeddings, List<TextSegment> textSegments) {
        if (embeddings == null || embeddings.isEmpty()) {
            return;
        }
        try {
            List<Document> docs = new ArrayList<>(embeddings.size());
            for (int i = 0; i < embeddings.size(); i++) {
                String id = (ids != null && i < ids.size()) ? ids.get(i) : UUID.randomUUID().toString();
                Embedding emb = embeddings.get(i);
                TextSegment seg = (textSegments != null && i < textSegments.size()) ? textSegments.get(i) : null;
                docs.add(buildDocument(id, emb, seg));
            }
            indexWriter.addDocuments(docs);
            refreshReader();
            log.debug("[LUCENE] Batch indexed {} documents", docs.size());
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed to batch index documents: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to batch index vector documents in Lucene: " + e.getMessage(), e);
        }
    }

    @Override
    public EmbeddingSearchResult<TextSegment> search(EmbeddingSearchRequest request) {
        Embedding queryEmbedding = request.queryEmbedding();
        int maxResults = request.maxResults();
        double minScore = request.minScore();

        if (queryEmbedding == null || maxResults <= 0) {
            return new EmbeddingSearchResult<>(Collections.emptyList());
        }

        KnnFloatVectorQuery query = new KnnFloatVectorQuery(FIELD_VECTOR, queryEmbedding.vector(), maxResults);

        IndexSearcher searcher = null;
        try {
            searcher = searcherManager.acquire();
            TopDocs topDocs = searcher.search(query, maxResults);

            List<EmbeddingMatch<TextSegment>> matches = new ArrayList<>();
            for (ScoreDoc scoreDoc : topDocs.scoreDocs) {
                double score = scoreDoc.score;
                if (score < minScore) {
                    continue;
                }

                Document doc = searcher.storedFields().document(scoreDoc.doc);
                String id = doc.get(FIELD_ID);
                String text = doc.get(FIELD_TEXT);
                String metadataJson = doc.get(FIELD_METADATA);

                Metadata metadata;
                if (metadataJson != null && !metadataJson.isBlank()) {
                    try {
                        Map<String, Object> map = objectMapper.readValue(
                                metadataJson, new TypeReference<Map<String, Object>>() {});
                        metadata = Metadata.from(map);
                    } catch (Exception ex) {
                        log.warn("[LUCENE] Failed to deserialize metadata for doc id={}: {}", id, ex.getMessage());
                        metadata = new Metadata();
                    }
                } else {
                    metadata = new Metadata();
                }

                TextSegment segment = TextSegment.from(text != null ? text : "", metadata);
                matches.add(new EmbeddingMatch<>(score, id, null, segment));
            }

            return new EmbeddingSearchResult<>(matches);
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed searching vector store: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to execute k-NN vector search in Lucene: " + e.getMessage(), e);
        } finally {
            if (searcher != null) {
                try {
                    searcherManager.release(searcher);
                } catch (IOException e) {
                    log.warn("[LUCENE WARN] Failed releasing searcher: {}", e.getMessage());
                }
            }
        }
    }

    @Override
    public void remove(String id) {
        try {
            indexWriter.deleteDocuments(new Term(FIELD_ID, id));
            refreshReader();
            log.debug("[LUCENE] Deleted document id={}", id);
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed deleting document id={}: {}", id, e.getMessage(), e);
            throw new RuntimeException("Failed to delete document from Lucene: " + e.getMessage(), e);
        }
    }

    @Override
    public void removeAll(Collection<String> ids) {
        if (ids == null || ids.isEmpty()) {
            return;
        }
        try {
            Term[] terms = ids.stream().map(id -> new Term(FIELD_ID, id)).toArray(Term[]::new);
            indexWriter.deleteDocuments(terms);
            refreshReader();
            log.debug("[LUCENE] Deleted {} documents by ids", ids.size());
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed deleting documents: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to batch delete documents from Lucene: " + e.getMessage(), e);
        }
    }

    @Override
    public void removeAll() {
        try {
            indexWriter.deleteAll();
            refreshReader();
            log.debug("[LUCENE] Deleted all documents in vector index");
        } catch (IOException e) {
            log.error("[LUCENE ERROR] Failed clearing vector store: {}", e.getMessage(), e);
            throw new RuntimeException("Failed to clear Lucene vector store: " + e.getMessage(), e);
        }
    }

    private Document buildDocument(String id, Embedding embedding, TextSegment textSegment) throws IOException {
        Document doc = new Document();
        doc.add(new StringField(FIELD_ID, id, Field.Store.YES));

        if (embedding != null && embedding.vector() != null) {
            doc.add(new KnnFloatVectorField(FIELD_VECTOR, embedding.vector(), similarityFunction));
        }

        if (textSegment != null) {
            String text = textSegment.text();
            if (text != null) {
                doc.add(new StoredField(FIELD_TEXT, text));
                doc.add(new TextField(FIELD_TEXT_CONTENT, text, Field.Store.NO));
            }
            if (textSegment.metadata() != null) {
                String metadataJson = objectMapper.writeValueAsString(textSegment.metadata().toMap());
                doc.add(new StoredField(FIELD_METADATA, metadataJson));
            }
        }

        return doc;
    }

    private void refreshReader() throws IOException {
        searcherManager.maybeRefresh();
    }

    @Override
    public void close() throws IOException {
        log.info("[LUCENE] Closing LuceneEmbeddingStore...");
        try {
            searcherManager.close();
        } finally {
            try {
                indexWriter.close();
            } finally {
                try {
                    analyzer.close();
                } finally {
                    directory.close();
                }
            }
        }
    }
}
