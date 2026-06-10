# Workshop Live Monitor — Flink SQL Queries

Run these in the Flink SQL workspace to watch the workshop progress in real time.
Each query produces a continuously updating result as new events arrive.

---

## 1. Lab 1 Live Order Rate

Orders and revenue per minute, most recent windows shown first.
Confirms the Lab 1 data generator is running and quantifies throughput.

```sql
SELECT
  window_start,
  window_end,
  COUNT(*)             AS orders,
  ROUND(SUM(price), 2) AS revenue
FROM TABLE(
  TUMBLE(TABLE orders, DESCRIPTOR(order_ts), INTERVAL '1' MINUTE)
)
GROUP BY window_start, window_end
ORDER BY window_start DESC
LIMIT 10;
```

---

## 2. Participant Query Leaderboard

Who is most active in Lab 2 — ranked by number of queries sent.

```sql
SELECT
  COALESCE(query_user, '(organizer)') AS participant,
  COUNT(*)                             AS queries_sent
FROM queries
GROUP BY query_user
ORDER BY queries_sent DESC;
```

---

## 3. RAG Pipeline Depth

How many queries have made it through each stage of the pipeline.
A healthy workshop shows roughly equal counts across all four stages.

```sql
SELECT 'queries'                  AS stage, COUNT(*) AS count FROM queries
UNION ALL
SELECT 'queries_embed',                     COUNT(*) FROM queries_embed
UNION ALL
SELECT 'search_results',                    COUNT(*) FROM search_results
UNION ALL
SELECT 'search_results_response',           COUNT(*) FROM search_results_response;
```

> If counts stall at a particular stage, that pipeline step needs attention.
> Run each line as a separate query if UNION ALL is not supported in your environment.

---

## 4. Knowledge Base Hot Spots

Which documents are being retrieved most across all three result slots.
Shows which parts of the knowledge base participants find most relevant.

```sql
SELECT
  document_id,
  COUNT(*) AS times_retrieved
FROM (
  SELECT document_id_1 AS document_id FROM search_results WHERE document_id_1 IS NOT NULL
  UNION ALL
  SELECT document_id_2                FROM search_results WHERE document_id_2 IS NOT NULL
  UNION ALL
  SELECT document_id_3                FROM search_results WHERE document_id_3 IS NOT NULL
)
GROUP BY document_id
ORDER BY times_retrieved DESC
LIMIT 10;
```

---

## 5. Search Quality per Participant

Average similarity score of the top result per participant.
Higher score means the participant's queries are well-matched to the indexed knowledge base.

```sql
SELECT
  COALESCE(q.query_user, '(organizer)') AS participant,
  COUNT(*)                               AS searches_completed,
  ROUND(AVG(sr.score_1), 4)             AS avg_top_score,
  ROUND(MIN(sr.score_1), 4)             AS lowest_top_score
FROM search_results sr
JOIN queries q ON sr.query = q.query
GROUP BY q.query_user
ORDER BY avg_top_score DESC;
```

---

## 6. Documents Published per Participant

How much each participant has contributed to the shared knowledge base in Lab 2.

```sql
SELECT
  COALESCE(document_publisher, '(unknown)') AS participant,
  COUNT(*)                                   AS docs_published,
  SUM(char_count)                            AS total_chars_indexed
FROM documents
GROUP BY document_publisher
ORDER BY docs_published DESC;
```

---

## 7. Quiz Answer Tracker

Live view of all quiz answers per participant and question.
Shows first and last answer submitted, when each was sent, and how many attempts were made.

```sql
SELECT
  question_number,
  participant,
  FIRST_VALUE(answer)    AS first_answer,
  LAST_VALUE(answer)     AS last_answer,
  MIN(answer_ts)         AS first_answered_at,
  MAX(answer_ts)         AS last_answered_at,
  COUNT(*)               AS attempts
FROM quiz_answers
GROUP BY question_number, participant;
```

> Flink streaming queries do not support `ORDER BY` on aggregated fields — sort by clicking column headers in the console.
> A participant who submitted more than once will show `attempts > 1` with the evolution from `first_answer` to `last_answer`.

---