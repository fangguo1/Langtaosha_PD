import { FormEvent, useEffect, useMemo, useState } from "react";
import { Search, RefreshCw } from "lucide-react";
import { searchPapers } from "./api/search";
import { getDailyNew } from "./api/dailyNew";
import { API_BASE_LABEL, ApiError } from "./api/client";
import type { DailyNewPaper, SearchMode, SearchResult } from "./types";

const SOURCE_OPTIONS = [
  { label: "Langtaosha", value: "langtaosha" },
  { label: "All sources", value: "" },
];

function formatAuthors(authors?: string): string {
  if (!authors) {
    return "Unknown authors";
  }
  return authors;
}

function ResultItem({ result, rank }: { result: SearchResult; rank: number }) {
  const reasons = result.retrieval_reasons || [];
  return (
    <article className="result-row">
      <div className="rank">{rank}</div>
      <div className="result-main">
        <div className="result-meta">
          <span>{result.source || result.source_name || "Unknown source"}</span>
          {result.online_date ? <span>{result.online_date}</span> : null}
          {result.work_id ? <span>{result.work_id}</span> : null}
        </div>
        <h2>
          {result.link ? (
            <a href={result.link} target="_blank" rel="noreferrer">
              {result.title || "Untitled paper"}
            </a>
          ) : (
            result.title || "Untitled paper"
          )}
        </h2>
        <p className="authors">{formatAuthors(result.authors)}</p>
        <p className="abstract">{result.abstract || "No abstract available."}</p>
        {reasons.length ? (
          <div className="reason-list">
            {reasons.map((reason) => (
              <span className="reason-chip" key={reason.key}>
                {reason.label}
                {typeof reason.score === "number" ? ` ${reason.score}` : ""}
              </span>
            ))}
          </div>
        ) : null}
      </div>
    </article>
  );
}

function DailyNewList({ items }: { items: DailyNewPaper[] }) {
  return (
    <section className="side-panel" aria-labelledby="daily-new-heading">
      <div className="panel-heading">
        <h2 id="daily-new-heading">Daily New</h2>
      </div>
      <div className="daily-list">
        {items.length ? (
          items.map((item) => (
            <article className="daily-item" key={item.work_id || item.paper_id}>
              <div className="daily-date">{item.online_date || "No date"}</div>
              <h3>
                {item.link ? (
                  <a href={item.link} target="_blank" rel="noreferrer">
                    {item.title || "Untitled paper"}
                  </a>
                ) : (
                  item.title || "Untitled paper"
                )}
              </h3>
              <p>{formatAuthors(item.authors)}</p>
            </article>
          ))
        ) : (
          <p className="muted">No papers loaded.</p>
        )}
      </div>
    </section>
  );
}

export default function App() {
  const [query, setQuery] = useState("Nav1.7");
  const [mode, setMode] = useState<SearchMode>("smart");
  const [topK, setTopK] = useState(10);
  const [sourceList, setSourceList] = useState("langtaosha");
  const [results, setResults] = useState<SearchResult[]>([]);
  const [dailyNew, setDailyNew] = useState<DailyNewPaper[]>([]);
  const [isSearching, setIsSearching] = useState(false);
  const [isLoadingDailyNew, setIsLoadingDailyNew] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [lastRequestId, setLastRequestId] = useState<string | null>(null);

  const trimmedQuery = useMemo(() => query.trim(), [query]);

  useEffect(() => {
    let cancelled = false;
    setIsLoadingDailyNew(true);
    getDailyNew(10)
      .then((data) => {
        if (cancelled) {
          return;
        }
        setDailyNew(data.results || []);
      })
      .catch((err: unknown) => {
        if (cancelled) {
          return;
        }
        setError(err instanceof Error ? err.message : "Failed to load daily new papers.");
      })
      .finally(() => {
        if (!cancelled) {
          setIsLoadingDailyNew(false);
        }
      });
    return () => {
      cancelled = true;
    };
  }, []);

  async function handleSearch(event?: FormEvent) {
    event?.preventDefault();
    if (!trimmedQuery) {
      setError("请输入搜索内容。");
      return;
    }
    setIsSearching(true);
    setError(null);
    setLastRequestId(null);
    try {
      const data = await searchPapers({
        query: trimmedQuery,
        mode,
        topK,
        sourceList,
      });
      setResults(data.results || []);
      setLastRequestId(data.request_id || null);
    } catch (err) {
      if (err instanceof ApiError) {
        setLastRequestId(err.requestId || null);
      }
      setError(err instanceof Error ? err.message : "搜索失败。");
    } finally {
      setIsSearching(false);
    }
  }

  return (
    <div className="app-shell">
      <header className="topbar">
        <div>
          <div className="eyebrow">Langtaosha</div>
          <h1>Search Console</h1>
        </div>
        <div className="api-pill">{API_BASE_LABEL}</div>
      </header>

      <main className="workspace">
        <section className="search-panel" aria-labelledby="search-heading">
          <div className="panel-heading">
            <h2 id="search-heading">Scholar Search</h2>
            <button
              className="icon-button"
              type="button"
              onClick={() => handleSearch()}
              disabled={isSearching}
              title="Run search"
              aria-label="Run search"
            >
              {isSearching ? <RefreshCw className="spin" size={18} /> : <Search size={18} />}
            </button>
          </div>

          <form className="search-form" onSubmit={handleSearch}>
            <label className="field full-width">
              <span>Query</span>
              <input
                value={query}
                onChange={(event) => setQuery(event.target.value)}
                placeholder="Search papers, authors, concepts..."
              />
            </label>

            <label className="field">
              <span>Mode</span>
              <select value={mode} onChange={(event) => setMode(event.target.value as SearchMode)}>
                <option value="smart">Smart</option>
                <option value="vector">Vector</option>
              </select>
            </label>

            <label className="field">
              <span>Source</span>
              <select value={sourceList} onChange={(event) => setSourceList(event.target.value)}>
                {SOURCE_OPTIONS.map((option) => (
                  <option key={option.label} value={option.value}>
                    {option.label}
                  </option>
                ))}
              </select>
            </label>

            <label className="field">
              <span>Top K</span>
              <input
                min={1}
                max={100}
                type="number"
                value={topK}
                onChange={(event) => setTopK(Number(event.target.value))}
              />
            </label>

            <button className="primary-button" type="submit" disabled={isSearching}>
              {isSearching ? "Searching" : "Search"}
            </button>
          </form>

          {error ? <div className="alert">{error}</div> : null}
          {lastRequestId ? <div className="request-id">request_id: {lastRequestId}</div> : null}

          <div className="result-summary">
            <strong>{results.length}</strong>
            <span>results</span>
          </div>

          <div className="result-list">
            {results.map((result, index) => (
              <ResultItem key={result.work_id || `${result.paper_id}-${index}`} result={result} rank={index + 1} />
            ))}
            {!results.length && !isSearching ? (
              <div className="empty-state">Run a search to load results.</div>
            ) : null}
          </div>
        </section>

        <DailyNewList items={dailyNew} />
        {isLoadingDailyNew ? <div className="floating-status">Loading daily new papers</div> : null}
      </main>
    </div>
  );
}
