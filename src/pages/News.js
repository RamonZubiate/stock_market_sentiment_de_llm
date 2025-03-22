import React, { useEffect, useState } from "react";

const POLYGON_API_KEY = process.env.REACT_APP_POLYGON_API_KEY;

export default function MarketNews() {
  const [news, setNews] = useState([]);
  const [filteredNews, setFilteredNews] = useState([]);
  const [loading, setLoading] = useState(true);
  const [searchLoading, setSearchLoading] = useState(false);
  const [error, setError] = useState(null);
  const [sortOption, setSortOption] = useState("relevance");
  const [searchTerm, setSearchTerm] = useState("");
  const [searchResults, setSearchResults] = useState([]);
  const [selectedTicker, setSelectedTicker] = useState(null);
  const [searchFocused, setSearchFocused] = useState(false);
  const [tickerLoading, setTickerLoading] = useState(false);

  // Enhanced weighted keywords for scoring financial market impact
  const weightedKeywords = {
    // Economic indicators and policy
    "interest rate": 10,
    "federal reserve": 10,
    "rate hike": 10,
    "rate cut": 10,
    "inflation": 9,
    "recession": 9,
    "economic growth": 8,
    "gdp": 8,
    "central bank": 8,
    "monetary policy": 8,
    "fiscal policy": 7,
    "unemployment": 7,
    "treasury yield": 7,
    "bond market": 7,
    //add more if you like
    
    // Geopolitical factors
    "war": 9,
    "conflict": 8,
    "sanctions": 8,
    "trade war": 8,
    "trade dispute": 7,
    "supply chain": 7,
    
    // Major economies and organizations
    "china": 7,
    "european union": 7,
    "united states": 6,
    "japan": 6,
    "russia": 6, 
    "india": 5,
    "imf": 6,
    "world bank": 6,
    "opec": 6,
    "ecb": 6,
    
    // Market sectors and commodities
    "oil": 6,
    "gas": 5,
    "energy crisis": 7,
    "stock market": 6,
    "housing market": 6,
    "real estate": 5,
    "crypto": 5,
    "bitcoin": 5,
    "tech": 5,
    "semiconductor": 6,
    "chip shortage": 6,
    
    // Corporate events
    "earnings report": 5,
    "merger": 5,
    "acquisition": 5,
    "ipo": 5,
    "bankruptcy": 6,
    "layoffs": 5,
    
    // Other market movers
    "regulation": 5,
    "deregulation": 5,
    "scandal": 4,
    "lawsuit": 4,
    "default risk": 6,
    "credit rating": 5,
  };

  const scoreArticle = (article) => {
    let score = 0;
    const fieldsToSearch = [
      article.title?.toLowerCase() || "",
      article.description?.toLowerCase() || "",
      ...(article.keywords?.map((kw) => kw.toLowerCase()) || []),
    ];

    // Check if article is in English
    if (article.publisher?.locale && article.publisher.locale !== "en-US" && 
        article.publisher.locale !== "en-GB" && !article.publisher.locale.startsWith("en")) {
      return -1; // Filter out non-English articles
    }

    // Check article freshness (higher score for newer articles)
    const publishDate = new Date(article.published_utc);
    const now = new Date();
    const daysDifference = (now - publishDate) / (1000 * 60 * 60 * 24);
    
    if (daysDifference < 1) {
      score += 15; // Today's news
    } else if (daysDifference < 2) {
      score += 10; // Yesterday's news
    } else if (daysDifference < 3) {
      score += 5; // Two days old
    } else if (daysDifference > 7) {
      return -1; // Older than a week, ignore
    }

    // Score based on keywords
    for (const [keyword, weight] of Object.entries(weightedKeywords)) {
      fieldsToSearch.forEach((field) => {
        if (field.includes(keyword)) {
          score += weight;
        }
      });
    }

    // Boost articles mentioning multiple tickers
    if (article.tickers && article.tickers.length > 0) {
      score += Math.min(article.tickers.length * 2, 10);
    }

    // Reputable sources boost
    const reputableSources = ["bloomberg", "reuters", "financial times", "wall street journal", "cnbc", "ft.com", "wsj"];
    if (article.publisher?.name) {
      const publisherName = article.publisher.name.toLowerCase();
      for (const source of reputableSources) {
        if (publisherName.includes(source)) {
          score += 5;
          break;
        }
      }
    }

    return score;
  };

  const fetchMarketNews = async (tickerSymbol = null) => {
    try {
      if (tickerSymbol) {
        setTickerLoading(true);
      } else {
        setLoading(true);
      }
      setError(null);
      
      // Build the URL based on whether we're fetching for a specific ticker
      let url = `https://api.polygon.io/v2/reference/news?limit=50&order=desc&sort=published_utc&apiKey=${POLYGON_API_KEY}`;
      
      // Add ticker parameter if a ticker is specified
      if (tickerSymbol) {
        url += `&ticker=${tickerSymbol}`;
      }
      
      const res = await fetch(url);
      
      if (!res.ok) {
        throw new Error(`API returned status: ${res.status}`);
      }
      
      const data = await res.json();

      if (data.results && data.results.length > 0) {
        const scoredArticles = data.results.map(article => ({
          ...article,
          _score: scoreArticle(article)
        }));
        
        // Filter relevant English articles with positive scores
        const filtered = scoredArticles
          .filter(article => article._score > 0)
          .sort((a, b) => b._score - a._score);

        // Take top 50 articles to ensure we have plenty to display
        const topArticles = filtered.slice(0, 50);
        
        if (tickerSymbol) {
          // Update filtered news when fetching for a specific ticker
          setFilteredNews(topArticles);
        } else {
          // Update both news and filtered news when fetching all news
          setNews(topArticles);
          setFilteredNews(topArticles);
        }
      } else {
        console.warn("No news results found:", data);
        if (tickerSymbol) {
          setFilteredNews([]);
        } else {
          setNews([]);
          setFilteredNews([]);
        }
      }
    } catch (err) {
      console.error("Error fetching market news:", err);
      setError(`Failed to fetch ${tickerSymbol ? `news for ${tickerSymbol}` : 'market news'}. Please try again later.`);
    } finally {
      if (tickerSymbol) {
        setTickerLoading(false);
      } else {
        setLoading(false);
      }
    }
  };

  // Search for ticker symbols
  const searchTickers = async (query) => {
    if (!query || query.length < 1) {
      setSearchResults([]);
      return;
    }
    
    setSearchLoading(true);
    
    try {
      const url = `https://api.polygon.io/v3/reference/tickers?search=${query}&active=true&sort=ticker&order=asc&limit=10&apiKey=${POLYGON_API_KEY}`;
      const response = await fetch(url);
      
      if (!response.ok) {
        throw new Error(`Ticker search failed: ${response.status}`);
      }
      
      const data = await response.json();
      
      if (data.results && data.results.length > 0) {
        setSearchResults(data.results);
      } else {
        setSearchResults([]);
      }
    } catch (error) {
      console.error("Error searching tickers:", error);
      setSearchResults([]);
    } finally {
      setSearchLoading(false);
    }
  };

  // Updated function to fetch news for a ticker instead of filtering
  const getNewsByTicker = (ticker) => {
    if (!ticker) {
      setSelectedTicker(null);
      setFilteredNews(news); // Reset to all news
      return;
    }
    
    setSelectedTicker(ticker);
    const tickerSymbol = ticker.ticker;
    
    // Fetch news specifically for this ticker
    fetchMarketNews(tickerSymbol);
  };

  useEffect(() => {
    fetchMarketNews();
    
    // Auto-refresh every 30 minutes
    const refreshInterval = setInterval(() => fetchMarketNews(), 30 * 60 * 1000);
    
    return () => clearInterval(refreshInterval);
  }, []);

  useEffect(() => {
    // Debounce the search to avoid too many API calls
    const handler = setTimeout(() => {
      searchTickers(searchTerm);
    }, 300);
    
    return () => clearTimeout(handler);
  }, [searchTerm]);

  const formatDate = (dateString) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 60) {
      return `${diffMins} min${diffMins !== 1 ? 's' : ''} ago`;
    } else if (diffHours < 24) {
      return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
    } else {
      return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
    }
  };

  const sortNews = (newsArray) => {
    switch (sortOption) {
      case "newest":
        return [...newsArray].sort((a, b) => 
          new Date(b.published_utc) - new Date(a.published_utc)
        );
      case "oldest":
        return [...newsArray].sort((a, b) => 
          new Date(a.published_utc) - new Date(b.published_utc)
        );
      case "relevance":
        return [...newsArray].sort((a, b) => b._score - a._score);
      case "tickers":
        return [...newsArray].sort((a, b) => 
          (b.tickers?.length || 0) - (a.tickers?.length || 0)
        );
      case "source":
        return [...newsArray].sort((a, b) => 
          (a.publisher?.name || "").localeCompare(b.publisher?.name || "")
        );
      default:
        return newsArray;
    }
  };

  const styles = {
    container: {
      fontFamily: "'Inter', sans-serif",
      background: "linear-gradient(to bottom right, #0f172a, #1e293b 80%)",
      color: "#fff",
      padding: "40px",
      minHeight: "100vh",
    },
    header: {
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: "20px",
      flexWrap: "wrap",
      gap: "16px",
    },
    title: {
      fontSize: "32px",
      fontWeight: "bold",
    },
    searchContainer: {
      position: "relative",
      marginBottom: "20px",
      width: "100%",
    },
    searchInput: {
      width: "100%",
      padding: "12px 16px",
      backgroundColor: "#1e293b",
      border: "1px solid #475569",
      borderRadius: "8px",
      color: "white",
      fontSize: "16px",
      boxShadow: "0 2px 6px rgba(0, 0, 0, 0.1)",
    },
    searchResults: {
      position: "absolute",
      top: "100%",
      left: 0,
      right: 0,
      backgroundColor: "#1e293b",
      border: "1px solid #475569",
      borderRadius: "0 0 8px 8px",
      zIndex: 10,
      maxHeight: "300px",
      overflowY: "auto",
      boxShadow: "0 4px 12px rgba(0, 0, 0, 0.2)",
      display: searchFocused && searchResults.length > 0 ? "block" : "none",
    },
    searchResultItem: {
      padding: "12px 16px",
      borderBottom: "1px solid #334155",
      cursor: "pointer",
      transition: "background-color 0.2s",
    },
    searchResultItemHover: {
      backgroundColor: "#334155",
    },
    ticker: {
      fontWeight: "bold",
      color: "#4c9aff",
    },
    companyName: {
      fontSize: "14px",
      color: "#94a3b8",
      marginTop: "4px",
    },
    selectedTicker: {
      display: "flex",
      alignItems: "center",
      backgroundColor: "#2563eb",
      borderRadius: "8px",
      padding: "8px 16px",
      marginBottom: "20px",
    },
    selectedTickerText: {
      fontWeight: "bold",
      marginRight: "8px",
    },
    clearButton: {
      marginLeft: "auto",
      backgroundColor: "transparent",
      border: "none",
      color: "white",
      fontSize: "14px",
      cursor: "pointer",
      padding: "4px 8px",
    },
    searchLoading: {
      padding: "12px 16px",
      color: "#94a3b8",
      textAlign: "center",
    },
    noResults: {
      padding: "12px 16px",
      color: "#94a3b8",
      textAlign: "center",
    },
    actions: {
      display: "flex",
      gap: "12px",
      alignItems: "center",
      flexWrap: "wrap",
    },
    refreshButton: {
      backgroundColor: "#3b82f6",
      color: "white",
      border: "none",
      padding: "8px 16px",
      borderRadius: "8px",
      cursor: "pointer",
      fontWeight: "bold",
      transition: "background-color 0.2s",
    },
    sortContainer: {
      display: "flex",
      alignItems: "center",
      gap: "8px",
    },
    sortLabel: {
      fontSize: "14px",
      color: "#94a3b8",
    },
    select: {
      backgroundColor: "#1e293b",
      color: "white",
      border: "1px solid #475569",
      borderRadius: "6px",
      padding: "7px 12px",
      cursor: "pointer",
      fontSize: "14px",
    },
    lastUpdated: {
      fontSize: "14px",
      color: "#94a3b8",
      marginBottom: "20px",
      textAlign: "right",
    },
    grid: {
      display: "grid",
      gridTemplateColumns: "repeat(auto-fill, minmax(300px, 1fr))",
      gap: "24px",
    },
    card: {
      backgroundColor: "#1e293b",
      borderRadius: "16px",
      overflow: "hidden",
      boxShadow: "0 6px 18px rgba(0,0,0,0.4)",
      display: "flex",
      flexDirection: "column",
      transition: "transform 0.3s ease, box-shadow 0.3s ease",
      height: "100%",
    },
    cardHover: {
      transform: "translateY(-5px)",
      boxShadow: "0 12px 24px rgba(0,0,0,0.5)",
    },
    image: {
      width: "100%",
      height: "180px",
      objectFit: "cover",
    },
    noImage: {
      width: "100%",
      height: "80px",
      backgroundColor: "#334155",
      display: "flex",
      alignItems: "center",
      justifyContent: "center",
      color: "#94a3b8",
    },
    content: {
      padding: "20px",
      display: "flex",
      flexDirection: "column",
      flexGrow: 1,
    },
    headline: {
      fontSize: "18px",
      fontWeight: "bold",
      color: "#4c9aff",
      marginBottom: "10px",
      textDecoration: "none",
    },
    metadata: {
      fontSize: "12px",
      color: "#94a3b8",
      marginBottom: "12px",
      display: "flex",
      alignItems: "center",
      justifyContent: "space-between",
    },
    source: {
      fontWeight: "500",
    },
    description: {
      fontSize: "14px",
      color: "#cbd5e1",
      flexGrow: 1,
      marginBottom: "15px",
      lineHeight: "1.5",
    },
    tags: {
      display: "flex",
      flexWrap: "wrap",
      gap: "6px",
      marginTop: "auto",
    },
    tickerTag: {
      backgroundColor: "rgba(96, 165, 250, 0.2)",
      color: "#60a5fa",
      padding: "4px 8px",
      borderRadius: "4px",
      fontSize: "12px",
      cursor: "pointer",
    },
    selectedTickerTag: {
      backgroundColor: "rgba(96, 165, 250, 0.5)",
      color: "white",
      padding: "4px 8px",
      borderRadius: "4px",
      fontSize: "12px",
      fontWeight: "bold",
      cursor: "pointer",
    },
    keyword: {
      backgroundColor: "rgba(148, 163, 184, 0.2)",
      color: "#94a3b8",
      padding: "4px 8px",
      borderRadius: "4px",
      fontSize: "12px",
    },
    error: {
      color: "#f87171",
      textAlign: "center",
      padding: "20px",
      backgroundColor: "rgba(248, 113, 113, 0.1)",
      borderRadius: "8px",
      marginBottom: "20px",
    },
    loading: {
      textAlign: "center",
      padding: "40px",
    },
    statsBar: {
      display: "flex",
      justifyContent: "space-between",
      backgroundColor: "#1e293b",
      padding: "12px 20px",
      borderRadius: "8px",
      marginBottom: "24px",
    },
    statItem: {
      textAlign: "center",
    },
    statValue: {
      fontSize: "18px",
      fontWeight: "bold",
      color: "#4c9aff",
    },
    statLabel: {
      fontSize: "12px",
      color: "#94a3b8",
      marginTop: "4px",
    },
    noMatchingNews: {
      textAlign: "center",
      padding: "40px",
      backgroundColor: "rgba(96, 165, 250, 0.1)",
      borderRadius: "8px",
      marginBottom: "20px",
    }
  };

  // Calculate stats for the stats bar
  const todayCount = filteredNews.filter(article => {
    const pubDate = new Date(article.published_utc);
    const today = new Date();
    return pubDate.getDate() === today.getDate() && 
           pubDate.getMonth() === today.getMonth() && 
           pubDate.getFullYear() === today.getFullYear();
  }).length;

  const uniqueSources = new Set(filteredNews.map(article => article.publisher?.name)).size;
  const mentionedTickers = new Set(filteredNews.flatMap(article => article.tickers || [])).size;

  const sortedNews = sortNews(filteredNews);

  return (
    <div style={styles.container}>
      <div style={styles.header}>
        <h1 style={styles.title}>Today's Market News</h1>
        <div style={styles.actions}>
          <div style={styles.sortContainer}>
            <span style={styles.sortLabel}>Sort by:</span>
            <select 
              style={styles.select} 
              value={sortOption}
              onChange={(e) => setSortOption(e.target.value)}
            >
              <option value="relevance">Relevance</option>
              <option value="newest">Newest First</option>
              <option value="oldest">Oldest First</option>
              <option value="tickers">Most Tickers</option>
              <option value="source">Source Name</option>
            </select>
          </div>
          <button 
            style={styles.refreshButton} 
            onClick={() => {
              // Reset selected ticker when refreshing all news
              setSelectedTicker(null);
              fetchMarketNews();
            }}
            disabled={loading || tickerLoading}
          >
            {loading ? "Refreshing..." : "Refresh News"}
          </button>
        </div>
      </div>
      
      <div style={styles.searchContainer}>
        <input
          type="text"
          placeholder="Search for a stock ticker (e.g. AAPL, TSLA, MSFT)..."
          value={searchTerm}
          onChange={(e) => setSearchTerm(e.target.value)}
          style={styles.searchInput}
          onFocus={() => setSearchFocused(true)}
          onBlur={() => {
            // Delay hiding results to allow for clicking
            setTimeout(() => setSearchFocused(false), 200);
          }}
        />
        
        <div style={styles.searchResults}>
          {searchLoading ? (
            <div style={styles.searchLoading}>Searching tickers...</div>
          ) : searchResults.length === 0 && searchTerm.length > 0 ? (
            <div style={styles.noResults}>No matching tickers found</div>
          ) : (
            searchResults.map((result) => (
              <div 
                key={result.ticker} 
                style={styles.searchResultItem}
                onClick={() => {
                  getNewsByTicker(result);
                  setSearchTerm("");
                  setSearchResults([]);
                }}
                onMouseEnter={(e) => {
                  e.currentTarget.style.backgroundColor = "#334155";
                }}
                onMouseLeave={(e) => {
                  e.currentTarget.style.backgroundColor = "";
                }}
              >
                <div style={styles.ticker}>{result.ticker}</div>
                <div style={styles.companyName}>{result.name}</div>
              </div>
            ))
          )}
        </div>
      </div>
      
      {selectedTicker && (
        <div style={styles.selectedTicker}>
          <span style={styles.selectedTickerText}>
            Showing news for: {selectedTicker.ticker} - {selectedTicker.name}
          </span>
          <button 
            style={styles.clearButton}
            onClick={() => getNewsByTicker(null)}
          >
            Clear Filter
          </button>
        </div>
      )}
      
      {error && <div style={styles.error}>{error}</div>}
      
      {!loading && !tickerLoading && !error && filteredNews.length > 0 && (
        <>
          <div style={styles.statsBar}>
            <div style={styles.statItem}>
              <div style={styles.statValue}>{filteredNews.length}</div>
              <div style={styles.statLabel}>Articles</div>
            </div>
            <div style={styles.statItem}>
              <div style={styles.statValue}>{todayCount}</div>
              <div style={styles.statLabel}>Today's News</div>
            </div>
            <div style={styles.statItem}>
              <div style={styles.statValue}>{uniqueSources}</div>
              <div style={styles.statLabel}>Sources</div>
            </div>
            <div style={styles.statItem}>
              <div style={styles.statValue}>{mentionedTickers}</div>
              <div style={styles.statLabel}>Tickers Mentioned</div>
            </div>
          </div>
          
          <div style={styles.lastUpdated}>
            Last updated: {new Date().toLocaleTimeString()}
          </div>
        </>
      )}

      {loading ? (
        <div style={styles.loading}>Loading market news...</div>
      ) : tickerLoading ? (
        <div style={styles.loading}>Loading news for {selectedTicker.ticker}...</div>
      ) : selectedTicker && filteredNews.length === 0 ? (
        <div style={styles.noMatchingNews}>
          <p>No news found for {selectedTicker.ticker} ({selectedTicker.name}).</p>
          <button 
            style={styles.refreshButton}
            onClick={() => getNewsByTicker(null)}
          >
            Show All News
          </button>
        </div>
      ) : filteredNews.length === 0 ? (
        <p>No relevant market news found. Try refreshing later.</p>
      ) : (
        <div style={styles.grid}>
          {sortedNews.map((article) => (
            <div 
              key={article.id} 
              style={styles.card}
              onMouseEnter={(e) => {
                e.currentTarget.style.transform = "translateY(-5px)";
                e.currentTarget.style.boxShadow = "0 12px 24px rgba(0,0,0,0.5)";
              }}
              onMouseLeave={(e) => {
                e.currentTarget.style.transform = "";
                e.currentTarget.style.boxShadow = "";
              }}
            >
              {article.image_url ? (
                <img 
                  src={article.image_url} 
                  alt={article.title || "News"} 
                  style={styles.image}
                  onError={(e) => {
                    e.target.style.display = "none";
                    e.target.parentNode.insertBefore(
                      Object.assign(document.createElement("div"), {
                        textContent: "Image Unavailable",
                        className: "noImage",
                        style: Object.entries(styles.noImage).reduce((acc, [key, value]) => {
                          acc[key] = value;
                          return acc;
                        }, {})
                      }),
                      e.target.nextSibling
                    );
                  }}
                />
              ) : (
                <div style={styles.noImage}>No Image Available</div>
              )}

              <div style={styles.content}>
                <a
                  href={article.article_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={styles.headline}
                >
                  {article.title}
                </a>

                <div style={styles.metadata}>
                  <span style={styles.source}>{article.publisher?.name || "Unknown Source"}</span>
                  <span>{formatDate(article.published_utc)}</span>
                </div>

                <div style={styles.description}>
                  {article.description || "No description available."}
                </div>

                <div style={styles.tags}>
                  {article.tickers?.slice(0, 5).map(ticker => (
                    <span 
                      key={ticker} 
                      style={selectedTicker && ticker === selectedTicker.ticker ? 
                        styles.selectedTickerTag : styles.tickerTag}
                      onClick={() => {
                        // Create a ticker object and fetch news for it
                        const tickerObj = { 
                          ticker: ticker, 
                          name: ticker // Use ticker as name if we don't have the full name
                        };
                        getNewsByTicker(tickerObj);
                      }}
                    >
                      {ticker}
                    </span>
                  ))}
                  
                  {article.tickers?.length > 5 && (
                    <span style={styles.tickerTag}>+{article.tickers.length - 5} more</span>
                  )}
                  
                  {article.keywords?.slice(0, 3).map(keyword => (
                    <span key={keyword} style={styles.keyword}>{keyword}</span>
                  ))}
                </div>
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}