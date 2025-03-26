import React, { useEffect, useState } from "react";
import { useParams } from "react-router-dom";
import ReactECharts from "echarts-for-react";
import { restClient } from '@polygon.io/client-js';

// Environment variables
const REACT_APP_ALPACA_API_KEY = process.env.REACT_APP_ALPACA_API_KEY;
const REACT_APP_ALPACA_SECRET_KEY = process.env.REACT_APP_ALPACA_SECRET_KEY;
const REACT_APP_POLYGON_API_KEY = process.env.REACT_APP_POLYGON_API_KEY;

// Endpoints
const ALPACA_WSS_URL = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_REST_URL = "https://data.alpaca.markets/v2";

// Initialize Polygon API client
const polygonClient = restClient(REACT_APP_POLYGON_API_KEY);

// Utility: get today's trading hours (or last Friday if weekend)
const getTradingHoursForCurrentOrLastFriday = () => {
  const now = new Date();
  const dayOfWeek = now.getDay(); // 0=Sunday, 6=Saturday
  let effectiveDate = new Date(now);
  if (dayOfWeek === 6) {
    effectiveDate.setDate(effectiveDate.getDate() - 1);
  } else if (dayOfWeek === 0) {
    effectiveDate.setDate(effectiveDate.getDate() - 2);
  }
  const start = new Date(effectiveDate);
  start.setHours(8, 30, 0, 0);
  const end = new Date(effectiveDate);
  end.setHours(15, 0, 0, 0);
  return { start, end };
};

export default function Stock() {
  const { symbol: routeSymbol } = useParams();

  const [stock, setStock] = useState({
    symbol: routeSymbol.toUpperCase(),
    price: 0,
    change: 0,
    percent: 0,
    volume: 0,
    high: 0,
    low: 0,
    open: 0,
    history: [],
    historicalData: [],
    bars: [],
  });

  const [connectionStatus, setConnectionStatus] = useState("Disconnected");
  const [loading, setLoading] = useState(true);
  
  // News states
  const [news, setNews] = useState([]);
  const [newsLoading, setNewsLoading] = useState(true);
  const [selectedTicker, setSelectedTicker] = useState(null);
  const [sortOption, setSortOption] = useState("relevance");

  // Score articles for relevance
  const scoreArticle = (article) => {
    let score = 0;
    
    // Check article freshness
    const publishDate = new Date(article.published_utc);
    const now = new Date();
    const daysDifference = (now - publishDate) / (1000 * 60 * 60 * 24);
    
    if (daysDifference < 1) score += 15;
    else if (daysDifference < 2) score += 10;
    else if (daysDifference < 3) score += 5;
    else if (daysDifference > 7) return -1;
    
    // Boost articles mentioning multiple tickers
    if (article.tickers && article.tickers.length > 0) {
      score += Math.min(article.tickers.length * 2, 10);
    }
    
    return score;
  };

  // Fetch daily data using the 1Day timeframe (for volume, high, low, open, and price)
  const fetchDailyData = async () => {
    try {
      const url = `${ALPACA_REST_URL}/stocks/${routeSymbol}/bars?timeframe=1Day&limit=5`;
      const response = await fetch(url, {
        headers: {
          "APCA-API-KEY-ID": REACT_APP_ALPACA_API_KEY,
          "APCA-API-SECRET-KEY": REACT_APP_ALPACA_SECRET_KEY,
        },
      });
      if (!response.ok) {
        console.error("Failed to fetch daily data:", await response.text());
        return;
      }
      const data = await response.json();
      if (!data.bars || data.bars.length === 0) {
        console.log(`No valid daily bars found for ${routeSymbol}`);
        return;
      }
      const latestBar = data.bars[data.bars.length - 1];
      setStock((prev) => ({
        ...prev,
        volume: latestBar.v,
        high: latestBar.h,
        low: latestBar.l,
        open: latestBar.o,
        price: latestBar.c,
      }));
    } catch (err) {
      console.error("Error fetching daily data:", err);
    }
  };

  // Fetch historical 1Min bars for today's trading hours
  const fetchHistoricalData = async () => {
    setLoading(true);
    try {
      const { start, end } = getTradingHoursForCurrentOrLastFriday();
      const startStr = start.toISOString();
      const endStr = end.toISOString();
      const url = `${ALPACA_REST_URL}/stocks/${routeSymbol}/bars?timeframe=1Min&start=${startStr}&end=${endStr}&limit=500`;
      const response = await fetch(url, {
        headers: {
          "APCA-API-KEY-ID": REACT_APP_ALPACA_API_KEY,
          "APCA-API-SECRET-KEY": REACT_APP_ALPACA_SECRET_KEY,
        },
      });
      if (!response.ok) {
        console.error("Failed to fetch 1Min bars:", await response.text());
        return;
      }
      const data = await response.json();
      if (!data.bars || data.bars.length === 0) {
        console.log(`No 1Min bars returned for ${routeSymbol}`, data);
        return;
      }
      const historicalBars = data.bars.map((bar) => ({
        timestamp: new Date(bar.t),
        open: bar.o,
        high: bar.h,
        low: bar.l,
        close: bar.c,
        volume: bar.v,
      }));
      historicalBars.sort((a, b) => a.timestamp - b.timestamp);
      const historyPoints = historicalBars.map((bar) => ({
        timestamp: bar.timestamp,
        price: bar.close,
      }));
      const firstBar = historicalBars[0];
      const lastBar = historicalBars[historicalBars.length - 1];
      const dailyChange = lastBar.close - firstBar.close;
      const dailyPercent =
        firstBar.close > 0 ? (dailyChange / firstBar.close) * 100 : 0;
      setStock((prev) => ({
        ...prev,
        historicalData: historicalBars,
        bars: historicalBars,
        history: historyPoints,
        price: lastBar.close,
        change: dailyChange,
        percent: dailyPercent,
      }));
    } catch (err) {
      console.error("Error fetching historical data:", err);
    } finally {
      setLoading(false);
    }
  };

  // Merge real-time trade data
  const mergeDataPoint = (newDataPoint) => {
    setStock((prev) => {
      const copy = { ...prev };
      const newHistory = copy.history ? [...copy.history] : [];
      newHistory.push(newDataPoint);
      newHistory.sort((a, b) => a.timestamp - b.timestamp);
      const firstPrice = newHistory[0].price;
      const currentPrice = newDataPoint.price;
      const change = currentPrice - firstPrice;
      const percent = firstPrice > 0 ? (change / firstPrice) * 100 : 0;
      copy.history = newHistory;
      copy.price = currentPrice;
      copy.change = change;
      copy.percent = percent;
      return copy;
    });
  };

  // Merge real-time bar data
  const mergeBarData = (newBar) => {
    setStock((prev) => {
      const copy = { ...prev };
      const bars = copy.bars ? [...copy.bars] : [];
      const index = bars.findIndex(
        (b) => b.timestamp.getTime() === newBar.timestamp.getTime()
      );
      if (index >= 0) {
        bars[index] = newBar;
      } else {
        bars.push(newBar);
      }
      bars.sort((a, b) => a.timestamp - b.timestamp);
      copy.bars = bars;
      return copy;
    });
  };

  // Fetch news for a specific ticker
  const fetchNewsForTicker = async (tickerSymbol = null) => {
    try {
      setNewsLoading(true);
      
      // Build the URL based on whether we're fetching for a specific ticker
      let url = `https://api.polygon.io/v2/reference/news?limit=20&order=desc&sort=published_utc&apiKey=${REACT_APP_POLYGON_API_KEY}`;
      
      // Add ticker parameter if a ticker is specified
      if (tickerSymbol) {
        url += `&ticker=${tickerSymbol}`;
      } else if (routeSymbol) {
        // Use the route symbol if no specific ticker is provided
        url += `&ticker=${routeSymbol}`;
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
        
        // Filter relevant articles with positive scores
        const filtered = scoredArticles
          .filter(article => article._score > 0)
          .sort((a, b) => b._score - a._score);

        setNews(filtered.slice(0, 20));
      } else {
        setNews([]);
      }
    } catch (err) {
      console.error("Error fetching news:", err);
    } finally {
      setNewsLoading(false);
    }
  };

  // Handle ticker selection for news
  const getNewsByTicker = (ticker) => {
    if (!ticker) {
      setSelectedTicker(null);
      fetchNewsForTicker(routeSymbol);
      return;
    }
    
    setSelectedTicker(ticker);
    fetchNewsForTicker(ticker.ticker);
  };

  // Format date for display
  const formatDate = (dateString) => {
    const date = new Date(dateString);
    const now = new Date();
    const diffMs = now - date;
    const diffMins = Math.floor(diffMs / 60000);
    const diffHours = Math.floor(diffMins / 60);
    const diffDays = Math.floor(diffHours / 24);

    if (diffMins < 60) return `${diffMins} min${diffMins !== 1 ? 's' : ''} ago`;
    if (diffHours < 24) return `${diffHours} hour${diffHours !== 1 ? 's' : ''} ago`;
    return `${diffDays} day${diffDays !== 1 ? 's' : ''} ago`;
  };

  // Sort news based on user selection
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

  // Set up WebSocket and API requests when component mounts
  useEffect(() => {
    fetchDailyData();
    fetchHistoricalData();
    fetchNewsForTicker(routeSymbol);

    const ws = new WebSocket(ALPACA_WSS_URL);
    let subscribed = false;

    ws.onopen = () => {
      setConnectionStatus("Connected");
      ws.send(
        JSON.stringify({
          action: "auth",
          key: REACT_APP_ALPACA_API_KEY,
          secret: REACT_APP_ALPACA_SECRET_KEY,
        })
      );
    };

    ws.onmessage = (event) => {
      const processMessage = (raw) => {
        try {
          const data = JSON.parse(raw);
          if (!Array.isArray(data)) return;
          if (
            data.length > 0 &&
            data[0].T === "success" &&
            data[0].msg === "authenticated"
          ) {
            setConnectionStatus("Authenticated");
            if (!subscribed) {
              ws.send(
                JSON.stringify({
                  action: "subscribe",
                  trades: [routeSymbol],
                  bars: [routeSymbol],
                })
              );
              subscribed = true;
              setConnectionStatus("Subscribed");
            }
            return;
          }
          data.forEach((msg) => {
            if (msg.T === "t") {
              const { p, t } = msg;
              mergeDataPoint({
                timestamp: new Date(t),
                price: parseFloat(p),
              });
            } else if (msg.T === "b") {
              const { o, h, l, c, v, t } = msg;
              const date = new Date(t);
              mergeBarData({
                timestamp: date,
                open: parseFloat(o),
                high: parseFloat(h),
                low: parseFloat(l),
                close: parseFloat(c),
                volume: parseFloat(v),
              });
              mergeDataPoint({
                timestamp: date,
                price: parseFloat(c),
              });
            }
          });
        } catch (err) {
          console.error("Error parsing WS message:", err, raw);
        }
      };

      if (event.data instanceof Blob) {
        const reader = new FileReader();
        reader.onload = () => processMessage(reader.result);
        reader.readAsText(event.data);
      } else {
        processMessage(event.data);
      }
    };

    ws.onerror = (err) => {
      console.error("WebSocket error:", err);
      setConnectionStatus("Error");
    };

    ws.onclose = (ev) => {
      console.log("WebSocket closed:", ev.code, ev.reason);
      setConnectionStatus("Disconnected");
    };

    return () => ws.close();
  }, [routeSymbol]);


  const styles = {
    container: {
      fontFamily: "'Inter', sans-serif",
      color: "#fff",
      background: "linear-gradient(to bottom right, #0f172a, #1e293b 80%)",
      minHeight: "100vh",
      padding: "30px",
    },
    title: {
      fontSize: "32px",
      fontWeight: "600",
      marginBottom: "20px",
      textAlign: "center",
      letterSpacing: "1px",
    },
    detailBox: {
      background: "#1e293b",
      padding: "16px",
      borderRadius: "8px",
      minWidth: "180px",
      boxShadow: "0 4px 12px rgba(0, 0, 0, 0.3)",
      marginBottom: "16px",
    },
    row: {
      display: "flex",
      gap: "20px",
      flexWrap: "wrap",
      marginTop: "20px",
      justifyContent: "center",
    },
    newsSection: {
      marginTop: "40px",
    },
    newsHeader: {
      display: "flex",
      justifyContent: "space-between",
      alignItems: "center",
      marginBottom: "20px",
      flexWrap: "wrap",
      gap: "16px",
    },
    newsTitle: { fontSize: "24px", fontWeight: "bold" },
    ticker: { fontWeight: "bold", color: "#4c9aff" },
    companyName: { fontSize: "14px", color: "#94a3b8", marginTop: "4px" },
    selectedTicker: {
      display: "flex",
      alignItems: "center",
      backgroundColor: "#2563eb",
      borderRadius: "8px",
      padding: "8px 16px",
      marginBottom: "20px",
    },
    actions: {
      display: "flex",
      gap: "12px",
      alignItems: "center",
      flexWrap: "wrap",
    },
    sortContainer: {
      display: "flex",
      alignItems: "center",
      gap: "8px",
    },
    sortLabel: { fontSize: "14px", color: "#94a3b8" },
    select: {
      backgroundColor: "#1e293b",
      color: "white",
      border: "1px solid #475569",
      borderRadius: "6px",
      padding: "7px 12px",
      fontSize: "14px",
    },
    refreshButton: {
      backgroundColor: "#3b82f6",
      color: "white",
      border: "none",
      padding: "8px 16px",
      borderRadius: "8px",
      cursor: "pointer",
      fontWeight: "bold",
    },
    clearButton: {
      marginLeft: "auto",
      backgroundColor: "transparent",
      border: "none",
      color: "white",
      fontSize: "14px",
      cursor: "pointer",
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
      transition: "transform 0.3s, box-shadow 0.3s",
      height: "100%",
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
      justifyContent: "space-between",
    },
    description: {
      fontSize: "14px",
      color: "#cbd5e1",
      flexGrow: 1,
      marginBottom: "15px",
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
    loading: { textAlign: "center", padding: "40px" },
  };

  // Sort the news articles
  const sortedNews = sortNews(news);

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>Details for {stock.symbol}</h1>
      
      {loading ? (
        <p>Loading historical data...</p>
      ) : (
        <>
          {/* Stock price and chart section */}
          <div style={styles.detailBox}>
            <h2>{stock.symbol}</h2>
            <p style={{ color: parseFloat(stock.change) >= 0 ? "#4caf50" : "#f44336" }}>
              {parseFloat(stock.change) >= 0 ? "▲" : "▼"} {stock.change.toFixed(2)} (
              {stock.percent.toFixed(2)}%)
            </p>
            <h1>${stock.price.toFixed(2)}</h1>
            
            {/* Stock price chart */}
            <ReactECharts
              option={{
                tooltip: {
                  trigger: "axis",
                  axisPointer: { type: "cross" },
                  formatter: (params) => {
                    if (!params.length) return "";
                    const [ts, price] = params[0].data;
                    const timeString = new Date(ts).toLocaleTimeString();
                    return `<div><p>Time: ${timeString}</p><p>Price: $${price.toFixed(2)}</p></div>`;
                  },
                },
                xAxis: { type: "time" },
                yAxis: {
                  type: "value",
                  scale: true,
                  splitLine: { show: true, lineStyle: { color: "rgba(255,255,255,0.1)" } },
                  axisLabel: { formatter: (val) => "$" + val.toFixed(2), color: "rgba(255,255,255,0.7)", fontSize: 10 },
                },
                grid: { left: 50, right: 10, top: 8, bottom: 24 },
                series: [{
                  type: "line",
                  data: stock.history.map((pt) => [pt.timestamp, pt.price]),
                  smooth: true,
                  showSymbol: false,
                  lineStyle: { width: 2, color: parseFloat(stock.change) >= 0 ? "#4caf50" : "#f44336" },
                  areaStyle: {
                    color: {
                      type: "linear", x: 0, y: 0, x2: 0, y2: 1,
                      colorStops: [
                        { offset: 0, color: parseFloat(stock.change) >= 0 ? "rgba(76, 175, 80, 0.4)" : "rgba(244, 67, 54, 0.4)" },
                        { offset: 1, color: parseFloat(stock.change) >= 0 ? "rgba(76, 175, 80, 0.1)" : "rgba(244, 67, 54, 0.1)" },
                      ],
                    },
                  },
                }],
                animation: true,
              }}
              style={{ height: "200px", width: "100%" }}
            />
          </div>

          {/* Stock details section */}
          <div style={styles.row}>
            <div style={styles.detailBox}>
              <h3>Volume</h3>
              <p>{stock.volume.toLocaleString()}</p>
            </div>
            <div style={styles.detailBox}>
              <h3>High Today</h3>
              <p>${stock.high.toFixed(2)}</p>
            </div>
            <div style={styles.detailBox}>
              <h3>Low Today</h3>
              <p>${stock.low.toFixed(2)}</p>
            </div>
            <div style={styles.detailBox}>
              <h3>Open Price</h3>
              <p>${stock.open.toFixed(2)}</p>
            </div>
          </div>

          {/* News section */}
          <div style={styles.newsSection}>
            <div style={styles.newsHeader}>
              <h2 style={styles.newsTitle}>Recent News for {stock.symbol}</h2>
              <div style={styles.actions}>
                <div style={styles.sortContainer}>
                  <span style={styles.sortLabel}>Sort by:</span>
                  <select style={styles.select} value={sortOption} onChange={(e) => setSortOption(e.target.value)}>
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
                    setSelectedTicker(null);
                    fetchNewsForTicker(routeSymbol);
                  }}
                  disabled={newsLoading}
                >
                  {newsLoading ? "Refreshing..." : "Refresh News"}
                </button>
              </div>
            </div>
             
            {/* Selected ticker indicator */}
            {selectedTicker && selectedTicker.ticker !== routeSymbol && (
              <div style={styles.selectedTicker}>
                <span>Showing news for: {selectedTicker.ticker} - {selectedTicker.name}</span>
                <button style={styles.clearButton} onClick={() => getNewsByTicker(null)}>
                  Return to {routeSymbol} News
                </button>
              </div>
            )}

{newsLoading ? (
              <div style={styles.loading}>Loading news...</div>
            ) : news.length === 0 ? (
              <p>No relevant news found. Try refreshing later.</p>
            ) : (
              <div style={styles.grid}>
                {sortedNews.slice(0, 6).map((article) => (
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
                        <span>{article.publisher?.name || "Unknown Source"}</span>
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
                              const tickerObj = { ticker: ticker, name: ticker };
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
        </>
      )}
    </div>
  );
}