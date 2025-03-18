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
  const [news, setNews] = useState([]);

  // Fetch daily data using the 1Day timeframe (for volume, high, low, open, and price)
  const fetchDailyData = async () => {
    try {
      // Use limit=5 to be safe; we then take the latest available daily bar
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

  // Set up WebSocket for real-time trades and bars
  useEffect(() => {
    fetchDailyData();
    fetchHistoricalData();

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

  // Fetch news using Polygon.io client-js library
  const fetchPolygonNews = async () => {
    try {
      const response = await polygonClient.reference.tickerNews({
        ticker: routeSymbol.toUpperCase(),
        order: "desc", // Most recent first
        limit: 5,
        sort: "published_utc"
      });
      
      if (!response.results || response.results.length === 0) {
        console.log(`No news found for ${routeSymbol}`);
        return;
      }

      // Filter articles: Only include those with a valid published date and where tickers include the symbol
      const filteredArticles = response.results.filter((article) => {
        // Check published date validity
        const pubDate = new Date(article.published_utc);
        if (pubDate.toString() === "Invalid Date") return false;
        // Ensure the symbol appears in the tickers array (case-insensitive)
        if (article.tickers && article.tickers.length > 0) {
          return article.tickers.some(
            (ticker) => ticker.toUpperCase() === routeSymbol.toUpperCase()
          );
        }
        return false;
      });

      setNews(filteredArticles);
    } catch (err) {
      console.error("Error fetching Polygon news:", err);
    }
  };

  useEffect(() => {
    fetchPolygonNews();
  }, [routeSymbol]);

  // Basic styling
  const styles = {
    container: {
      fontFamily: "'Inter', sans-serif",
      color: "#fff",
      background: "linear-gradient(to bottom right, #0f172a, #1e293b 80%)",
      minHeight: "100vh",
      padding: "50px",
    },
    title: {
      fontSize: "32px",
      fontWeight: "600",
      marginBottom: "10px",
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
      marginTop: "30px",
    },
    newsItem: {
      backgroundColor: "#1e293b",
      borderRadius: "8px",
      padding: "16px",
      marginBottom: "10px",
      boxShadow: "0 2px 8px rgba(0, 0, 0, 0.3)",
    },
    newsItemTitle: {
      fontSize: "18px",
      color: "#4c9aff",
      marginBottom: "8px",
    },
  };

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>Details for {stock.symbol}</h1>
      {loading ? (
        <p>Loading historical data...</p>
      ) : (
        <>
          <div style={styles.detailBox}>
            <h2>{stock.symbol}</h2>
            <p style={{ color: parseFloat(stock.change) >= 0 ? "#4caf50" : "#f44336" }}>
              {parseFloat(stock.change) >= 0 ? "▲" : "▼"} {stock.change.toFixed(2)} (
              {stock.percent.toFixed(2)}%)
            </p>
            <h1>${stock.price.toFixed(2)}</h1>
            {/* Mini line chart */}
            <ReactECharts
              option={{
                tooltip: {
                  trigger: "axis",
                  axisPointer: { type: "cross" },
                  formatter: (params) => {
                    if (!params.length) return "";
                    const [ts, price] = params[0].data;
                    const timeString = new Date(ts).toLocaleTimeString();
                    return `
                      <div>
                        <p>Time: ${timeString}</p>
                        <p>Price: $${price.toFixed(2)}</p>
                      </div>
                    `;
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
                series: [
                  {
                    type: "line",
                    data: stock.history.map((pt) => [pt.timestamp, pt.price]),
                    smooth: true,
                    showSymbol: false,
                    lineStyle: {
                      width: 2,
                      color: parseFloat(stock.change) >= 0 ? "#4caf50" : "#f44336",
                    },
                    areaStyle: {
                      color: {
                        type: "linear",
                        x: 0,
                        y: 0,
                        x2: 0,
                        y2: 1,
                        colorStops: [
                          { offset: 0, color: parseFloat(stock.change) >= 0 ? "rgba(76, 175, 80, 0.4)" : "rgba(244, 67, 54, 0.4)" },
                          { offset: 1, color: parseFloat(stock.change) >= 0 ? "rgba(76, 175, 80, 0.1)" : "rgba(244, 67, 54, 0.1)" },
                        ],
                      },
                    },
                  },
                ],
                animation: true,
              }}
              style={{ height: "200px", width: "100%" }}
            />
          </div>

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

          <div style={styles.newsSection}>
            <h2>Recent News for {stock.symbol}</h2>
            {news.length === 0 && <p>No recent news found.</p>}
            {news.map((article) => (
              <div key={article.id} style={styles.newsItem}>
                <a
                  href={article.article_url}
                  target="_blank"
                  rel="noopener noreferrer"
                  style={styles.newsItemTitle}
                >
                  {article.title}
                </a>
                <p>{article.description}</p>
                <p style={{ fontSize: "12px" }}>
                  Source: {article.publisher.name} –{" "}
                  {new Date(article.published_utc).toLocaleString()}
                </p>
              </div>
            ))}
          </div>
        </>
      )}
    </div>
  );
}