import React, { useEffect, useState } from "react";
import { Link } from "react-router-dom"; // <-- ONLY ADDITION
import ReactECharts from "echarts-for-react";

const REACT_APP_ALPACA_API_KEY = process.env.REACT_APP_ALPACA_API_KEY;
const REACT_APP_ALPACA_SECRET_KEY = process.env.REACT_APP_ALPACA_SECRET_KEY;

const ALPACA_WSS_URL = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_REST_URL = "https://data.alpaca.markets/v2";

const stockSymbols = [
  "AAPL",
  "SPY",
  "MSFT",
  "AMZN",
  "GOOGL",
  "TSLA",
  "NFLX",
  "META",
  "NVDA",
  "AMD",
  "INTC",
  "DIS",
  "V",
  "JPM",
  "BA",
  "WMT",
  "PG",
  "KO",
  "PEP",
  "XOM",
  "CVX",
];

const Stocks = () => {
  const [stocks, setStocks] = useState(
    stockSymbols.reduce((acc, symbol) => {
      acc[symbol] = {
        price: 0,
        change: 0,
        percent: 0,
        history: [],
        historicalData: [],
        bars: [],
      };
      return acc;
    }, {})
  );

  const [connectionStatus, setConnectionStatus] = useState("Disconnected");
  const [loading, setLoading] = useState(true);

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

  const fetchHistoricalData = async () => {
    setLoading(true);
    try {
      for (const symbol of stockSymbols) {
        const { start, end } = getTradingHoursForCurrentOrLastFriday();
        const startStr = start.toISOString();
        const endStr = end.toISOString();

        const url = `${ALPACA_REST_URL}/stocks/${symbol}/bars?timeframe=1Min&start=${startStr}&end=${endStr}&limit=500`;

        const response = await fetch(url, {
          headers: {
            "APCA-API-KEY-ID": REACT_APP_ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": REACT_APP_ALPACA_SECRET_KEY,
          },
        });

        if (!response.ok) {
          console.error("Failed to fetch bars:", await response.text());
          continue;
        }

        const data = await response.json();
        if (!data.bars || data.bars.length === 0) {
          console.log(`No bars returned for ${symbol}`, data);
          continue;
        }

        const historicalBars = data.bars.map((bar) => {
          const utcDate = new Date(bar.t);
          const localDate = new Date(
            utcDate.toLocaleString("en-US", {
              timeZone: "America/New_York",
            })
          );
          return {
            timestamp: localDate,
            open: bar.o,
            high: bar.h,
            low: bar.l,
            close: bar.c,
            volume: bar.v,
          };
        });

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

        setStocks((prev) => {
          const updated = { ...prev };
          updated[symbol].historicalData = historicalBars;
          updated[symbol].bars = historicalBars;
          updated[symbol].history = historyPoints;
          updated[symbol].price = lastBar.close.toFixed(2);
          updated[symbol].change = dailyChange.toFixed(2);
          updated[symbol].percent = dailyPercent.toFixed(2);
          return updated;
        });
      }
    } catch (err) {
      console.error("Error fetching historical data:", err);
    } finally {
      setLoading(false);
    }
  };

  useEffect(() => {
    fetchHistoricalData();

    const refreshInterval = setInterval(() => {
      fetchHistoricalData();
    }, 5 * 60 * 1000);

    return () => clearInterval(refreshInterval);
  }, []);

  // Merge real-time trades
  const mergeDataPoint = (symbol, newDataPoint) => {
    setStocks((prev) => {
      const copy = { ...prev };
      const stock = copy[symbol];

      const newHistory = stock.history ? [...stock.history] : [];
      newHistory.push({
        timestamp: newDataPoint.timestamp,
        price: newDataPoint.price,
      });

      newHistory.sort((a, b) => a.timestamp - b.timestamp);

      const firstPrice = newHistory[0].price;
      const change = newDataPoint.price - firstPrice;
      const percent = firstPrice > 0 ? (change / firstPrice) * 100 : 0;

      stock.history = newHistory;
      stock.price = newDataPoint.price.toFixed(2);
      stock.change = change.toFixed(2);
      stock.percent = percent.toFixed(2);

      return copy;
    });
  };

  // Merge real-time bars
  const mergeBarData = (symbol, newBar) => {
    setStocks((prev) => {
      const copy = { ...prev };
      const stock = copy[symbol];
      const bars = [...(stock.bars || [])];

      const index = bars.findIndex(
        (b) => b.timestamp.getTime() === newBar.timestamp.getTime()
      );
      if (index >= 0) {
        bars[index] = newBar;
      } else {
        bars.push(newBar);
      }

      bars.sort((a, b) => a.timestamp - b.timestamp);
      stock.bars = bars;

      return copy;
    });
  };

  useEffect(() => {
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
      const processMessage = (msgString) => {
        try {
          const data = JSON.parse(msgString);
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
                  trades: stockSymbols,
                  bars: stockSymbols,
                })
              );
              subscribed = true;
              setConnectionStatus("Subscribed");
            }
            return;
          }

          data.forEach((message) => {
            if (message.T === "t") {
              const { S: symbol, p, t } = message;
              const date = new Date(t);
              mergeDataPoint(symbol, {
                timestamp: date,
                price: parseFloat(p),
              });
            } else if (message.T === "b") {
              const { S: symbol, o, h, l, c, v, t } = message;
              const date = new Date(t);
              const newBar = {
                timestamp: date,
                open: parseFloat(o),
                high: parseFloat(h),
                low: parseFloat(l),
                close: parseFloat(c),
                volume: parseFloat(v),
              };
              mergeBarData(symbol, newBar);
              mergeDataPoint(symbol, {
                timestamp: date,
                price: parseFloat(c),
              });
            }
          });
        } catch (err) {
          console.error("Error parsing WebSocket message:", err, msgString);
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

    ws.onclose = (event) => {
      console.log("WebSocket closed:", event.code, event.reason);
      setConnectionStatus("Disconnected");
    };

    return () => ws.close();
  }, []);

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>📈 Stock Market Overview</h1>

      <div style={styles.statusBar}>
        <span>
          Connection: <strong>{connectionStatus}</strong>
        </span>
        <span>
          Trading Hours: <strong>8:30 AM - 3:00 PM</strong>
        </span>
        <button
          onClick={fetchHistoricalData}
          style={styles.refreshButton}
          disabled={loading}
        >
          {loading ? "Refreshing..." : "Refresh Data"}
        </button>
      </div>

      {loading ? (
        <div style={styles.loadingContainer}>
          <div style={styles.loadingSpinner}></div>
          <p>Loading historical data...</p>
        </div>
      ) : (
        <div style={styles.gridContainer}>
          {Object.entries(stocks).map(([symbol, stock]) => (
            // Wrap each card in a Link to go to /stock/:symbol
            <Link
              key={symbol}
              to={`/stock/${symbol}`}
              style={{ textDecoration: "none", color: "inherit" }}
            >
              <div style={styles.card}>
                <div style={styles.stockHeader}>
                  <h2 style={styles.stockSymbol}>{symbol}</h2>
                  <span
                    style={{
                      ...styles.priceChange,
                      color:
                        parseFloat(stock.change) >= 0 ? "#4caf50" : "#f44336",
                    }}
                  >
                    {parseFloat(stock.change) >= 0 ? "▲" : "▼"} {stock.change} (
                    {stock.percent}%)
                  </span>
                </div>
                <h3 style={styles.stockPrice}>${stock.price}</h3>

                <div style={styles.chartContainer}>
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
                      xAxis: {
                        type: "time",
                      },
                      yAxis: {
                        type: "value",
                        scale: true,
                        splitLine: {
                          show: true,
                          lineStyle: {
                            color: "rgba(255, 255, 255, 0.1)",
                          },
                        },
                        axisLabel: {
                          formatter: (val) => "$" + val.toFixed(2),
                          color: "rgba(255, 255, 255, 0.7)",
                          fontSize: 10,
                        },
                      },
                      grid: {
                        left: 50,
                        right: 10,
                        top: 8,
                        bottom: 24,
                      },
                      series: [
                        {
                          type: "line",
                          data: stock.history.map((pt) => [
                            pt.timestamp,
                            pt.price,
                          ]),
                          smooth: true,
                          showSymbol: false,
                          lineStyle: {
                            width: 2,
                            color:
                              parseFloat(stock.change) >= 0
                                ? "#4caf50"
                                : "#f44336",
                          },
                          areaStyle: {
                            color: {
                              type: "linear",
                              x: 0,
                              y: 0,
                              x2: 0,
                              y2: 1,
                              colorStops: [
                                {
                                  offset: 0,
                                  color:
                                    parseFloat(stock.change) >= 0
                                      ? "rgba(76, 175, 80, 0.4)"
                                      : "rgba(244, 67, 54, 0.4)",
                                },
                                {
                                  offset: 1,
                                  color:
                                    parseFloat(stock.change) >= 0
                                      ? "rgba(76, 175, 80, 0.1)"
                                      : "rgba(244, 67, 54, 0.1)",
                                },
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
              </div>
            </Link>
          ))}
        </div>
      )}
    </div>
  );
};

// EXACT SAME STYLES, UNMODIFIED
const styles = {
  container: {
    fontFamily: "'Inter', sans-serif",
    padding: "40px",
    marginLeft: "220px",
    background: "linear-gradient(to bottom, #0f172a, #1e293b)",
    minHeight: "100vh",
    color: "#fff",
  },
  title: {
    fontSize: "32px",
    fontWeight: "600",
    marginBottom: "10px",
    textAlign: "center",
    letterSpacing: "1px",
  },
  statusBar: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: "20px",
    padding: "8px 16px",
    backgroundColor: "rgba(255, 255, 255, 0.1)",
    borderRadius: "8px",
  },
  refreshButton: {
    backgroundColor: "#4c6ef5",
    color: "white",
    border: "none",
    padding: "6px 12px",
    borderRadius: "4px",
    cursor: "pointer",
    transition: "background-color 0.2s",
    fontWeight: "500",
  },
  gridContainer: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(400px, 1fr))",
    gap: "24px",
  },
  card: {
    background: "linear-gradient(to bottom, rgb(77, 99, 153), #1e293b)",
    padding: "20px",
    borderRadius: "12px",
    boxShadow: "0px 6px 20px rgba(0, 191, 255, 0.15)",
    transition: "transform 0.3s ease-in-out, box-shadow 0.3s ease-in-out",
    cursor: "pointer",
    border: "1px solid rgba(255, 255, 255, 0.1)",
    backdropFilter: "blur(8px)",
  },
  stockHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: "10px",
  },
  stockSymbol: {
    fontSize: "22px",
    fontWeight: "bold",
  },
  priceChange: {
    fontSize: "16px",
    fontWeight: "bold",
  },
  stockPrice: {
    fontSize: "24px",
    fontWeight: "bold",
    marginBottom: "10px",
  },
  chartContainer: {
    width: "100%",
    height: "200px",
  },
  loadingContainer: {
    display: "flex",
    flexDirection: "column",
    alignItems: "center",
    justifyContent: "center", 
    height: "200px",
  },
  loadingSpinner: {
    border: "4px solid rgba(255, 255, 255, 0.1)",
    borderTop: "4px solid #3498db",
    borderRadius: "50%",
    width: "40px",
    height: "40px",
    animation: "spin 1s linear infinite",
    marginBottom: "16px",
  },
  "@keyframes spin": {
    "0%": { transform: "rotate(0deg)" },
    "100%": { transform: "rotate(360deg)" },
  },
};

export default Stocks;
