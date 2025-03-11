import React, { useEffect, useState } from "react";
import ReactECharts from "echarts-for-react";

const REACT_APP_ALPACA_API_KEY = process.env.REACT_APP_ALPACA_API_KEY;
const REACT_APP_ALPACA_SECRET_KEY = process.env.REACT_APP_ALPACA_SECRET_KEY;

// Use the test API for development
const ALPACA_WSS_URL = "wss://stream.data.alpaca.markets/v2/iex";
const ALPACA_REST_URL = "https://data.alpaca.markets/v2";

// Stock symbols to track
const stockSymbols = ["SPY", "AAPL"];

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

  // Function to get trading hours start/end times for today
  const getTradingHoursToday = () => {
    const now = new Date();
  
    // Create start time at 9:30 AM today
    const start = new Date(now);
    start.setHours(8, 30, 0, 0); // 9:30 AM
  
    // Create end time at 4:00 PM today
    const end = new Date(now);
    end.setHours(15, 0, 0, 0); // 4:00 PM
  
    return { start, end };
  };
  

  // Function to fetch historical data
  const fetchHistoricalData = async () => {
    setLoading(true);
    try {
      for (const symbol of stockSymbols) {
        // Get today's trading hours
        const { start, end } = getTradingHoursToday();

        // Format dates for API
        const startStr = start.toISOString();
        const endStr = end.toISOString();

        // Use 1Min timeframe to get more granular data for the custom timeframe
        const url = `${ALPACA_REST_URL}/stocks/${symbol}/bars?timeframe=1Min&start=${startStr}&end=${endStr}&limit=500`;

        const response = await fetch(url, {
          headers: {
            "APCA-API-KEY-ID": REACT_APP_ALPACA_API_KEY,
            "APCA-API-SECRET-KEY": REACT_APP_ALPACA_SECRET_KEY,
          },
        });

        if (response.ok) {
          const data = await response.json();

          if (data.bars && data.bars.length > 0) {
            setStocks((prev) => {
              const updatedStock = { ...prev };

              // Process historical bars
              const historicalBars = data.bars.map((bar) => {
                const utcDate = new Date(bar.t);
                const localDate = new Date(
                  utcDate.toLocaleString("en-US", {
                    timeZone: "America/New_York",
                  })
                ); // Change to your timezone

                return {
                  time: localDate.toLocaleTimeString(),
                  timestamp: localDate, // Store the local timestamp
                  open: bar.o,
                  high: bar.h,
                  low: bar.l,
                  close: bar.c,
                  volume: bar.v,
                };
              });

              // Sort by timestamp
              historicalBars.sort((a, b) => a.timestamp - b.timestamp);

              // Create history array for line chart
              const historyPoints = historicalBars.map((bar) => ({
                time: bar.time,
                timestamp: bar.timestamp,
                price: bar.close,
              }));

              // Update the stock data
              updatedStock[symbol].historicalData = historicalBars;
              updatedStock[symbol].history = historyPoints;
              updatedStock[symbol].bars = historicalBars;

              // Set current price to the last close
              if (historicalBars.length > 0) {
                const lastBar = historicalBars[historicalBars.length - 1];
                const firstBar = historicalBars[0];

                const change = lastBar.close - firstBar.close;
                const percentChange =
                  firstBar.close > 0 ? (change / firstBar.close) * 100 : 0;

                updatedStock[symbol].price = lastBar.close.toFixed(2);
                updatedStock[symbol].change = change.toFixed(2);
                updatedStock[symbol].percent = percentChange.toFixed(2);
              }

              return updatedStock;
            });
          } else {
            console.log(
              "No bars returned from API for the specified timeframe"
            );
          }
        } else {
          console.error(
            "Failed to fetch historical data:",
            await response.text()
          );
        }
      }
    } catch (error) {
      console.error("Error fetching historical data:", error);
    } finally {
      setLoading(false);
    }
  };

  // Fetch historical data when component mounts
  useEffect(() => {
    fetchHistoricalData();

    // Set up a refresh interval (every 5 minutes)
    const refreshInterval = setInterval(() => {
      fetchHistoricalData();
    }, 5 * 60 * 1000);

    return () => clearInterval(refreshInterval);
  }, []);

  // Function to check if a timestamp is within today's trading hours
  const isWithinTradingHours = (timestamp) => {
    const localDate = new Date(
      timestamp.toLocaleString("en-US", { timeZone: "America/New_York" })
    ); // Convert to local time
    const { start, end } = getTradingHoursToday();

    console.log(
      `Checking timestamp ${localDate} within range: ${start} - ${end}`
    );

    return localDate >= start && localDate <= end;
  };

  // Function to merge real-time data with historical data
  const mergeDataPoint = (symbol, newDataPoint) => {
    // Only process data points within trading hours
    if (!isWithinTradingHours(newDataPoint.timestamp)) {
      return;
    }

    setStocks((prev) => {
      const updatedStock = { ...prev };

      // Add to history
      const history = [...(updatedStock[symbol].history || [])];
      history.push(newDataPoint);

      // Keep only data within today's trading hours
      const { start, end } = getTradingHoursToday();
      const filteredHistory = history.filter(
        (item) => item.timestamp >= start && item.timestamp <= end
      );

      // Calculate price change against the first price of the day
      const currentPrice = parseFloat(newDataPoint.price);
      const historicalData = updatedStock[symbol].historicalData || [];
      const firstPrice =
        historicalData.length > 0
          ? historicalData[0].close
          : parseFloat(updatedStock[symbol].price) || currentPrice;

      const change = currentPrice - firstPrice;
      const percentChange = firstPrice > 0 ? (change / firstPrice) * 100 : 0;

      updatedStock[symbol] = {
        ...updatedStock[symbol],
        price: currentPrice.toFixed(2),
        change: change.toFixed(2),
        percent: percentChange.toFixed(2),
        history: filteredHistory,
      };

      return updatedStock;
    });
  };

  // Function to merge real-time bar with historical bars
  const mergeBarData = (symbol, newBar) => {
    // Only process bars within trading hours
    if (!isWithinTradingHours(newBar.timestamp)) {
      return;
    }

    setStocks((prev) => {
      const updatedStock = { ...prev };

      // Add to bars
      const bars = [...(updatedStock[symbol].bars || [])];

      // Check if we have a bar for this timestamp already and update it
      const existingIndex = bars.findIndex(
        (bar) => bar.timestamp.getTime() === newBar.timestamp.getTime()
      );

      if (existingIndex >= 0) {
        // Update existing bar
        bars[existingIndex] = newBar;
      } else {
        // Add new bar
        bars.push(newBar);
      }

      // Keep only data within today's trading hours
      const { start, end } = getTradingHoursToday();
      const filteredBars = bars.filter(
        (bar) => bar.timestamp >= start && bar.timestamp <= end
      );

      // Sort by timestamp
      filteredBars.sort((a, b) => a.timestamp - b.timestamp);

      // Update the stock data
      updatedStock[symbol].bars = filteredBars;

      return updatedStock;
    });
  };

  // Set up WebSocket connection
  useEffect(() => {
    const ws = new WebSocket(ALPACA_WSS_URL);

    ws.onopen = () => {
      console.log("Connected to Alpaca WebSocket");
      setConnectionStatus("Connected");

      // Authenticate
      ws.send(
        JSON.stringify({
          action: "auth",
          key: REACT_APP_ALPACA_API_KEY,
          secret: REACT_APP_ALPACA_SECRET_KEY,
        })
      );
    };

    let subscribed = false;

    ws.onmessage = (event) => {
      const processMessage = (messageData) => {
        try {
          console.log("Raw message:", messageData);
          const data = JSON.parse(messageData);
          console.log("Parsed data:", data);

          // Handle authentication response
          if (
            Array.isArray(data) &&
            data.length > 0 &&
            data[0].T === "success" &&
            data[0].msg === "authenticated"
          ) {
            console.log("Successfully authenticated with Alpaca");
            setConnectionStatus("Authenticated");

            if (!subscribed) {
              // Subscribe to both trades and bars
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

          if (Array.isArray(data)) {
            data.forEach((message) => {
              // Handle trade updates
              if (message.T === "t") {
                const { S: symbol, p: price, t: timestamp } = message;

                if (stocks[symbol]) {
                  const currentPrice = parseFloat(price);
                  const date = new Date(timestamp);
                  const timeString = date.toLocaleTimeString();

                  // Create data point for the line chart
                  const newDataPoint = {
                    time: timeString,
                    price: currentPrice,
                    timestamp: date,
                  };

                  // Merge with historical data
                  mergeDataPoint(symbol, newDataPoint);
                }
              }

              // Handle bar updates
              if (message.T === "b") {
                const {
                  S: symbol,
                  o: open,
                  h: high,
                  l: low,
                  c: close,
                  v: volume,
                  t: timestamp,
                } = message;

                if (stocks[symbol]) {
                  const date = new Date(timestamp);
                  const timeString = date.toLocaleTimeString();

                  // Create a new bar
                  const newBar = {
                    time: timeString,
                    timestamp: date,
                    open: parseFloat(open),
                    high: parseFloat(high),
                    low: parseFloat(low),
                    close: parseFloat(close),
                    volume: parseFloat(volume),
                  };

                  // Merge with historical bars
                  mergeBarData(symbol, newBar);

                  // Also update the price data
                  const newDataPoint = {
                    time: timeString,
                    price: parseFloat(close),
                    timestamp: date,
                  };
                  mergeDataPoint(symbol, newDataPoint);
                }
              }
            });
          }
        } catch (error) {
          console.error("Error processing message:", error);
          console.log("Raw message:", messageData);
        }
      };

      if (event.data instanceof Blob) {
        const reader = new FileReader();
        reader.onload = () => {
          processMessage(reader.result);
        };
        reader.readAsText(event.data);
      } else {
        processMessage(event.data);
      }
    };

    ws.onerror = (error) => {
      console.error("WebSocket Error:", error);
      setConnectionStatus("Error");
    };

    ws.onclose = (event) => {
      console.log(
        "Disconnected from Alpaca WebSocket:",
        event.code,
        event.reason
      );
      setConnectionStatus("Disconnected");
    };

    return () => {
      ws.close();
    };
  }, []);

  return (
    <div style={styles.container}>
      <h1 style={styles.title}>📈 Stock Market Overview</h1>

      <div style={styles.statusBar}>
        <span>
          Connection: <strong>{connectionStatus}</strong>
        </span>
        <span>
          Trading Hours: <strong>9:30 AM - 4:00 PM EST</strong>
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
            <div key={symbol} style={styles.card}>
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
                      formatter: function (params) {
                        const index = params[0].dataIndex; // Get hovered data index
                        const time = stock.history[index]?.time || "Unknown"; // Retrieve time
                        const price = params[0].data;

                        return `<div>
                        <p>Time: ${time}</p>
                        <p>Price: $${typeof price === "number" ? price.toFixed(2) : price}</p>
                      </div>`;
                      },
                    },
                    xAxis: {
                      type: "category",
                      data: stock.history.map((_, index) => index), // Use index instead of time
                      show: false, // Hide the time axis
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
                        formatter: (value) => "$" + value.toFixed(2),
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
                        data: stock.history.map((h) => h.price),
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
          ))}
        </div>
      )}
    </div>
  );
};

// Real-Time Stock Cards with Dark Blue Theme
const styles = {
  container: {
    fontFamily: "'Inter', sans-serif",
    padding: "40px",
    marginLeft: "220px",
    background: "linear-gradient(to bottom, #0f172a, #1e293b)", // Dark blue gradient
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
    background: "linear-gradient(to bottom,rgb(77, 99, 153), #1e293b)",
    padding: "20px",
    borderRadius: "12px",
    boxShadow: "0px 6px 20px rgba(0, 191, 255, 0.15)", // Soft glow effect
    transition: "transform 0.3s ease-in-out, box-shadow 0.3s ease-in-out",
    cursor: "pointer",
    border: "1px solid rgba(255, 255, 255, 0.1)",
    backdropFilter: "blur(8px)", // Frosted glass effect
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
  timeInfo: {
    fontSize: "12px",
    color: "rgba(255, 255, 255, 0.7)",
    textAlign: "center",
    marginTop: "8px",
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
