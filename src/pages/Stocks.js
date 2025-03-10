import React from "react";
import ReactECharts from "echarts-for-react";

// Function to generate realistic stock price fluctuations
const generateStockHistory = (initialPrice, count = 10000) => {
  let data = [];
  let price = initialPrice;

  for (let i = 0; i < count; i++) {
    let change = (Math.random() - 0.5) * 8; // Larger fluctuations for realism
    price += change;
    price = Math.max(price, 1);
    data.push({ time: `T${i}`, price: parseFloat(price.toFixed(2)) });
  }

  return data;
};

// Test stock data with larger price fluctuations
const testStockData = [
  {
    symbol: "AAPL",
    price: 145.32,
    change: 2.10,
    percent: 1.46,
    history: generateStockHistory(145.32),
  },
  {
    symbol: "TSLA",
    price: 234.50,
    change: -6.45,
    percent: -2.67,
    history: generateStockHistory(234.50),
  },
  {
    symbol: "AMZN",
    price: 109.87,
    change: 3.25,
    percent: 3.04,
    history: generateStockHistory(109.87),
  },
];

const Stocks = () => {
  return (
    <div style={styles.container}>
      <h1 style={styles.title}>📈 Stock Market Overview</h1>
      <div style={styles.gridContainer}>
        {testStockData.map((stock) => (
          <div key={stock.symbol} style={styles.card}>
            <div style={styles.stockHeader}>
              <h2 style={styles.stockSymbol}>{stock.symbol}</h2>
              <span style={{ ...styles.priceChange, color: stock.change >= 0 ? "#4caf50" : "#f44336" }}>
                {stock.change >= 0 ? "▲" : "▼"} {stock.change.toFixed(2)} ({stock.percent.toFixed(2)}%)
              </span>
            </div>
            <h3 style={styles.stockPrice}>${stock.price.toFixed(2)}</h3>
            <div style={styles.chartContainer}>
              <ReactECharts
                option={{
                  xAxis: { type: "category", data: stock.history.map((point) => point.time), show: false },
                  yAxis: { type: "value", show: false },
                  series: [
                    {
                      type: "line",
                      data: stock.history.map((point) => point.price),
                      smooth: true,
                      symbol: "none",
                      lineStyle: { width: 2, color: stock.change >= 0 ? "#4caf50" : "#f44336" },
                      areaStyle: { color: stock.change >= 0 ? "#4caf5030" : "#f4433630" },
                    },
                  ],
                }}
                style={{ height: "80px", width: "100%" }}
              />
            </div>
          </div>
        ))}
      </div>
    </div>
  );
};

// Stylish Stock Cards with Dark Blue Theme
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
    marginBottom: "20px",
    textAlign: "center",
    letterSpacing: "1px",
  },
  gridContainer: {
    display: "grid",
    gridTemplateColumns: "repeat(auto-fit, minmax(320px, 1fr))",
    gap: "24px",
  },
  card: {
    background: "linear-gradient(to bottom,rgb(29, 50, 97), #1e293b)",
    padding: "20px",
    borderRadius: "12px",
    boxShadow: "0px 6px 20px rgba(8, 10, 10, 0.15)", // Soft glow effect
    transition: "transform 0.3s ease-in-out, box-shadow 0.3s ease-in-out",
    cursor: "pointer",
    border: "1px solid rgba(255, 255, 255, 0.1)",
    backdropFilter: "blur(8px)", // Frosted glass effect
  },
  cardHover: {
    transform: "scale(1.05)",
    boxShadow: "0px 8px 25px rgba(0, 191, 255, 0.3)", // Stronger glow on hover
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
    height: "80px",
  },
};

export default Stocks;
