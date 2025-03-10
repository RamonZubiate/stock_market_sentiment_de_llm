import React from "react";
import ReactECharts from "echarts-for-react";

const StockCard = ({ stock }) => {
  const chartOptions = {
    grid: { left: 0, right: 0, top: 0, bottom: 0 },
    xAxis: { type: "category", show: false },
    yAxis: { type: "value", show: false },
    series: [
      {
        type: "line",
        data: stock.history.map((point) => point.price),
        smooth: true,
        lineStyle: { width: 2, color: stock.change >= 0 ? "#4caf50" : "#f44336" },
        areaStyle: { color: stock.change >= 0 ? "#4caf5030" : "#f4433630" },
      },
    ],
  };

  return (
    <div style={styles.card}>
      <div style={styles.stockHeader}>
        <h2 style={styles.stockSymbol}>{stock.symbol}</h2>
        <span style={{ ...styles.priceChange, color: stock.change >= 0 ? "green" : "red" }}>
          {stock.change >= 0 ? "▲" : "▼"} {stock.change.toFixed(2)} ({stock.percent.toFixed(2)}%)
        </span>
      </div>
      <h3 style={styles.stockPrice}>${stock.price.toFixed(2)}</h3>
      <ReactECharts option={chartOptions} style={{ height: "50px", width: "100%" }} />
    </div>
  );
};

// Embedded styles
const styles = {
  card: {
    background: "linear-gradient(135deg, #1e1e1e, #292929)",
    padding: "20px",
    borderRadius: "12px",
    boxShadow: "0 4px 10px rgba(0, 0, 0, 0.3)",
    transition: "transform 0.3s ease-in-out",
    cursor: "pointer",
  },
  stockHeader: {
    display: "flex",
    justifyContent: "space-between",
    alignItems: "center",
    marginBottom: "10px",
  },
  stockSymbol: {
    fontSize: "20px",
    fontWeight: "bold",
    color: "#fff",
  },
  priceChange: {
    fontSize: "16px",
    fontWeight: "bold",
  },
  stockPrice: {
    fontSize: "22px",
    fontWeight: "bold",
    marginBottom: "10px",
    color: "#ddd",
  },
};

export default StockCard;
