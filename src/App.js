import React from "react";
import { BrowserRouter as Router, Routes, Route } from "react-router-dom";
import Sidebar from "./components/Sidebar";
import Stocks from "./pages/Stocks";
import Home from "./pages/Home";
import Insights from "./pages/Insights";
import Stock from "./pages/Stock";

function App() {
  return (
    <Router>
      <div style={styles.appContainer}>
        <Sidebar />
        <div style={styles.contentContainer}>
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/stocks" element={<Stocks />} />
            <Route path="/stock/:symbol" element={<Stock />} />
            <Route path="/insights" element={<Insights />} />
          </Routes>
        </div>
      </div>
    </Router>
  );
}

// Global Minimalist Theme with Inter Font
const styles = {
  appContainer: {
    display: "flex",
    background: "linear-gradient(to bottom, #0f172a, #1e293b)",
    minHeight: "100vh",
    color: "#fff",
    fontFamily: "'Inter', sans-serif", // 👈 Applied globally
  },
  contentContainer: {
    flexGrow: 1,
    padding: "20px",
    marginLeft: "220px",
  },
};

export default App;
