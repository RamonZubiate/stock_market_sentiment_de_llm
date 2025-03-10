import React from "react";
import { Link } from "react-router-dom";

const Sidebar = () => {
  return (
    <div style={styles.sidebar}>
      <h2 style={styles.logo}>📊 Market Dashboard</h2>
      <ul style={styles.navLinks}>
        <li><Link to="/" style={styles.link}>🏠 Home</Link></li>
        <li><Link to="/stocks" style={styles.link}>📈 Stocks</Link></li>
        <li><Link to="/news" style={styles.link}>📰 Market News</Link></li>
        <li><Link to="/insights" style={styles.link}>🔍 Insights</Link></li>
      </ul>
    </div>
  );
};

// Minimalist Sidebar with Inter Font
const styles = {
  sidebar: {
    position: "fixed",
    left: 0,
    top: 0,
    width: "220px",
    height: "100vh",
    background: "linear-gradient(to bottom, #1e293b, #0f172a)",
    padding: "20px",
    boxShadow: "2px 0 10px rgba(255, 255, 255, 0.05)",
    color: "#fff",
    fontFamily: "'Inter', sans-serif", // 👈 Applied here
  },
  logo: {
    fontSize: "22px",
    fontWeight: "600",
    marginBottom: "20px",
    textAlign: "center",
  },
  navLinks: {
    listStyle: "none",
    padding: 0,
    margin: 0,
  },
  link: {
    display: "block",
    padding: "12px",
    color: "#bbb",
    textDecoration: "none",
    fontSize: "16px",
    transition: "background 0.3s, color 0.3s",
    borderRadius: "5px",
    fontWeight: "400",
  },
};

export default Sidebar;
