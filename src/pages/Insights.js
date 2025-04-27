import React, { useEffect, useState } from "react";
import { motion } from "framer-motion";
import { FaChartLine, FaFire, FaLightbulb, FaTags } from "react-icons/fa"; // Added FaTags
import TagCloud from "react-tag-cloud";
import './Insights.css'; // We'll add some CSS for bubble styles

// --- Framer Motion Variants ---
const containerVariants = {
  initial: { opacity: 0 },
  animate: { opacity: 1, transition: { duration: 0.6, delayChildren: 0.3 } },
  exit: { opacity: 0, transition: { duration: 0.4 } },
};

const headingVariants = {
  initial: { y: -30, opacity: 0 },
  animate: { y: 0, opacity: 1, transition: { type: "spring", stiffness: 100, damping: 15, delay: 0.2 } },
};

const loadingVariants = {
  initial: { opacity: 0, scale: 0.9 },
  animate: { opacity: 1, scale: 1, transition: { duration: 0.5 } },
};

const cardVariants = {
  initial: { y: 60, opacity: 0 },
  animate: { y: 0, opacity: 1, transition: { duration: 0.7, ease: [0.25, 0.1, 0.25, 1] } }, // Smoother ease
};

const highlightItemVariants = {
  initial: { x: -20, opacity: 0 },
  animate: (index) => ({
    x: 0,
    opacity: 1,
    transition: { duration: 0.5, delay: 0.1 * index + 0.5, ease: "easeOut" }, // Start after card animates
  }),
};

// --- Bubble Tag Component (Custom Renderer for TagCloud) ---
const BubbleTag = ({ tag, size, color }) => {
  // Randomize animation properties slightly for each bubble
  const randomDuration = Math.random() * 4 + 5; // Duration between 5s and 9s
  const randomYOffset = Math.random() * 6 + 3; // Vertical movement between 3px and 9px
  const randomDelay = Math.random() * 2; // Start delay up to 2s

  return (
    <motion.div
      className="tag-bubble" // Use CSS class for base styles
      style={{ fontSize: `${size}px` }}
      initial={{ opacity: 0, scale: 0.5 }}
      animate={{
        opacity: 1,
        scale: 1,
        y: [0, -randomYOffset, 0, randomYOffset / 2, 0], // Floating motion
      }}
      transition={{
        opacity: { duration: 0.5, delay: randomDelay },
        scale: { duration: 0.5, delay: randomDelay },
        y: {
          duration: randomDuration,
          repeat: Infinity,
          ease: "easeInOut",
          delay: randomDelay, // Stagger the start of floating
          repeatType: "mirror",
        },
      }}
      whileHover={{ scale: 1.15, zIndex: 1, boxShadow: "0 5px 15px rgba(0, 0, 0, 0.4)" }} // Pop effect on hover
    >
      {tag.value}
    </motion.div>
  );
};


// --- Main Insights Component ---
const Insights = () => {
  const [dailyAnalysis, setDailyAnalysis] = useState(null);
  const [loading, setLoading] = useState(true);
  const [tagsForCloud, setTagsForCloud] = useState([]);

  useEffect(() => {
    const fetchData = async () => {
      setLoading(true); // Ensure loading state is true on fetch
      try {
        // Simulate API delay for testing loading state
        // await new Promise(resolve => setTimeout(resolve, 1500));
        const res = await fetch("http://localhost:8000/daily-analysis");
        if (!res.ok) {
            throw new Error(`HTTP error! status: ${res.status}`);
        }
        const data = await res.json();
        console.log("Fetched daily analysis:", data);
        setDailyAnalysis(data);
      } catch (error) {
        console.error("Error fetching analysis:", error);
        setDailyAnalysis(null); // Clear data on error
      } finally {
        setLoading(false);
      }
    };

    fetchData();
  }, []);

  // Process keywords for the tag cloud when data arrives
  useEffect(() => {
    if (dailyAnalysis?.clusters) {
      const keywordCounts = {};
      dailyAnalysis.clusters.forEach(cluster => {
        cluster.keywords.forEach(keyword => {
          const cleanKeyword = keyword.trim().toLowerCase(); // Normalize keywords
          if (cleanKeyword) {
            keywordCounts[cleanKeyword] = (keywordCounts[cleanKeyword] || 0) + 1;
          }
        });
      });

      const formattedTags = Object.entries(keywordCounts)
        .map(([value, count]) => ({ value, count }))
        .sort((a, b) => b.count - a.count); // Optional: sort by count

      setTagsForCloud(formattedTags);
      console.log("Formatted tags for cloud:", formattedTags);
    } else {
       setTagsForCloud([]); // Clear tags if no clusters
    }
  }, [dailyAnalysis]);


  // Custom renderer function for TagCloud
  const customTagRenderer = (tag, size, color) => (
    <BubbleTag key={tag.value} tag={tag} size={size} color={color} />
  );

  return (
    <motion.div
      className="insights-container" // Use class for main container styles
      variants={containerVariants}
      initial="initial"
      animate="animate"
      exit="exit"
    >
      <motion.h1
        variants={headingVariants}
        className="insights-main-heading"
      >
        <FaChartLine className="inline-block mr-3 text-emerald-400 text-4xl" /> Market Insights Hub
      </motion.h1>

      {loading && ( // Use && for cleaner conditional rendering
        <motion.div
          variants={loadingVariants}
          className="insights-status insights-loading"
        >
          <span className="animate-spin text-3xl text-teal-300 mr-4">⚙️</span> Gathering today's crucial market intelligence...
        </motion.div>
      )}

      {!loading && dailyAnalysis && ( // Render content when not loading and data exists
        <>
          {/* --- Date & Market Move Card --- */}
          <motion.div
            variants={cardVariants}
            className="insights-card insights-summary-card"
          >
            <h2 className="insights-date">
              {new Date(dailyAnalysis.date).toLocaleDateString(undefined, {
                year: "numeric",
                month: "long",
                day: "numeric",
              })}
            </h2>
            <p className="insights-market-move">
              Today's Market Direction:{" "}
              <strong className={`font-extrabold text-2xl px-2 py-1 rounded ${
                  dailyAnalysis.analysis.market_move.toLowerCase().includes('up') ? 'text-green-200 bg-green-700/50' :
                  dailyAnalysis.analysis.market_move.toLowerCase().includes('down') ? 'text-red-200 bg-red-700/50' :
                  'text-yellow-200 bg-yellow-700/50'
              }`}>
                {dailyAnalysis.analysis.market_move.toUpperCase()}
              </strong>
            </p>
          </motion.div>

          {/* --- Key Takeaways Card --- */}
          <motion.div
            variants={cardVariants}
            className="insights-card"
          >
            <h3 className="insights-card-heading">
              <FaFire className="inline-block mr-3 text-orange-400" /> Key Takeaways
            </h3>
            <motion.ul className="insights-list">
              {dailyAnalysis.analysis.highlights?.length > 0 ? (
                dailyAnalysis.analysis.highlights.map((item, idx) => (
                  <motion.li
                    key={idx}
                    variants={highlightItemVariants}
                    initial="initial"
                    animate="animate"
                    custom={idx}
                    className="insights-list-item"
                  >
                    <span className="insights-list-bullet">✦</span>
                    {item}
                  </motion.li>
                ))
              ) : (
                <p className="insights-no-data">
                  No significant highlights identified today.
                </p>
              )}
            </motion.ul>
          </motion.div>

          {/* --- Reasoning Card --- */}
          <motion.div
            variants={cardVariants}
            className="insights-card"
          >
            <h3 className="insights-card-heading">
              <FaLightbulb className="inline-block mr-3 text-cyan-300" /> Underlying Reasoning
            </h3>
            <motion.p
              className="insights-reasoning"
              initial={{ opacity: 0 }}
              animate={{ opacity: 1, transition: { delay: 0.6 } }}
            >
              {dailyAnalysis.analysis.reasoning}
            </motion.p>
          </motion.div>

          {/* --- Keyword Bubbles Card --- */}
          {tagsForCloud.length > 0 && ( // Only show card if there are tags
            <motion.div
              variants={cardVariants}
              className="insights-card insights-keywords-card"
            >
              <h3 className="insights-card-heading">
                <FaTags className="inline-block mr-3 text-indigo-300" /> Key Themes & Keywords
              </h3>
              {/* Container to define the small area for bubbles */}
              <div className="tag-cloud-container">
                <TagCloud
                    tags={tagsForCloud}
                    minSize={14} // Slightly larger min size for bubbles
                    maxSize={38} // Slightly larger max size
                    renderer={customTagRenderer} // Use our custom bubble renderer
                    shuffle={true}
                    className="keyword-tag-cloud" // Add class for specific cloud styling if needed
                    // We control size via the container div, not the style prop here
                 />
              </div>
               <p className="insights-keywords-footer">
                 Prominent keywords shaping today's market narrative. Hover for emphasis.
               </p>
            </motion.div>
          )}
        </>
      )}

      {!loading && !dailyAnalysis && ( // Render error when not loading and no data
        <motion.div
          variants={loadingVariants} // Can reuse loading variants
          className="insights-status insights-error"
        >
          <span className="text-3xl text-red-400 mr-4">⚠️</span> Failed to load today's market insights. Please check the connection or try again later.
        </motion.div>
      )}
    </motion.div>
  );
};

export default Insights;