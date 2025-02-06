require("dotenv").config();
const express = require("express");
const cors = require("cors");
const { Pool } = require("pg");

const app = express();
const PORT = process.env.PORT || 5000;

// Enable CORS
app.use(cors());

// Database configuration
const pool = new Pool({
  user: process.env.DB_USER || "postgres",
  host: process.env.DB_HOST || "localhost",
  database: process.env.DB_NAME || "oil_monitoring",
  password: process.env.DB_PASSWORD || "1234",
  port: parseInt(process.env.DB_PORT || "5432", 10),
});

// Total machines (configurable)
const TOTAL_MACHINES = parseInt(process.env.TOTAL_MACHINES || "6", 10);

// Fetch machine summaries
app.get("/api/machines", async (req, res) => {
  try {
    const machineQueries = [];
    for (let i = 1; i <= TOTAL_MACHINES; i++) {
      const query = `
        SELECT 
          (oil_level_shift1 + oil_level_shift2 + oil_level_shift3) AS "currentOilLevel",
          tank_capacity AS "tankCapacity",
          min_oil_level AS "minimumOilLevel"
        FROM station_${i}
        ORDER BY timestamp DESC
        LIMIT 1;
      `;
      machineQueries.push(pool.query(query));
    }

    const results = await Promise.all(machineQueries);

    // Map results to include station info
    const machines = results.map((result, index) => {
      const data = result.rows[0] || {};
      return {
        station: `station_${index + 1}`,
        currentOilLevel: data.currentOilLevel || 0,
        tankCapacity: data.tankCapacity || 0,
        minimumOilLevel: data.minimumOilLevel || 0,
      };
    });

    res.json(machines);
  } catch (error) {
    console.error("Error fetching machine summaries:", error);
    res.status(500).json({ error: "Failed to fetch machines", message: error.message });
  }
});

// Fetch oil history for a specific station
app.get("/api/history/:stationNo", async (req, res) => {
  const { stationNo } = req.params;

  try {
    const sanitizedStationNo = parseInt(stationNo, 10);
    if (isNaN(sanitizedStationNo) || sanitizedStationNo < 1 || sanitizedStationNo > TOTAL_MACHINES) {
      return res.status(400).json({ error: "Invalid station number. Must be between 1 and " + TOTAL_MACHINES });
    }

    const query = `
      SELECT
        id,
        timestamp,
        oil_level_shift1 AS "shift1",
        oil_level_shift2 AS "shift2",
        oil_level_shift3 AS "shift3"
      FROM station_${sanitizedStationNo}
      ORDER BY timestamp DESC
      LIMIT 100;
    `;

    const result = await pool.query(query);

    if (result.rows.length === 0) {
      return res.status(404).json({ error: "No history data found for the station" });
    }

    res.json(result.rows);
  } catch (error) {
    console.error(`Error fetching history for station ${stationNo}:`, error);
    res.status(500).json({ error: "Failed to fetch history", message: error.message });
  }
});

// Fetch top-up history
app.get("/api/topup-history", async (req, res) => {
  try {
    const topupQueries = [];
    for (let i = 1; i <= TOTAL_MACHINES; i++) {
      const query = `
        WITH previous_reading AS (
          SELECT timestamp, 
                 (oil_level_shift1 + oil_level_shift2 + oil_level_shift3) AS oil_level,
                 LAG(oil_level_shift1 + oil_level_shift2 + oil_level_shift3) OVER (ORDER BY timestamp) AS prev_oil_level
          FROM station_${i}
        )
        SELECT 
          s.timestamp,
          '${i}' AS station,
          (oil_level - prev_oil_level) AS topup, 
          SUM(oil_level - prev_oil_level) OVER (PARTITION BY DATE(timestamp)) AS total_topup_today,
          (prev_oil_level - oil_level) AS oil_reduction
        FROM previous_reading s
        WHERE oil_level > prev_oil_level
        ORDER BY timestamp DESC
        LIMIT 100;
      `;

      topupQueries.push(pool.query(query));
    }

    const results = await Promise.all(topupQueries);

    const topupHistory = results.flatMap((result, index) =>
      result.rows.map((row) => ({
        station: `station_${index + 1}`,
        timestamp: row.timestamp,
        topup: row.topup || 0,
        totalTopupToday: row.total_topup_today || 0,
        oilReduction: row.oil_reduction || 0,
      }))
    );

    res.json(topupHistory);
  } catch (error) {
    console.error("Error fetching top-up history:", error);
    res.status(500).json({ error: "Failed to fetch top-up history", message: error.message });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`Server running at http://localhost:${PORT}`);
});
