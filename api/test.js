const { Pool } = require('pg');
const pool = new Pool({ connectionString: process.env.PG_SUPABASE_URL });

module.exports = async (req, res) => {
  try {
    const { rows } = await pool.query('SELECT 1');
    res.status(200).json({ success: true, rows });
  } catch(err) {
    res.status(500).json({ success: false, error: err.message });
  }
};
