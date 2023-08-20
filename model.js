const mongoose = require("mongoose");

// / MongoDB Schema and Model
const ipStatusSchema = new mongoose.Schema({
  ip: String,
  isDown: Boolean,
  lastUp: Date,
  lastDown: Date,
  email: String,
  wasDown: { type: Boolean, default: false },
});
// const IPStatus = mongoose.model("IPStatus", ipStatusSchema);

module.exports = mongoose.model("IPStatus", ipStatusSchema);
