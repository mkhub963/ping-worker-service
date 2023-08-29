const mongoose = require("mongoose");
const kafka = require("kafka-node");
const http = require("http");
const socketIo = require("socket.io");
const Redis = require("ioredis");
// const { IPStatus } = require("./model");

const redis = new Redis(); // Default Redis connection to localhost:6379

// MongoDB connection
mongoose
  .connect("mongodb://localhost:27017/pingip", {
    useNewUrlParser: true,
    useUnifiedTopology: true,
    auth: {
      username: "acns3",
      password: "acns3@123#DB",
    },
  })
  .then(() => {
    console.log("DB connected");
  })
  .catch((err) => {
    console.log("DB Error => ", err);
  });

// MongoDB Schema and Model
const ipStatusSchema = new mongoose.Schema({
  ip: String,
  isDown: Boolean,
  lastUp: Date,
  lastDown: Date,
  email: String,
  wasDown: { type: Boolean, default: false },
});
const IPStatus = mongoose.model("IPStatus", ipStatusSchema);

const messageLogSchema = new mongoose.Schema({
  messageId: String,
  status: String,
  timestamp: Date,
  ip: String,
});

// Create a unique index on the messageId field for efficient lookups
messageLogSchema.index({ messageId: 1 }, { unique: true });
const MessageLog = mongoose.model("MessageLog", messageLogSchema);

// Kafka Client Configuration
const kafkaClientOptions = {
  kafkaHost: "localhost:9092",
  connectTimeout: 1000,
  requestTimeout: 1000,
};
const kafkaClient = new kafka.KafkaClient(kafkaClientOptions);

// Kafka Consumer Configuration
const kafkaConsumer = new kafka.Consumer(
  kafkaClient,
  [{ topic: "ip-status-updates", partition: 0 }],
  { autoCommit: true }
);

// WebSocket server setup for client communication
const server = http.createServer();
const io = socketIo(server);

io.on("connection", (socket) => {
  console.log("Client connected");

  const redisSubscriber = new Redis();
  redisSubscriber.subscribe("ip-status-updates");

  // Forward the Redis updates to the connected client
  redisSubscriber.on("message", (channel, message) => {
    if (channel === "ip-status-updates") {
      socket.emit("update", message);
    }
  });

  // Clean up Redis connection on client disconnect
  socket.on("disconnect", () => {
    console.log("Client disconnected");
    redisSubscriber.quit();
  });
});

server.listen(8003, () => {
  console.log("WebSocket server listening on port 8003");
});

// Handle incoming messages
kafkaConsumer.on("message", (message) => {
  console.log("Received raw message:", message.value);

  if (
    message &&
    typeof message.value === "string" &&
    message.value.trim() !== ""
  ) {
    try {
      const ipStatusUpdate = JSON.parse(message.value);

      console.log("Received message:", ipStatusUpdate); // Log the unique identifier for triaging
      console.log("Unique Message ID:", ipStatusUpdate.messageId);

      const { ip, status, timestamp, messageId } = ipStatusUpdate;

      // Update the database based on the incoming message
      // if (status === "UP") {
      //   IPStatus.findOneAndUpdate(
      //     { ip: ip },
      //     {
      //       ip: ip,
      //       isDown: false,
      //       lastUp: new Date(timestamp),
      //       wasDown: false,
      //     },
      //     { upsert: true, new: true }
      //   )
      //     .then((document) => {
      //       console.log("Database updated successfully for UP status");
      //     })
      //     .catch((error) => {
      //       console.log(
      //         "Error while updating IP status in database:",
      //         error.message
      //       );
      //     });
      // } else if (status === "DOWN") {
      //   IPStatus.findOneAndUpdate(
      //     { ip: ip },
      //     {
      //       ip: ip,
      //       isDown: true,
      //       lastDown: new Date(timestamp),
      //       wasDown: true,
      //     },
      //     { upsert: true, new: true }
      //   )
      //     .then((document) => {
      //       console.log("Database updated successfully for DOWN status");
      //     })
      //     .catch((error) => {
      //       console.log(
      //         "Error while updating IP status in database:",
      //         error.message
      //       );
      //     });
      // }
      if (status === "UP" || status === "DOWN") {
        IPStatus.findOneAndUpdate(
          { ip: ip },
          {
            ip: ip,
            isDown: status === "DOWN",
            lastUp: status === "UP" ? new Date(timestamp) : undefined,
            lastDown: status === "DOWN" ? new Date(timestamp) : undefined,
            wasDown: status === "DOWN",
          },
          { upsert: true, new: true }
        )
          .then((document) => {
            console.log(`Database updated successfully for ${status} status`);

            // Publish the status update to a Redis channel
            redis.publish("ip-status-updates", JSON.stringify(ipStatusUpdate));
          })
          .catch((error) => {
            console.log(
              `Error while updating IP status in database:`,
              error.message
            );
          });
      }

      const messageLog = new MessageLog({
        messageId: messageId,
        status: status,
        timestamp: new Date(timestamp),
        ip: ip,
      });

      messageLog
        .save()
        .then((document) => {
          console.log("MessageLog saved successfully:", document);
        })
        .catch((error) => {
          console.log("Error while saving MessageLog:", error.message);
        });
    } catch (e) {
      console.error("Invalid JSON:", e);
    }
  } else {
    console.error("Empty message received or not a valid string:", message);
  }
});

// Handle errors
kafkaConsumer.on("error", (error) => {
  console.error("Error in Kafka consumer:", error);
});
