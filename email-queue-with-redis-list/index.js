import express from "express";
import Redis from "ioredis";

const app = express();

app.use(express.json());
const redis = new Redis(process.env.REDIS_URL || "redis://localhost:6379");

const QUEUE_NAME = "email_queue";

app.post("/email_queue", async (req, res) => {
  try {
    const { to, subject, body } = req.body;
    if (!to || !subject || !body) {
      return res.status(400).json({
        success: false,
        message: "to, subject and body are required ",
      });
    }

    const emailJob = {
      id: Date.now(),
      to,
      subject,
      body,
      createdAt: new Date().toISOString(),
    };

    await redis.lpush(QUEUE_NAME, JSON.stringify(emailJob));

    res.status(201).json({
      success: true,
      message: "Email added to Queue",
      job: emailJob,
    });
  } catch (error) {
    res.status(500).json({
      success: false,
      message: "internal server error",
    });
  }
});

app.get("/email_queue", async (req, res) => {
  try {
    const jobs = await redis.lrange(QUEUE_NAME, 0 , -1);
    const emails = jobs.map(job => JSON.parse(job))

    res.json({
        success: true,
        total: emails.length,
        queue: emails
    })
  } catch (error) {
    console.log(error)

    res.status(500).json({
        success: false,
        message: "internal server error"
    })
  }
});
app.listen(3000, () => {
  console.log("server running on port 3000");
});
