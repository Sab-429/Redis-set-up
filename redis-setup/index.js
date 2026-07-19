import express from "express"
import Redis from "ioredis"
import mongoose from 'mongoose'

const app = express()


const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');
const BANNER_KEY = "app:banner";
app.get('/redis', async(req, res) => {
    const reply = await redis.ping()
    res.json({
        redis : reply
    })
})

app.get('/mongo', async(req,res) => {
    try{
        const url = process.env.MONGO_URI || "mongodb://localhost:27017/Mongo_redis"
        if(mongoose.connection.readyState === 0) {
            await mongoose.connect(url)
        }
        res.json({
            mongo: "connected",
            database: mongoose.connection.name
        })
    }catch(error) {
        res.status(500).json({
            error: error.message
        })
    }
})
app.use(express.json());
app.post("/banner", async(req,res)=> {
    try {
        console.log("===== POST HIT =====");
        console.log("Headers:", req.headers);
        console.log("Body:", req.body);
        await redis.set(BANNER_KEY, req.body?.message || "welcome to website");
        res.json({ headers: req.headers,
            body: req.body});
    } catch (error) {
        res.status(500).json({error : "error"})
    }
})

app.get("/banner", async (req,res) => {
    const message = await redis.get(BANNER_KEY);
    res.json({message});
})

app.delete("/banner", async(req,res) => {
    await redis.del(BANNER_KEY);
    res.json({message});
})

app.get("/banner/exists", async(req, res) => {
    const exists = await redis.exists(BANNER_KEY);
    res.json({ exists: Boolean(exists)})
})

app.listen(4000, () => {
    console.log('server running on port 4000')
})