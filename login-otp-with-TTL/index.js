
import express from "express"
import Redis from "ioredis"

const app = express()
app.use(express.json());
const redis = new Redis(process.env.REDIS_URL || 'redis://localhost:6379');

function otpkey(phone) {
    return `otp:${phone}`;
}

app.post('/otp', async(req, res) => {
    const {phone} = req.body
    const otp = Math.floor(100000 + Math.random()*900000).toString();

    await redis.set(otpkey(phone), otp, "EX", 30);

    res.json({
        message : 'OTP sent', otp
    })
})

app.post("/otp/verify", async (req, res) => {
    try {
        const { phone, otp } = req.body;

        const savedOtp = await redis.get(otpkey(phone));

        if (!savedOtp) {
            return res.status(400).json({
                message: "OTP expired or not found"
            });
        }

        if (savedOtp !== otp) {
            return res.status(400).json({
                message: "Invalid OTP"
            });
        }

        await redis.del(otpkey(phone));

        res.json({
            message: "OTP verified"
        });

    } catch (err) {
        console.error(err);
        res.status(500).json({
            error: err.message
        });
    }
});

app.get('/otp/:phone/ttl', async (req, res) => {
    console.log("TTL route hit:", req.params.phone);

    const ttl = await redis.ttl(otpkey(req.params.phone));

    res.json({ ttl });
});
app.listen(6000 , () => {
    console.log('server running on port 6000')
})