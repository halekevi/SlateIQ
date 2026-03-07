import app from './backend/server.js';
import { MongoClient } from 'mongodb';
import dotenv from "dotenv";

dotenv.config();

const PORT = process.env.PORT || 8000;
const MONGO_URL = process.env.MONGO_URL;

const client = new MongoClient(MONGO_URL);

async function main() {
  try {
    await client.connect();
    console.log('Connected successfully to MongoDB');

    const db = client.db('yourDatabaseName');
    app.locals.db = db;

    app.listen(PORT, () => {
      console.log(`Server is running on port ${PORT}`);
    });
  } catch (err) {
    console.error('MongoDB connection error:', err);
  }
}

main().catch(console.error);