from pymongo import MongoClient
import redis
import pandas as pd
import config


class DatabaseManager:
    def __init__(self):
        # Connect to MongoDB
        self.mongo_client = MongoClient(config.MONGO_HOST, config.MONGO_PORT)
        self.mongo_db = self.mongo_client[config.MONGO_DATABASE]
        self.nodes_collection = self.mongo_db["nodes"]
        self.edges_collection = self.mongo_db["edges"]

        # Connect to Redis
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
        )

        print("✓ Connected to MongoDB and Redis")

    def clear_databases(self):
        """Clear all data from both databases"""
        print("Clearing databases...")

        # Clear MongoDB
        self.mongo_db.drop_collection("nodes")
        self.mongo_db.drop_collection("edges")
        print("  ✓ MongoDB cleared")

        # Clear Redis
        self.redis_client.flushdb()
        print("  ✓ Redis cleared")

    def load_data(self):
        """Load data into MongoDB"""
        print("\nLoading data into MongoDB...")

        # Load source files
        nodes_df = pd.read_csv(config.NODES_FILE, sep="\t")
        edges_df = pd.read_csv(config.EDGES_FILE, sep="\t")

        print(f"  Found {len(nodes_df)} nodes")
        print(f"  Found {len(edges_df)} edges")

        # Insert nodes
        print("  Inserting nodes...")
        nodes_data = nodes_df.to_dict("records")
        self.nodes_collection.insert_many(nodes_data)
        print(f"    ✓ {len(nodes_data)} nodes inserted")

        # Insert edges
        print("  Inserting edges...")
        edges_data = edges_df.to_dict("records")
        self.edges_collection.insert_many(edges_data)
        print(f"    ✓ {len(edges_data)} edges inserted")

        # Create indexes for fast lookups
        print("  Creating indexes...")
        self.nodes_collection.create_index("id")
        self.nodes_collection.create_index("kind")
        self.edges_collection.create_index("source")
        self.edges_collection.create_index("target")
        self.edges_collection.create_index("metaedge")
        print("    ✓ Indexes created")

        print("\n✓ All data loaded successfully!")

    def close(self):
        """Close database connections"""
        self.mongo_client.close()
        print("✓ Connections closed")
