from pymongo import MongoClient
from cassandra.cluster import Cluster
from cassandra.query import BatchStatement
import pandas as pd
import config


class DatabaseManager:
    def __init__(self):
        # Connect to MongoDB
        self.mongo_client = MongoClient(config.MONGO_HOST, config.MONGO_PORT)
        self.mongo_db = self.mongo_client[config.MONGO_DATABASE]
        self.nodes_collection = self.mongo_db["nodes"]
        self.edges_collection = self.mongo_db["edges"]

        # Connect to Cassandra
        self.cassandra_cluster = Cluster(config.CASSANDRA_HOSTS)
        self.cassandra_session = self.cassandra_cluster.connect(
            config.CASSANDRA_KEYSPACE
        )

        # Prepare statements for Cassandra
        self.insert_node = self.cassandra_session.prepare(
            "INSERT INTO nodes (id, name, kind) VALUES (?, ?, ?)"
        )
        self.insert_edge = self.cassandra_session.prepare(
            "INSERT INTO edges (source, target, metaedge) VALUES (?, ?, ?)"
        )

        print("✓ Connected to MongoDB and Cassandra")

    def clear_databases(self):
        """Clear all data from both databases"""
        print("Clearing databases...")

        # Clear MongoDB
        self.mongo_db.drop_collection("nodes")
        self.mongo_db.drop_collection("edges")
        print("  ✓ MongoDB cleared")

        # Clear Cassandra
        self.cassandra_session.execute("TRUNCATE nodes")
        self.cassandra_session.execute("TRUNCATE edges")
        print("  ✓ Cassandra cleared")

    def load_data(self):
        """Load data into both databases"""
        print("\nLoading data into databases...")

        # Load source files
        nodes_df = pd.read_csv(config.NODES_FILE, sep="\t")
        edges_df = pd.read_csv(config.EDGES_FILE, sep="\t")

        print(f"  Found {len(nodes_df)} nodes")
        print(f"  Found {len(edges_df)} edges")

        # Load MongoDB (for Query 1 - simple lookups)
        print("\n[1/2] Loading MongoDB...")
        self._load_mongodb(nodes_df, edges_df)

        # Load Cassandra (for Query 2 - column-family access patterns)
        print("\n[2/2] Loading Cassandra...")
        self._load_cassandra(nodes_df, edges_df)

        print("\n✓ All databases loaded successfully!")

    def _load_mongodb(self, nodes_df, edges_df):
        """Load data into MongoDB"""

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

        # Create indexes
        print("  Creating indexes...")
        self.nodes_collection.create_index("id")
        self.nodes_collection.create_index("kind")
        self.edges_collection.create_index("source")
        self.edges_collection.create_index("target")
        self.edges_collection.create_index("metaedge")
        print("    ✓ Indexes created")

    def _load_cassandra(self, nodes_df, edges_df):
        """Load data into Cassandra using concurrent execution"""
        from cassandra.concurrent import execute_concurrent_with_args

        # Insert nodes
        print("  Inserting nodes...")
        node_params = [
            (row["id"], row["name"], row["kind"]) for _, row in nodes_df.iterrows()
        ]

        # Execute in batches of 100 concurrently
        for i in range(0, len(node_params), 100):
            batch_params = node_params[i : i + 100]
            execute_concurrent_with_args(
                self.cassandra_session, self.insert_node, batch_params
            )
            if (i + 100) % 5000 == 0:
                print(f"    {min(i + 100, len(node_params))}/{len(node_params)} nodes")

        print(f"    ✓ {len(node_params)} nodes inserted")

        # Insert edges
        print("  Inserting edges...")
        edge_params = [
            (row["source"], row["target"], row["metaedge"])
            for _, row in edges_df.iterrows()
        ]

        # Execute in batches of 100 concurrently
        for i in range(0, len(edge_params), 100):
            batch_params = edge_params[i : i + 100]
            execute_concurrent_with_args(
                self.cassandra_session, self.insert_edge, batch_params
            )
            if (i + 100) % 50000 == 0:
                print(f"    {min(i + 100, len(edge_params))}/{len(edge_params)} edges")

        print(f"    ✓ {len(edge_params)} edges inserted")

    def close(self):
        """Close database connections"""
        self.mongo_client.close()
        self.cassandra_cluster.shutdown()
        print("✓ Connections closed")
