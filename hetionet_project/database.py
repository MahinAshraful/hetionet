from neo4j import GraphDatabase
import redis
import pandas as pd
import config
import json


class DatabaseManager:
    def __init__(self):
        # Connect to Neo4j
        self.neo4j_driver = GraphDatabase.driver(
            config.NEO4J_URI, auth=(config.NEO4J_USER, config.NEO4J_PASSWORD)
        )

        # Connect to Redis
        self.redis_client = redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db=config.REDIS_DB,
            decode_responses=True,
        )

        print("✓ Connected to Neo4j and Redis")

    def clear_databases(self):
        """Clear all data from both databases"""
        # Clear Neo4j
        with self.neo4j_driver.session() as session:
            session.run("MATCH (n) DETACH DELETE n")

        # Clear Redis
        self.redis_client.flushdb()

        print("✓ Databases cleared")

    def load_data(self):
        """Load nodes and edges from TSV files into Neo4j"""
        print("Loading data...")

        # Load nodes
        nodes_df = pd.read_csv(config.NODES_FILE, sep="\t")
        print(f"  Found {len(nodes_df)} nodes")

        # Load edges
        edges_df = pd.read_csv(config.EDGES_FILE, sep="\t")
        print(f"  Found {len(edges_df)} edges")

        with self.neo4j_driver.session() as session:
            # Create nodes by type
            print("  Creating nodes...")
            for kind in nodes_df["kind"].unique():
                kind_nodes = nodes_df[nodes_df["kind"] == kind]
                nodes_data = kind_nodes.to_dict("records")

                # Create nodes with their specific label
                query = f"""
                    UNWIND $nodes AS node
                    CREATE (n:{kind} {{id: node.id, name: node.name, kind: node.kind}})
                """
                session.run(query, nodes=nodes_data)

            print("  ✓ Nodes created")

            # Create indexes BEFORE edges (critical for speed!)
            print("  Creating indexes...")
            self.create_indexes()

            # Create edges in batches - group by metaedge type
            print("  Creating edges...")
            BATCH_SIZE = 10000  # Process edges in batches

            for metaedge in edges_df["metaedge"].unique():
                metaedge_edges = edges_df[edges_df["metaedge"] == metaedge]
                total_edges = len(metaedge_edges)

                print(f"    Processing {metaedge}: {total_edges} edges")

                # Process in batches
                for i in range(0, total_edges, BATCH_SIZE):
                    batch = metaedge_edges.iloc[i : i + BATCH_SIZE]
                    edges_data = batch.to_dict("records")

                    # ⚡ USE BACKTICKS TO ESCAPE SPECIAL CHARACTERS IN RELATIONSHIP TYPE
                    query = f"""
                        UNWIND $edges AS edge
                        MATCH (source {{id: edge.source}})
                        MATCH (target {{id: edge.target}})
                        CREATE (source)-[r:`{metaedge}`]->(target)
                    """
                    session.run(query, edges=edges_data)

                    # Progress indicator
                    processed = min(i + BATCH_SIZE, total_edges)
                    if processed % 50000 == 0 or processed == total_edges:
                        print(f"      {processed}/{total_edges} edges created")

                print(f"      ✓ {metaedge} complete")

            print("  ✓ All edges created")

        print("✓ Data loaded successfully")

    def create_indexes(self):
        """Create indexes for faster queries"""

        with self.neo4j_driver.session() as session:
            # Create index on id for ALL nodes
            try:
                session.run(
                    "CREATE INDEX node_id_index IF NOT EXISTS FOR (n) ON (n.id)"
                )
            except:
                pass

            # Also create type-specific indexes
            node_types = ["Disease", "Compound", "Gene", "Anatomy"]

            for node_type in node_types:
                try:
                    session.run(
                        f"CREATE INDEX IF NOT EXISTS FOR (n:{node_type}) ON (n.id)"
                    )
                except:
                    pass

        print("  ✓ Indexes created")

    def close(self):
        """Close database connections"""
        self.neo4j_driver.close()
        print("✓ Connections closed")
