from cassandra.cluster import Cluster

print("Connecting to Cassandra...")
cluster = Cluster(["127.0.0.1"])
session = cluster.connect()

print("Creating keyspace...")
session.execute(
    """
    CREATE KEYSPACE IF NOT EXISTS hetionet
    WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}
"""
)

print("Using hetionet keyspace...")
session.execute("USE hetionet")

print("Creating nodes table...")
session.execute(
    """
    CREATE TABLE IF NOT EXISTS nodes (
        id text PRIMARY KEY,
        name text,
        kind text
    )
"""
)

print("Creating edges table...")
session.execute(
    """
    CREATE TABLE IF NOT EXISTS edges (
        source text,
        target text,
        metaedge text,
        PRIMARY KEY (source, target, metaedge)
    )
"""
)

print("Creating indexes...")
session.execute("CREATE INDEX IF NOT EXISTS ON nodes (kind)")
session.execute("CREATE INDEX IF NOT EXISTS ON edges (target)")
session.execute("CREATE INDEX IF NOT EXISTS ON edges (metaedge)")

print("✓ Cassandra schema created successfully!")

cluster.shutdown()
