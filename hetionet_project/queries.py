import json


class QueryEngine:
    def __init__(self, neo4j_driver, redis_client):
        self.neo4j_driver = neo4j_driver
        self.redis_client = redis_client

    def query1_disease_profile(self, disease_id):
        """
        Query 1: Given a disease id, return:
        - Disease name
        - Drugs that treat/palliate it
        - Genes that cause it
        - Anatomical locations where it occurs
        """

        # Check Redis cache first
        cache_key = f"disease:profile:{disease_id}"
        cached = self.redis_client.get(cache_key)

        if cached:
            print("  (from cache)")
            return json.loads(cached)

        # Query Neo4j
        with self.neo4j_driver.session() as session:
            query = """
            MATCH (d:Disease {id: $disease_id})
            OPTIONAL MATCH (d)<-[r1]-(c:Compound)
            WHERE type(r1) IN ['CtD', 'CpD']
            OPTIONAL MATCH (d)-[r2]->(g:Gene)
            WHERE type(r2) IN ['DaG', 'DuG', 'DdG']
            OPTIONAL MATCH (d)-[:DlA]->(a:Anatomy)
            RETURN d.name AS disease_name,
                   collect(DISTINCT c.name) AS drugs,
                   collect(DISTINCT g.name) AS genes,
                   collect(DISTINCT a.name) AS anatomies
            """

            result = session.run(query, disease_id=disease_id)
            record = result.single()

            if not record:
                return None

            profile = {
                "disease_id": disease_id,
                "disease_name": record["disease_name"],
                "drugs": [d for d in record["drugs"] if d],
                "genes": [g for g in record["genes"] if g],
                "anatomies": [a for a in record["anatomies"] if a],
            }

            # Cache the result
            self.redis_client.set(cache_key, json.dumps(profile))

            return profile

    def query2_drug_repurposing(self, disease_id):
        """
        Query 2: Find compounds that could treat a disease through
        indirect pathways (opposite regulation of genes at disease locations)
        """

        # Check Redis cache
        cache_key = f"drug:repurposing:{disease_id}"
        cached = self.redis_client.get(cache_key)

        if cached:
            print("  (from cache)")
            return json.loads(cached)

        # Query Neo4j
        with self.neo4j_driver.session() as session:
            query = """
            MATCH (d:Disease {id: $disease_id})-[:DlA]->(a:Anatomy)
            MATCH (c:Compound)-[r1]->(g:Gene)<-[r2]-(a)
            WHERE (
                (type(r1) = 'CuG' AND type(r2) IN ['AdG']) OR
                (type(r1) = 'CdG' AND type(r2) IN ['AuG'])
            )
            AND NOT EXISTS((c)-[:CtD|CpD]->(d))
            RETURN DISTINCT c.id AS compound_id, c.name AS compound_name
            ORDER BY c.name
            """

            result = session.run(query, disease_id=disease_id)

            candidates = []
            for record in result:
                candidates.append(
                    {
                        "compound_id": record["compound_id"],
                        "compound_name": record["compound_name"],
                    }
                )

            # Cache the result
            self.redis_client.set(cache_key, json.dumps(candidates))

            return candidates
