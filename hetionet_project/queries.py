import json


class QueryEngine:
    def __init__(self, db_manager):
        self.mongo_db = db_manager.mongo_db
        self.cassandra_session = db_manager.cassandra_session

    def query1_disease_profile(self, disease_id):
        """
        Query 1: Disease Profile
        Uses MONGODB (optimized for this simple lookup)
        """

        print("  (from MongoDB)")

        nodes_collection = self.mongo_db["nodes"]
        edges_collection = self.mongo_db["edges"]

        # Get disease info
        disease = nodes_collection.find_one({"id": disease_id})
        if not disease:
            return None

        # Get treating drugs
        drug_edges = edges_collection.find(
            {"target": disease_id, "metaedge": {"$in": ["CtD", "CpD"]}}
        )

        drug_ids = [edge["source"] for edge in drug_edges]
        drugs = list(
            nodes_collection.find({"id": {"$in": drug_ids}}, {"name": 1, "_id": 0})
        )

        # Get associated genes
        gene_edges = edges_collection.find(
            {"source": disease_id, "metaedge": {"$in": ["DaG", "DuG", "DdG"]}}
        )

        gene_ids = [edge["target"] for edge in gene_edges]
        genes = list(
            nodes_collection.find({"id": {"$in": gene_ids}}, {"name": 1, "_id": 0})
        )

        # Get anatomical locations
        anatomy_edges = edges_collection.find({"source": disease_id, "metaedge": "DlA"})

        anatomy_ids = [edge["target"] for edge in anatomy_edges]
        anatomies = list(
            nodes_collection.find({"id": {"$in": anatomy_ids}}, {"name": 1, "_id": 0})
        )

        profile = {
            "disease_id": disease_id,
            "disease_name": disease.get("name"),
            "drugs": [d["name"] for d in drugs],
            "genes": [g["name"] for g in genes],
            "anatomies": [a["name"] for a in anatomies],
        }

        return profile

    def query2_drug_repurposing(self, disease_id):
        """
        Query 2: Drug Repurposing
        Uses CASSANDRA (demonstrates column-family querying)
        """

        print("  (from Cassandra)")

        # Step 1: Get anatomies where disease occurs
        anatomy_query = """
            SELECT target FROM edges 
            WHERE source = %s AND metaedge = 'DlA'
            ALLOW FILTERING
        """
        anatomy_rows = self.cassandra_session.execute(anatomy_query, [disease_id])
        anatomy_ids = [row.target for row in anatomy_rows]

        if not anatomy_ids:
            return []

        # Step 2: Get genes regulated by anatomies
        anatomy_gene_map = {}

        for anatomy_id in anatomy_ids:
            gene_query = """
                SELECT target, metaedge FROM edges 
                WHERE source = %s AND metaedge IN ('AdG', 'AuG')
                ALLOW FILTERING
            """
            gene_rows = self.cassandra_session.execute(gene_query, [anatomy_id])

            for row in gene_rows:
                if row.target not in anatomy_gene_map:
                    anatomy_gene_map[row.target] = []
                anatomy_gene_map[row.target].append(row.metaedge)

        # Step 3: Find compounds with opposite regulation
        candidates = set()

        for gene_id, regulations in anatomy_gene_map.items():
            # If anatomy down-regulates, find compounds that up-regulate
            if "AdG" in regulations:
                compound_query = """
                    SELECT source FROM edges 
                    WHERE target = %s AND metaedge = 'CuG'
                    ALLOW FILTERING
                """
                compound_rows = self.cassandra_session.execute(
                    compound_query, [gene_id]
                )
                candidates.update([row.source for row in compound_rows])

            # If anatomy up-regulates, find compounds that down-regulate
            if "AuG" in regulations:
                compound_query = """
                    SELECT source FROM edges 
                    WHERE target = %s AND metaedge = 'CdG'
                    ALLOW FILTERING
                """
                compound_rows = self.cassandra_session.execute(
                    compound_query, [gene_id]
                )
                candidates.update([row.source for row in compound_rows])

        # Step 4: Filter out existing treatments
        filtered_candidates = []
        for compound_id in candidates:
            treatment_query = """
                SELECT * FROM edges 
                WHERE source = %s AND target = %s AND metaedge IN ('CtD', 'CpD')
                ALLOW FILTERING
            """
            treatment_rows = list(
                self.cassandra_session.execute(
                    treatment_query, [compound_id, disease_id]
                )
            )

            if not treatment_rows:
                # Get compound name
                name_query = "SELECT name FROM nodes WHERE id = %s"
                name_row = self.cassandra_session.execute(
                    name_query, [compound_id]
                ).one()

                if name_row:
                    filtered_candidates.append(
                        {"compound_id": compound_id, "compound_name": name_row.name}
                    )

        # Sort by name
        filtered_candidates.sort(key=lambda x: x["compound_name"])

        return filtered_candidates
