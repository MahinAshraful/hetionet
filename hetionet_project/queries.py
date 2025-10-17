import json


class QueryEngine:
    def __init__(self, db_manager):
        self.mongo_db = db_manager.mongo_db
        self.redis_client = db_manager.redis_client

    def query1_disease_profile(self, disease_id):
        """
        Query 1: Disease Profile
        Uses MongoDB for simple lookup
        """

        # Check Redis cache first
        cache_key = f"disease:profile:{disease_id}"
        cached = self.redis_client.get(cache_key)

        if cached:
            print("  (from Redis cache)")
            return json.loads(cached)

        print("  (from MongoDB)")

        # Query MongoDB
        nodes_collection = self.mongo_db["nodes"]
        edges_collection = self.mongo_db["edges"]

        # Get disease info
        disease = nodes_collection.find_one({"id": disease_id})
        if not disease:
            return None

        # Get treating drugs (CtD or CpD relationships)
        drug_edges = edges_collection.find(
            {"target": disease_id, "metaedge": {"$in": ["CtD", "CpD"]}}
        )

        drug_ids = [edge["source"] for edge in drug_edges]
        drugs = list(
            nodes_collection.find({"id": {"$in": drug_ids}}, {"name": 1, "_id": 0})
        )

        # Get associated genes (DaG, DuG, DdG relationships)
        gene_edges = edges_collection.find(
            {"source": disease_id, "metaedge": {"$in": ["DaG", "DuG", "DdG"]}}
        )

        gene_ids = [edge["target"] for edge in gene_edges]
        genes = list(
            nodes_collection.find({"id": {"$in": gene_ids}}, {"name": 1, "_id": 0})
        )

        # Get anatomical locations (DlA relationships)
        anatomy_edges = edges_collection.find({"source": disease_id, "metaedge": "DlA"})

        anatomy_ids = [edge["target"] for edge in anatomy_edges]
        anatomies = list(
            nodes_collection.find({"id": {"$in": anatomy_ids}}, {"name": 1, "_id": 0})
        )

        # Build result
        profile = {
            "disease_id": disease_id,
            "disease_name": disease.get("name"),
            "drugs": [d["name"] for d in drugs],
            "genes": [g["name"] for g in genes],
            "anatomies": [a["name"] for a in anatomies],
        }

        # Cache the result
        self.redis_client.set(cache_key, json.dumps(profile))

        return profile

    def query2_drug_repurposing(self, disease_id):
        """
        Query 2: Drug Repurposing
        Uses MongoDB with multi-step queries
        """

        # Check Redis cache
        cache_key = f"drug:repurposing:{disease_id}"
        cached = self.redis_client.get(cache_key)

        if cached:
            print("  (from Redis cache)")
            return json.loads(cached)

        print("  (from MongoDB)")

        nodes_collection = self.mongo_db["nodes"]
        edges_collection = self.mongo_db["edges"]

        # Step 1: Get anatomies where disease occurs (Disease -DlA-> Anatomy)
        anatomy_edges = edges_collection.find({"source": disease_id, "metaedge": "DlA"})
        anatomy_ids = [edge["target"] for edge in anatomy_edges]

        if not anatomy_ids:
            return []

        # Step 2: Get genes that anatomies regulate (Anatomy -AdG/AuG-> Gene)
        anatomy_gene_edges = list(
            edges_collection.find(
                {"source": {"$in": anatomy_ids}, "metaedge": {"$in": ["AdG", "AuG"]}}
            )
        )

        # Step 3: Find compounds with opposite regulation
        candidates = set()

        for ag_edge in anatomy_gene_edges:
            gene_id = ag_edge["target"]
            anatomy_regulation = ag_edge["metaedge"]  # 'AdG' or 'AuG'

            # Find compounds that regulate this gene in OPPOSITE way
            if anatomy_regulation == "AdG":
                # Anatomy down-regulates, find compounds that UP-regulate
                compound_edges = edges_collection.find(
                    {"target": gene_id, "metaedge": "CuG"}
                )
            else:  # anatomy_regulation == 'AuG'
                # Anatomy up-regulates, find compounds that DOWN-regulate
                compound_edges = edges_collection.find(
                    {"target": gene_id, "metaedge": "CdG"}
                )

            for comp_edge in compound_edges:
                compound_id = comp_edge["source"]

                # Check this compound doesn't already treat the disease
                existing_treatment = edges_collection.find_one(
                    {
                        "source": compound_id,
                        "target": disease_id,
                        "metaedge": {"$in": ["CtD", "CpD"]},
                    }
                )

                if not existing_treatment:
                    candidates.add(compound_id)

        # Get compound names
        result = []
        for compound_id in candidates:
            compound = nodes_collection.find_one({"id": compound_id})
            if compound:
                result.append(
                    {"compound_id": compound_id, "compound_name": compound["name"]}
                )

        # Sort by name
        result.sort(key=lambda x: x["compound_name"])

        # Cache the result
        self.redis_client.set(cache_key, json.dumps(result))

        return result
