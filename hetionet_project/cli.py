import sys
from database import DatabaseManager
from queries import QueryEngine


def print_banner():
    print("\n" + "=" * 60)
    print("  HetioNet Database System")
    print("=" * 60 + "\n")


def print_menu():
    print("\nCommands:")
    print("  1. setup      - Clear and load data into databases")
    print("  2. query1     - Disease profile query")
    print("  3. query2     - Drug repurposing query")
    print("  4. exit       - Exit the program")
    print()


def setup_database(db_manager):
    print("\n--- Setting Up Database ---")
    confirm = input("This will clear existing data. Continue? (yes/no): ")

    if confirm.lower() != "yes":
        print("Setup cancelled.")
        return

    db_manager.clear_databases()
    db_manager.load_data()
    db_manager.create_indexes()
    print("\n✓ Setup complete!")


def run_query1(query_engine):
    print("\n--- Query 1: Disease Profile ---")
    disease_id = input("Enter disease ID (e.g., Disease::DOID:0050156): ").strip()

    print(f"\nSearching for {disease_id}...")
    result = query_engine.query1_disease_profile(disease_id)

    if not result:
        print(f"Disease {disease_id} not found.")
        return

    print("\n" + "=" * 60)
    print(f"Disease: {result['disease_name']}")
    print("=" * 60)

    print(f"\nTreating Drugs ({len(result['drugs'])}):")
    if result["drugs"]:
        for drug in result["drugs"]:
            print(f"  • {drug}")
    else:
        print("  (none)")

    print(f"\nAssociated Genes ({len(result['genes'])}):")
    if result["genes"]:
        for gene in result["genes"][:10]:  # Show first 10
            print(f"  • {gene}")
        if len(result["genes"]) > 10:
            print(f"  ... and {len(result['genes']) - 10} more")
    else:
        print("  (none)")

    print(f"\nAnatomical Locations ({len(result['anatomies'])}):")
    if result["anatomies"]:
        for anatomy in result["anatomies"]:
            print(f"  • {anatomy}")
    else:
        print("  (none)")

    print()


def run_query2(query_engine):
    print("\n--- Query 2: Drug Repurposing ---")
    disease_id = input("Enter disease ID (e.g., Disease::DOID:0050156): ").strip()

    print(f"\nFinding repurposing candidates for {disease_id}...")
    results = query_engine.query2_drug_repurposing(disease_id)

    print("\n" + "=" * 60)
    print(f"Drug Repurposing Candidates: {len(results)} compounds found")
    print("=" * 60 + "\n")

    if results:
        for i, compound in enumerate(results[:20], 1):  # Show first 20
            print(f"{i:2d}. {compound['compound_name']} ({compound['compound_id']})")

        if len(results) > 20:
            print(f"\n... and {len(results) - 20} more compounds")
    else:
        print("No repurposing candidates found.")

    print()


def main():
    print_banner()

    # Initialize database connections
    try:
        db_manager = DatabaseManager()
        query_engine = QueryEngine(db_manager.neo4j_driver, db_manager.redis_client)
    except Exception as e:
        print(f"Error connecting to databases: {e}")
        print("\nMake sure Neo4j and Redis are running!")
        sys.exit(1)

    # Main loop
    while True:
        print_menu()
        command = input("Enter command: ").strip().lower()

        if command == "1" or command == "setup":
            setup_database(db_manager)

        elif command == "2" or command == "query1":
            run_query1(query_engine)

        elif command == "3" or command == "query2":
            run_query2(query_engine)

        elif command == "4" or command == "exit":
            print("\nClosing connections...")
            db_manager.close()
            print("Goodbye!\n")
            break

        else:
            print("Invalid command. Please try again.")


if __name__ == "__main__":
    main()
