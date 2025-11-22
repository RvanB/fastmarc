from fastmarc import MARCReader

# Explicit indexing: use .add_index() to register fields, then .index() to build
with open("CUY.UCB_serials_test_combined.mrc", "rb") as f:
    reader = MARCReader(f).add_index("245$a").index()

    print(f"Loaded {len(reader)} records")

    # Try different searches
    for term in ["music"]:
        results = reader.search("245$a", term)
        print(f"'{term}': Found {len(results)} matches")

        for idx in results:
            record = reader.get_record(idx)
            title = record['245']['a']
            print(f"  Record {idx}: {title[:80]}")
