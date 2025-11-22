from fastmarc import MARCReader

# Explicit indexing: call .index() with the fields you want searchable
with open("CUY.UCB_serials_test_combined.mrc", "rb") as f:
    reader = MARCReader(f).index("245$a")

    print(f"Loaded {len(reader)} records")

    # Try different searches
    for term in ["music"]:
        results = reader.search("245$a", term)
        print(f"'{term}': Found {len(results)} matches")

        for idx in results:
            record = reader.get_record(idx)
            title = record['245']['a']
            print(f"  Record {idx}: {title[:80]}")
