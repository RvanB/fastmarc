from fastmarc import MARCReader

# Open with indexing enabled
with open("CUY.UCB_serials_test_combined.mrc", "rb") as f:
    reader = MARCReader(
        f, 
        enable_indexing=True,
        index_fields=("245$a",),  # Note: trailing comma makes it a tuple
        mask_lanes=4
    )
    
    print(f"Loaded {len(reader)} records")
    
    # Try different searches
    for term in ["music"]:
        results = reader.search("245", "a", term)
        print(f"'{term}': Found {len(results)} matches")

        for idx in results:
            record = reader.get_record(idx)
            title = record['245']['a']
            print(f"  Record {idx}: {title[:80]}")
