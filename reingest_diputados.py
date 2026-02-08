from processor import process_batch_generator
print("Starting re-ingestion of DIPUTADOS data...")
for msg in process_batch_generator(None, None):
    print(msg.strip())
print("Re-ingestion complete.")
