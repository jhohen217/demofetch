import os
import asyncio
import aiohttp
from .score_filter import MatchProcessor

async def migrate_matches():
    """
    Migrate quad matches from quad_matchids_old.txt to quad_matchids.txt with new naming schema.
    """
    processor = MatchProcessor()
    
    # Paths
    old_quad_file = os.path.join(processor.textfiles_dir, "quad_matchids_old.txt")
    quad_file = processor.quad_file
    
    # Ensure files exist
    processor.ensure_file_exists(quad_file)
    
    # Set to track all processed matches
    processed_matches = set()
    
    # Create semaphore for API rate limiting
    semaphore = asyncio.Semaphore(processor.max_concurrent_requests)
    
    async with aiohttp.ClientSession() as session:
        # Process quad matches from old file
        print("\nProcessing matches from quad_matchids_old.txt...")
        if os.path.exists(old_quad_file):
            with open(old_quad_file, "r") as f:
                matches = [line.strip() for line in f if line.strip()]
            
            # Filter out already processed matches
            matches = [m for m in matches if m not in processed_matches]
            
            # Temporary storage for new formatted IDs
            new_matches = []
            
            for match_id in matches:
                print(f"\nProcessing match {match_id}")
                match_data = await processor.fetch_scoreboard(session, match_id, semaphore)
                if match_data:
                    result = processor.analyze_match(match_data)
                    result.match_id = match_id
                    if result.has_quad:  # Only include if it actually has quad kills
                        new_matches.append(result.formatted_match_id)
                        print(f"Reformatted match {match_id} -> {result.formatted_match_id}")
                    processed_matches.add(match_id)
                await asyncio.sleep(processor.rate_limit_delay)
            
            # Read existing matches from quad_matchids.txt
            existing_matches = []
            if os.path.exists(quad_file):
                with open(quad_file, "r") as f:
                    existing_matches = [line.strip() for line in f if line.strip()]
            
            # Combine existing matches with new ones and sort
            all_matches = existing_matches + new_matches
            all_matches.sort(reverse=True)
            
            # Write all matches back to file
            with open(quad_file, "w") as f:
                for match_id in all_matches:
                    f.write(match_id + "\n")
    
    print("\nMigration complete!")

if __name__ == "__main__":
    asyncio.run(migrate_matches())
