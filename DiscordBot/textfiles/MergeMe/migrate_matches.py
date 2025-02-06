import os
import asyncio
import aiohttp
from .MatchScoreFilter import MatchProcessor

async def migrate_matches():
    """
    Migrate matches to use prefixed addresses and merge acequad matches into ace_matchids.txt.
    """
    processor = MatchProcessor()
    
    # Paths
    ace_file = processor.ace_file
    quad_file = processor.quad_file
    acequad_file = os.path.join(processor.textfiles_dir, "acequad_matchids.txt")
    
    # Temporary files
    ace_temp = ace_file + ".temp"
    quad_temp = quad_file + ".temp"
    
    # Ensure main files exist
    processor.ensure_file_exists(ace_file)
    processor.ensure_file_exists(quad_file)
    
    # Set to track all processed matches
    processed_matches = set()
    
    # Create semaphore for API rate limiting
    semaphore = asyncio.Semaphore(processor.max_concurrent_requests)
    
    async with aiohttp.ClientSession() as session:
        # Read all existing matches
        ace_matches = set()
        quad_matches = set()
        acequad_matches = set()
        
        if os.path.exists(ace_file):
            with open(ace_file, "r") as f:
                ace_matches = {line.strip() for line in f if line.strip()}
        
        if os.path.exists(quad_file):
            with open(quad_file, "r") as f:
                quad_matches = {line.strip() for line in f if line.strip()}
        
        if os.path.exists(acequad_file):
            with open(acequad_file, "r") as f:
                acequad_matches = {line.strip() for line in f if line.strip()}
        
        # Process and update matches
        async def process_match(match_id):
            if match_id in processed_matches:
                return None, None
            
            print(f"\nProcessing match {match_id}")
            match_data = await processor.fetch_scoreboard(session, match_id, semaphore)
            if match_data:
                result = processor.analyze_match(match_data)
                result.match_id = match_id
                processed_matches.add(match_id)
                return match_id, result.formatted_match_id
            return None, None
        
        # Process all matches
        new_ace_matches = set()
        new_quad_matches = set()
        
        # Process ace matches
        print("\nProcessing ace matches...")
        tasks = [process_match(match_id) for match_id in ace_matches]
        results = await asyncio.gather(*tasks)
        for old_id, new_id in results:
            if new_id:
                new_ace_matches.add(new_id)
        
        # Process quad matches
        print("\nProcessing quad matches...")
        tasks = [process_match(match_id) for match_id in quad_matches]
        results = await asyncio.gather(*tasks)
        for old_id, new_id in results:
            if new_id:
                new_quad_matches.add(new_id)
        
        # Process acequad matches and add them to ace matches
        print("\nProcessing acequad matches...")
        tasks = [process_match(match_id) for match_id in acequad_matches]
        results = await asyncio.gather(*tasks)
        for old_id, new_id in results:
            if new_id:
                new_ace_matches.add(new_id)
        
        # Write updated matches to temporary files
        with open(ace_temp, "w") as f:
            for match_id in sorted(new_ace_matches, reverse=True):
                f.write(match_id + "\n")
        
        with open(quad_temp, "w") as f:
            for match_id in sorted(new_quad_matches, reverse=True):
                f.write(match_id + "\n")
        
        # Replace original files with updated ones
        os.replace(ace_temp, ace_file)
        os.replace(quad_temp, quad_file)
        
        # Remove acequad file if it exists
        if os.path.exists(acequad_file):
            os.remove(acequad_file)
    
    print("\nMigration complete!")

if __name__ == "__main__":
    asyncio.run(migrate_matches())
