import asyncio
import json
from MatchScoreFilter import MatchProcessor, MatchResult

def test_match_result():
    """Test that MatchResult correctly handles different ace/quad combinations"""
    textfiles_dir = "test_dir"
    
    # Test case 1: Match with only ace
    result1 = MatchResult("test1", textfiles_dir)
    result1.has_ace = True
    result1.ace_count = 1
    result1.quad_count = 0
    assert result1.formatted_match_id == "0100_test1"
    assert "ace_matchids.txt" in result1.target_file
    
    # Test case 2: Match with ace and quad
    result2 = MatchResult("test2", textfiles_dir)
    result2.has_ace = True
    result2.has_quad = True
    result2.ace_count = 2
    result2.quad_count = 3
    assert result2.formatted_match_id == "0203_test2"
    assert "ace_matchids.txt" in result2.target_file
    
    # Test case 3: Match with only quad (should not go to ace file)
    result3 = MatchResult("test3", textfiles_dir)
    result3.has_quad = True
    result3.quad_count = 2
    assert result3.formatted_match_id == "0002_test3"
    assert "unapproved_matchids.txt" in result3.target_file

    print("All test cases passed!")
    print("\nTest Results:")
    print(f"Case 1 (ace only): {result1.formatted_match_id} -> {result1.target_file}")
    print(f"Case 2 (ace+quad): {result2.formatted_match_id} -> {result2.target_file}")
    print(f"Case 3 (quad only): {result3.formatted_match_id} -> {result3.target_file}")

if __name__ == "__main__":
    test_match_result()
