"""Tests for GBX parser."""

import os
import sys
import pytest

# Add parent directory to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from tm_gbx import GBXParser


class TestGBXParser:
    """Test GBX parser functionality."""
    
    @pytest.fixture
    def test_file_path(self):
        """Get path to test replay file."""
        # Look for test file in tests directory
        test_file = os.path.join(os.path.dirname(__file__), "Ville (Best).Gbx")
        if not os.path.exists(test_file):
            pytest.skip(f"Test file not found: {test_file}")
        return test_file
        
    def test_parser_initialization(self, test_file_path):
        """Test parser can be initialized with a file."""
        parser = GBXParser(test_file_path)
        assert parser.file_path == test_file_path
        
    def test_parser_with_nonexistent_file(self):
        """Test parser raises error for nonexistent file."""
        with pytest.raises(FileNotFoundError):
            GBXParser("nonexistent.Gbx")
            
    def test_parse_metadata(self, test_file_path):
        """Test parsing extracts metadata correctly."""
        parser = GBXParser(test_file_path)
        data = parser.parse()
        
        # Check structure
        assert 'metadata' in data
        assert 'ghost_samples' in data
        
        metadata = data['metadata']
        
        # Check expected values from problem statement
        assert metadata['race_time_ms'] == 26335, f"Expected race_time_ms=26335, got {metadata['race_time_ms']}"
        assert metadata['map_name'] == "Fall 2023 - 03", f"Expected map_name='Fall 2023 - 03', got {metadata['map_name']}"
        assert metadata['player_login'] == "xtVxJniqQciL7b9biL1pzg", f"Expected player_login='xtVxJniqQciL7b9biL1pzg', got {metadata['player_login']}"
        assert metadata['player_nickname'] == "VilleZekeViking", f"Expected player_nickname='VilleZekeViking', got {metadata['player_nickname']}"
        assert len(metadata['checkpoints']) == 7, f"Expected 7 checkpoints, got {len(metadata['checkpoints'])}"
        
    def test_parse_returns_correct_structure(self, test_file_path):
        """Test parse returns expected data structure."""
        parser = GBXParser(test_file_path)
        data = parser.parse()
        
        # Check top-level keys
        assert 'metadata' in data
        assert 'ghost_samples' in data
        
        metadata = data['metadata']
        
        # Check metadata keys exist (even if None)
        expected_keys = [
            'player_login', 'player_nickname', 'map_name', 
            'map_uid', 'map_author', 'race_time_ms', 
            'checkpoints', 'num_respawns', 'game_version', 'title_id'
        ]
        
        for key in expected_keys:
            assert key in metadata, f"Missing metadata key: {key}"
            
    def test_ghost_samples_structure(self, test_file_path):
        """Test ghost samples have correct structure (even if empty)."""
        parser = GBXParser(test_file_path)
        data = parser.parse()
        
        ghost_samples = data['ghost_samples']
        assert isinstance(ghost_samples, list)
        
        # If we have samples, check structure
        if ghost_samples:
            sample = ghost_samples[0]
            assert 'time_ms' in sample
            assert 'position' in sample
            assert 'velocity' in sample
            assert 'speed' in sample
            
            # Check position/velocity structure
            for key in ['x', 'y', 'z']:
                assert key in sample['position']
                assert key in sample['velocity']


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
