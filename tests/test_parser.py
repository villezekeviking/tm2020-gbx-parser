"""Tests for GBX parser."""

import os
import pytest
from tm_gbx import parse_gbx


class TestGBXParser:
    """Test GBX parser functionality."""
    
    @pytest.fixture
    def test_files_dir(self):
        """Get path to test files directory."""
        return os.path.dirname(__file__)
    
    @pytest.fixture
    def all_test_files(self, test_files_dir):
        """Get all test .Gbx files."""
        files = [
            "Johan (Best).Gbx",
            "Johan (First).Gbx",
            "Jon (Best).Gbx",
            "Jon (First).Gbx",
            "Oskar (Best).gbx",
            "Oskar (First).gbx",
            "Ville (Best).Gbx",
            "Ville (First).Gbx"
        ]
        return [os.path.join(test_files_dir, f) for f in files]
    
    def test_parser_doesnt_crash_on_all_files(self, all_test_files):
        """Test parser doesn't crash on any of the 8 test files."""
        for filepath in all_test_files:
            if not os.path.exists(filepath):
                pytest.skip(f"Test file not found: {filepath}")
            
            # Should not raise exception
            result = parse_gbx(filepath)
            
            # Check basic structure
            assert 'metadata' in result
            assert 'ghost_samples' in result
            assert isinstance(result['metadata'], dict)
            assert isinstance(result['ghost_samples'], list)
    
    def test_parse_ville_best_metadata(self, test_files_dir):
        """Test parsing extracts correct metadata from Ville (Best).Gbx."""
        filepath = os.path.join(test_files_dir, "Ville (Best).Gbx")
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        result = parse_gbx(filepath)
        metadata = result['metadata']
        
        # Verify specific values from the file
        assert metadata['player_nickname'] == 'VilleZekeViking'
        assert metadata['player_login'] == 'xtVxJniqQciL7b9biL1pzg'
        assert metadata['race_time_ms'] == 26335
        assert metadata['map_uid'] == 'L4ZaQ8GwLjMRAnm5xafWb2pvS_j'
        assert metadata['map_name'] == 'Fall 2023 - 03'
        assert metadata['map_author'] == 'Nadeo'
        assert metadata['title_id'] == 'TMStadium'
        assert metadata['num_checkpoints'] == 7
    
    def test_header_parsing_extracts_player_info(self, all_test_files):
        """Test header parsing extracts player info from all files."""
        for filepath in all_test_files:
            if not os.path.exists(filepath):
                continue
            
            result = parse_gbx(filepath)
            metadata = result['metadata']
            
            # All files should have at least player nickname
            assert 'player_nickname' in metadata
            assert metadata['player_nickname'] is not None
            assert len(metadata['player_nickname']) > 0
    
    def test_header_parsing_extracts_race_time(self, all_test_files):
        """Test header parsing extracts race time from all files."""
        for filepath in all_test_files:
            if not os.path.exists(filepath):
                continue
            
            result = parse_gbx(filepath)
            metadata = result['metadata']
            
            # All files should have race time
            assert 'race_time_ms' in metadata
            assert metadata['race_time_ms'] is not None
            assert metadata['race_time_ms'] > 0
    
    def test_ghost_samples_structure(self, test_files_dir):
        """Test ghost samples have correct structure when present."""
        filepath = os.path.join(test_files_dir, "Ville (Best).Gbx")
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        result = parse_gbx(filepath)
        ghost_samples = result['ghost_samples']
        
        # Ghost samples should be a list (may be empty if LZO not available)
        assert isinstance(ghost_samples, list)
        
        # If we have samples, check structure
        if len(ghost_samples) > 0:
            sample = ghost_samples[0]
            assert 'time_ms' in sample
            assert 'position' in sample
            
            # Check position structure
            position = sample['position']
            assert 'x' in position
            assert 'y' in position
            assert 'z' in position


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
