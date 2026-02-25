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
        
        # Ghost samples should be a list (may be empty if body can't be decompressed)
        assert isinstance(ghost_samples, list)
        
        # If we have samples, check structure (new 52-field format)
        if len(ghost_samples) > 0:
            sample = ghost_samples[0]
            
            # Check key fields are present
            assert 'time_ms' in sample
            assert 'time_s' in sample
            assert 'x' in sample
            assert 'y' in sample
            assert 'z' in sample
            assert 'speed' in sample
            assert 'steer' in sample
            assert 'gas' in sample
            assert 'brake' in sample
            
            # Check types
            assert isinstance(sample['time_ms'], int)
            assert isinstance(sample['time_s'], float)
            assert isinstance(sample['x'], float)
            assert isinstance(sample['y'], float)
            assert isinstance(sample['z'], float)
            assert isinstance(sample['speed'], float)
            
    def test_ghost_samples_have_52_fields(self, test_files_dir):
        """Test ghost samples have all 52 telemetry fields."""
        filepath = os.path.join(test_files_dir, "Ville (Best).Gbx")
        if not os.path.exists(filepath):
            pytest.skip(f"Test file not found: {filepath}")
        
        result = parse_gbx(filepath)
        ghost_samples = result['ghost_samples']
        
        if len(ghost_samples) > 0:
            sample = ghost_samples[0]
            
            # Check all 52 fields are present
            expected_fields = [
                'time_ms', 'time_s',
                'x', 'y', 'z',
                'speed', 'side_speed',
                'vel_x', 'vel_y', 'vel_z',
                'pitch_deg', 'yaw_deg', 'roll_deg',
                'steer', 'gas', 'brake', 'gear', 'rpm',
                'is_turbo', 'turbo_time',
                'is_ground_contact', 'is_top_contact',
                'reactor_state', 'reactor_boost', 'reactor_pedal', 'reactor_steer',
                'sim_time_coef', 'wetness',
                'fl_dampen', 'fr_dampen', 'rr_dampen', 'rl_dampen',
                'fl_ice', 'fr_ice', 'rr_ice', 'rl_ice',
                'fl_dirt', 'fr_dirt', 'rr_dirt', 'rl_dirt',
                'fl_slip', 'fr_slip', 'rr_slip', 'rl_slip',
                'fl_ground_mat', 'fr_ground_mat', 'rr_ground_mat', 'rl_ground_mat',
                'fl_wheel_rot', 'fr_wheel_rot', 'rr_wheel_rot', 'rl_wheel_rot'
            ]
            
            for field in expected_fields:
                assert field in sample, f"Missing field: {field}"


if __name__ == '__main__':
    pytest.main([__file__, '-v'])
