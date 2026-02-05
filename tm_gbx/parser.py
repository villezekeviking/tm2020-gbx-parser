"""Main GBX parser implementation."""

import os
import logging
from typing import Dict, List, Any, Optional
from .reader import GBXReader
from .models import Metadata, GhostSample, Vec3
from .chunks import ChunkParser

logger = logging.getLogger(__name__)

# Try to import pygbx for metadata extraction
try:
    from pygbx import Gbx, GbxType
    PYGBX_AVAILABLE = True
except ImportError:
    PYGBX_AVAILABLE = False


class GBXParser:
    """Parser for TrackMania 2020 GBX replay files."""
    
    def __init__(self, file_path: str):
        """Initialize parser with GBX file path.
        
        Args:
            file_path: Path to the .Gbx replay file
        """
        if not os.path.exists(file_path):
            raise FileNotFoundError(f"File not found: {file_path}")
        
        self.file_path = file_path
        self.metadata = Metadata()
        self.ghost_samples = []
        
    def parse(self) -> Dict[str, Any]:
        """Parse GBX file and return data dictionary.
        
        Returns:
            Dictionary with 'metadata' and 'ghost_samples' keys
        """
        # Phase 1: Use pygbx for reliable metadata extraction
        if PYGBX_AVAILABLE:
            self._parse_with_pygbx()
        else:
            # Fallback to custom parsing
            self._parse_custom()
            
        # Phase 2: Try to extract ghost samples (future enhancement)
        # For now, we focus on metadata which pygbx handles well
        
        return {
            "metadata": self.metadata.to_dict(),
            "ghost_samples": [sample.to_dict() for sample in self.ghost_samples]
        }
        
    def _parse_with_pygbx(self):
        """Parse using pygbx library for metadata."""
        try:
            g = Gbx(self.file_path)
            
            # Get ghost class
            ghost = g.get_class_by_id(GbxType.CTN_GHOST)
            
            if ghost:
                # Extract metadata from ghost
                if hasattr(ghost, 'race_time'):
                    self.metadata.race_time_ms = ghost.race_time
                
                if hasattr(ghost, 'cp_times'):
                    self.metadata.checkpoints = ghost.cp_times
                    
                if hasattr(ghost, 'login'):
                    self.metadata.player_login = ghost.login
                    
                if hasattr(ghost, 'nickname'):
                    self.metadata.player_nickname = ghost.nickname
                    
                if hasattr(ghost, 'nb_respawns'):
                    self.metadata.num_respawns = ghost.nb_respawns
                    
            # Get challenge (map) class
            challenge = g.get_class_by_id(GbxType.CHALLENGE)
            
            if challenge:
                if hasattr(challenge, 'map_uid'):
                    self.metadata.map_uid = challenge.map_uid
                    
                if hasattr(challenge, 'map_name'):
                    self.metadata.map_name = challenge.map_name
                    
                if hasattr(challenge, 'author_login'):
                    self.metadata.map_author = challenge.author_login
                    
            # Try to get version info
            if hasattr(g, 'version'):
                self.metadata.game_version = str(g.version)
                
        except Exception as e:
            # If pygbx fails, fall back to custom parsing
            logger.warning(f"pygbx parsing failed: {e}, falling back to custom parsing")
            self._parse_custom()
            
    def _parse_custom(self):
        """Custom GBX parsing (fallback when pygbx not available)."""
        with open(self.file_path, 'rb') as f:
            reader = GBXReader(f)
            
            # Parse header
            header_data = self._parse_header(reader)
            
            # Parse metadata from chunks
            metadata_dict = self._parse_metadata(reader)
            
            # Update metadata from parsed data
            for key, value in metadata_dict.items():
                if hasattr(self.metadata, key) and value is not None:
                    setattr(self.metadata, key, value)
                    
    def _parse_header(self, reader: GBXReader) -> Dict[str, Any]:
        """Parse GBX header.
        
        Returns:
            Dictionary with header information
        """
        # Read magic bytes
        magic = reader.read_bytes(3)
        if magic != b'GBX':
            raise ValueError(f"Invalid GBX file: magic bytes are {magic}")
            
        # Read version
        version = reader.read_uint16()
        
        # Read format byte
        format_byte = reader.read_uint8()
        
        # Read compression info
        ref_table_compressed = reader.read_uint8()
        body_compressed = reader.read_uint8()
        
        # Read unknown byte
        unknown = reader.read_uint8()
        
        # Read class ID
        class_id = reader.read_uint32()
        
        return {
            'version': version,
            'format': chr(format_byte) if 32 <= format_byte < 127 else format_byte,
            'class_id': class_id,
            'ref_table_compressed': chr(ref_table_compressed) if 32 <= ref_table_compressed < 127 else ref_table_compressed,
            'body_compressed': chr(body_compressed) if 32 <= body_compressed < 127 else body_compressed
        }
        
    def _parse_metadata(self, reader: GBXReader) -> Dict[str, Any]:
        """Extract metadata from header chunks.
        
        Returns:
            Dictionary with metadata fields
        """
        metadata = {}
        
        try:
            # Read user data size
            user_data_size = reader.read_uint32()
            
            if user_data_size > 0:
                # Read number of header chunks
                num_chunks = reader.read_uint32()
                
                # Read each chunk
                for _ in range(num_chunks):
                    chunk_id = reader.read_uint32()
                    chunk_size = reader.read_uint32()
                    
                    # Parse known chunks
                    if chunk_id == 0x03093000:
                        chunk_data = ChunkParser.parse_chunk_0x03093000(reader, chunk_size)
                        metadata.update(chunk_data)
                    elif chunk_id == 0x03093002:
                        chunk_data = ChunkParser.parse_chunk_0x03093002(reader, chunk_size)
                        metadata.update(chunk_data)
                    elif chunk_id == 0x03093018:
                        chunk_data = ChunkParser.parse_chunk_0x03093018(reader, chunk_size)
                        metadata.update(chunk_data)
                    else:
                        # Skip unknown chunks
                        reader.read_bytes(chunk_size)
                        
        except Exception as e:
            logger.warning(f"Error parsing metadata chunks: {e}")
            
        return metadata
        
    def _parse_ghost_data(self):
        """Extract ghost samples (future enhancement).
        
        This is a placeholder for future implementation of full
        telemetry data extraction.
        """
        # TODO: Implement ghost sample extraction
        # This requires deeper understanding of TM2020 binary format
        # and handling of compressed data
        pass
