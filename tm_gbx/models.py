"""Data models for GBX parser."""

from dataclasses import dataclass
from typing import List, Dict, Optional


@dataclass
class Vec3:
    """3D vector for position/velocity."""
    x: float
    y: float
    z: float
    
    def to_dict(self) -> Dict[str, float]:
        """Convert to dictionary."""
        return {"x": self.x, "y": self.y, "z": self.z}


@dataclass
class GhostSample:
    """Single ghost data sample."""
    time_ms: int
    position: Vec3
    velocity: Vec3
    speed: float
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "time_ms": self.time_ms,
            "position": self.position.to_dict(),
            "velocity": self.velocity.to_dict(),
            "speed": self.speed
        }


@dataclass
class Metadata:
    """Replay metadata."""
    player_login: Optional[str] = None
    player_nickname: Optional[str] = None
    map_name: Optional[str] = None
    map_uid: Optional[str] = None
    map_author: Optional[str] = None
    race_time_ms: Optional[int] = None
    checkpoints: Optional[List[int]] = None
    num_respawns: Optional[int] = 0
    game_version: Optional[str] = None
    title_id: Optional[str] = None
    
    def to_dict(self) -> dict:
        """Convert to dictionary."""
        return {
            "player_login": self.player_login,
            "player_nickname": self.player_nickname,
            "map_name": self.map_name,
            "map_uid": self.map_uid,
            "map_author": self.map_author,
            "race_time_ms": self.race_time_ms,
            "checkpoints": self.checkpoints or [],
            "num_respawns": self.num_respawns,
            "game_version": self.game_version,
            "title_id": self.title_id
        }
