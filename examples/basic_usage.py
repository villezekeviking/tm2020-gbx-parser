"""Basic usage example for TM2020 GBX Parser."""

import os
import json

from tm_gbx import GBXParser


def main():
    """Run basic example."""
    # Path to test replay file
    test_file = os.path.join(os.path.dirname(__file__), '..', 'tests', 'Ville (Best).Gbx')
    
    if not os.path.exists(test_file):
        print(f"Error: Test file not found: {test_file}")
        print("Please ensure 'Ville (Best).Gbx' is in the tests/ directory")
        return
        
    # Parse replay file
    print(f"Parsing: {test_file}\n")
    parser = GBXParser(test_file)
    data = parser.parse()
    
    # Print metadata
    print("Replay Metadata:")
    print(f"  Player: {data['metadata']['player_nickname']} ({data['metadata']['player_login']})")
    print(f"  Map: {data['metadata']['map_name']}")
    print(f"  Time: {data['metadata']['race_time_ms']}ms")
    print(f"  Checkpoints: {len(data['metadata']['checkpoints'])}")
    
    if data['metadata']['checkpoints']:
        print(f"  Checkpoint times: {data['metadata']['checkpoints']}")
    
    # Check for ghost samples
    if data['ghost_samples']:
        print(f"\nGhost samples: {len(data['ghost_samples'])}")
        print("First few samples:")
        for sample in data['ghost_samples'][:5]:
            print(f"  Time {sample['time_ms']}ms: Position {sample['position']}, Speed {sample['speed']}")
    else:
        print("\nNo ghost samples available (telemetry extraction not yet implemented)")
    
    # Save to JSON
    output_file = 'output.json'
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\nSaved to {output_file}")


if __name__ == '__main__':
    main()
