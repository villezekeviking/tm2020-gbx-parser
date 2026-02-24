"""Basic usage example for TM2020 GBX Parser."""

import os
import json

from tm_gbx import parse_gbx


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
    data = parse_gbx(test_file)
    
    # Print metadata
    metadata = data['metadata']
    print("Replay Metadata:")
    print(f"  Player: {metadata.get('player_nickname', '?')} ({metadata.get('player_login', '?')})")
    print(f"  Map: {metadata.get('map_name', '?')}")
    print(f"  Map UID: {metadata.get('map_uid', '?')}")
    print(f"  Time: {metadata.get('race_time_ms', 0)}ms")
    print(f"  Title: {metadata.get('title_id', '?')}")
    
    # Check for ghost samples
    if data['ghost_samples']:
        print(f"\nGhost samples: {len(data['ghost_samples'])}")
        print("First few samples:")
        for i, sample in enumerate(data['ghost_samples'][:5]):
            pos = sample['position']
            print(f"  {i}: Time {sample['time_ms']}ms - "
                  f"Position ({pos['x']:.2f}, {pos['y']:.2f}, {pos['z']:.2f})")
    else:
        print("\nNo ghost samples (requires python-lzo for body decompression)")
    
    # Save to JSON
    output_file = 'output.json'
    with open(output_file, 'w') as f:
        json.dump(data, f, indent=2)
    
    print(f"\nSaved to {output_file}")


if __name__ == '__main__':
    main()
