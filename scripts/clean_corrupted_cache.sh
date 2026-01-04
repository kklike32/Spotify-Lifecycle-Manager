#!/bin/bash
# Clean corrupted track cache entries (missing artist_names)

echo "Finding corrupted entries..."
corrupted=$(aws dynamodb scan --table-name spotify-tracks \
    --projection-expression "track_id,artist_names" \
    --output json | \
    jq -r '.Items[] | select(.artist_names == null or (.artist_names.L | length == 0)) | .track_id.S')

count=$(echo "$corrupted" | wc -l | tr -d ' ')
echo "Found $count corrupted entries"

if [ "$count" -gt 0 ]; then
    echo "Deleting corrupted entries..."
    echo "$corrupted" | while read -r track_id; do
        echo "  Deleting: $track_id"
        aws dynamodb delete-item --table-name spotify-tracks \
            --key "{\"track_id\":{\"S\":\"$track_id\"}}" 2>/dev/null
    done
    echo "Cleanup complete!"
else
    echo "No corrupted entries found"
fi
