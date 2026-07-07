#!/bin/bash
# Deprecated: use Settings > Danger zone or POST /api/admin/clean-slate (admin JWT required).
# This script will delete all data from the server and start fresh.

echo "Cleaning api data..."

# Step one: delete all migration folders in the data/ folder, but not api-runtime.log or migrations.yaml
if [ -d "data/" ]; then
    # Find all items in data/ directory
    find data/ -mindepth 1 -maxdepth 1 | while read -r item; do
        # Get just the filename/dirname
        name=$(basename "$item")
        # Skip api-runtime.log and migrations.yaml
        if [ "$name" != "api-runtime.log" ] && [ "$name" != "migrations.yaml" ] && [ "$name" != "sylos.duckdb" ]; then
            # Delete the item (file or directory)
            rm -rf "$item"
        fi
    done
fi

# Step two: clear the log file api-runtime.log
if [ -f "data/api-runtime.log" ]; then
    > "data/api-runtime.log"
fi

# Step three: clear the migrations.yaml file to where it's just 'migrations:'
if [ -f "data/migrations.yaml" ]; then
    echo "migrations:" > "data/migrations.yaml"
else
    # Create the file if it doesn't exist
    mkdir -p data
    echo "migrations:" > "data/migrations.yaml"
fi

echo "Done cleaning api data."
