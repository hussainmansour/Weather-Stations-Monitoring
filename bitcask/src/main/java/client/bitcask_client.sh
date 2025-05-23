#!/bin/bash

SERVER_URL="http://localhost:8085/bitcask"
TIMESTAMP=$(date +%s)

function view_all() {
    output_file="${TIMESTAMP}.csv"
    curl -s "$SERVER_URL/view-all" | jq -r '.[] | "\(.key),\(.value)"' > "$output_file"
    echo "Saved to $output_file"
}

function view_key() {
    key="$1"
    response=$(curl -s "$SERVER_URL/view?key=$key")
    if echo "$response" | jq -e . >/dev/null 2>&1; then
        value=$(echo "$response" | jq -r '.value')
        echo "Key: $key => Value: $value"
    else
        echo "Key not found or server error"
    fi
}

function perf_test() {
    client_count="$1"
    for ((i=1; i<=client_count; i++)); do
        (
            output_file="${TIMESTAMP}_thread_${i}.csv"
            curl -s "$SERVER_URL/view-all" | jq -r '.[] | "\(.key),\(.value)"' > "$output_file"
        ) &
    done
    wait
    echo "Performance test complete with $client_count clients"
}

case "$1" in
    --view-all)
        view_all
        ;;
    --view)
        key="${2#--key=}"
        if [[ -z "$key" ]]; then
            echo "Error: --key=KEY is required"
            exit 1
        fi
        view_key "$key"
        ;;
    --perf)
        clients="${2#--clients=}"
        if ! [[ "$clients" =~ ^[0-9]+$ ]]; then
            echo "Error: --clients must be a number"
            exit 1
        fi
        perf_test "$clients"
        ;;
    *)
        echo "Usage:"
        echo "  ./bitcask_client.sh --view-all"
        echo "  ./bitcask_client.sh --view --key=SOME_KEY"
        echo "  ./bitcask_client.sh --perf --clients=100"
        exit 1
        ;;
esac
