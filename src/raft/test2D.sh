count=50
    for i in $(seq $count); do
        go test -run 2D -race
        sleep 1
    done