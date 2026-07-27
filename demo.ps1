Write-Host "Starting KafkaLite cluster..."
docker-compose down -v
docker-compose up -d --build

Write-Host "Waiting for brokers to be healthy..."
while ($true) {
    try {
        $r0 = Invoke-WebRequest -Uri "http://localhost:8081/health" -UseBasicParsing -ErrorAction Stop
        $r1 = Invoke-WebRequest -Uri "http://localhost:8083/health" -UseBasicParsing -ErrorAction Stop
        $r2 = Invoke-WebRequest -Uri "http://localhost:8085/health" -UseBasicParsing -ErrorAction Stop
        if ($r0.StatusCode -eq 200 -and $r1.StatusCode -eq 200 -and $r2.StatusCode -eq 200) {
            break
        }
    } catch {
        Write-Host -NoNewline "."
    }
    Start-Sleep -Seconds 1
}
Write-Host " Cluster is healthy!"

Write-Host "Producing initial 1000 messages..."
go run cmd/client/main.go --mode produce --broker localhost:9092 --topic test --messages 1000 --log

Write-Host "Starting Consumer A and Consumer B..."
$jobA = Start-Job -ScriptBlock { go run cmd/client/main.go --mode consume --broker localhost:9092 --topic test --group my-group --log --consumer-id A }
$jobB = Start-Job -ScriptBlock { go run cmd/client/main.go --mode consume --broker localhost:9093 --topic test --group my-group --log --consumer-id B }

Write-Host "Sleeping 5 seconds before killing leader..."
Start-Sleep -Seconds 5

Write-Host "Killing leader broker-0..."
docker stop kafkalite-broker-0

Write-Host "Producing 500 more messages..."
go run cmd/client/main.go --mode produce --broker localhost:9093 --topic test --messages 500 --log

Write-Host "Sleeping 5 seconds to allow failover and consumption..."
Start-Sleep -Seconds 5

Write-Host "Cleaning up..."
Stop-Job $jobA
Stop-Job $jobB
docker-compose down -v

Write-Host "SUCCESS"
