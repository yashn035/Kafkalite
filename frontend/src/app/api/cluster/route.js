import { NextResponse } from 'next/server';

export async function GET() {
  // The three brokers and their admin HTTP ports as per docker-compose.yml
  const brokers = [
    { id: 0, url: 'http://localhost:8081/health', name: 'Broker 0' },
    { id: 1, url: 'http://localhost:8083/health', name: 'Broker 1' },
    { id: 2, url: 'http://localhost:8085/health', name: 'Broker 2' }
  ];

  const results = await Promise.all(
    brokers.map(async (broker) => {
      try {
        const res = await fetch(broker.url, { 
          cache: 'no-store',
          signal: AbortSignal.timeout(2000) // 2 second timeout
        });
        
        if (res.ok) {
          const data = await res.json();
          return {
            ...broker,
            status: 'Healthy',
            isLeaderFor: data.is_leader_for || [],
            partitionCount: data.partition_count || 0,
          };
        }
        return { ...broker, status: 'Unhealthy', error: `HTTP ${res.status}` };
      } catch (err) {
        return { ...broker, status: 'Unreachable', error: err.message };
      }
    })
  );

  return NextResponse.json({ brokers: results });
}
