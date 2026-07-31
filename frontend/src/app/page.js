"use client";

import { useState, useEffect } from 'react';

export default function Dashboard() {
  const [brokers, setBrokers] = useState([]);
  const [loading, setLoading] = useState(true);
  const [refreshing, setRefreshing] = useState(false);

  const fetchClusterStatus = async () => {
    try {
      const res = await fetch('/api/cluster');
      if (res.ok) {
        const data = await res.json();
        setBrokers(data.brokers);
      }
    } catch (err) {
      console.error("Failed to fetch cluster status", err);
    } finally {
      setLoading(false);
      setRefreshing(false);
    }
  };

  useEffect(() => {
    fetchClusterStatus();
    const interval = setInterval(fetchClusterStatus, 3000); // Poll every 3 seconds
    return () => clearInterval(interval);
  }, []);

  const handleRefresh = () => {
    setRefreshing(true);
    fetchClusterStatus();
  };

  return (
    <div className="container">
      <header className="header">
        <div>
          <h1 className="title">KafkaLite Cluster</h1>
          <p className="subtitle">Real-time administration dashboard</p>
        </div>
        <button 
          className={`refresh-button ${refreshing ? 'loading' : ''}`} 
          onClick={handleRefresh}
        >
          <svg className="icon" width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
            <polyline points="23 4 23 10 17 10"></polyline>
            <polyline points="1 20 1 14 7 14"></polyline>
            <path d="M3.51 9a9 9 0 0 1 14.85-3.36L23 10M1 14l4.64 4.36A9 9 0 0 0 20.49 15"></path>
          </svg>
          {refreshing ? 'Refreshing...' : 'Refresh'}
        </button>
      </header>

      {loading ? (
        <div style={{ textAlign: 'center', marginTop: '40px', color: 'var(--text-secondary)' }}>
          Loading cluster status...
        </div>
      ) : (
        <div className="dashboard-grid">
          {brokers.map((broker) => (
            <div key={broker.id} className="broker-card">
              <div className="broker-header">
                <h2 className="broker-name">{broker.name}</h2>
                <div className={`status-badge status-${broker.status}`}>
                  {broker.status}
                </div>
              </div>
              
              <div className="broker-details">
                <div className="detail-row">
                  <span className="detail-label">Admin API</span>
                  <span className="detail-value" style={{fontFamily: 'monospace', fontSize: '0.85rem'}}>{broker.url.replace('/health', '')}</span>
                </div>
                
                {broker.status === 'Healthy' && (
                  <>
                    <div className="detail-row">
                      <span className="detail-label">Partition Count</span>
                      <span className="detail-value">{broker.partitionCount}</span>
                    </div>
                    <div style={{ marginTop: '16px' }}>
                      <span className="detail-label">Leading Topics:</span>
                      {broker.isLeaderFor && broker.isLeaderFor.length > 0 ? (
                        <div className="topics-list">
                          {broker.isLeaderFor.map((topic, idx) => (
                            <span key={idx} className="topic-tag">{topic}</span>
                          ))}
                        </div>
                      ) : (
                        <div style={{ color: 'var(--text-secondary)', fontSize: '0.85rem', marginTop: '8px', fontStyle: 'italic' }}>
                          Not currently leading any partitions
                        </div>
                      )}
                    </div>
                  </>
                )}

                {broker.status !== 'Healthy' && (
                  <div className="error-msg">
                    Error: {broker.error}
                  </div>
                )}
              </div>
            </div>
          ))}
        </div>
      )}
    </div>
  );
}
