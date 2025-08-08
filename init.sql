CREATE TABLE IF NOT EXISTS domains (
    id BIGSERIAL PRIMARY KEY,
    domain VARCHAR(255) UNIQUE NOT NULL,
    status VARCHAR(20) NOT NULL,
    ip_address INET,
    http_status INTEGER,
    title VARCHAR(1000),
    content_length INTEGER,
    server_header VARCHAR(200),
    content_type VARCHAR(100),
    discovered_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    validated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);
            
CREATE INDEX IF NOT EXISTS idx_domains_status ON domains(status);
CREATE INDEX IF NOT EXISTS idx_domains_discovered ON domains(discovered_at);
CREATE INDEX IF NOT EXISTS idx_domains_domain_hash ON domains USING hash(domain);