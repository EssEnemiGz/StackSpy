#!/usr/bin/env python3
"""
High-Probability Domain Generator & Validator
Genera dominios con alta probabilidad de existir - Optimizado para 10GB dataset
MacBook Air M2 - Sin dependencias cloud
"""

import asyncio
import aiohttp
import aiodns
import psycopg2
import random
import time
import gc
from datetime import datetime
import logging
from dotenv import load_dotenv
import os

load_dotenv(dotenv_path="../.env")

DB_CONFIG = {
    'host': os.getenv("DB_HOST"),
    'database': os.getenv("DB_NAME"),
    'user': os.getenv("DB_USER"), 
    'password': os.getenv("DB_PASSWORD"),
    'port': os.getenv("DB_PORT")
}

# Configurar logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

class HighProbabilityDomainGenerator:
    def __init__(self):
        self.dns_resolver = aiodns.DNSResolver(timeout=2.0)
        self.session = None
        self.db_conn = None
        self.domains_validated = 0
        self.start_time = time.time()
        
        self.domain_buffer = []
        self.batch_save_size = 1000
        self.last_save_time = time.time()
        
        self.load_intelligent_datasets()
        
    def load_intelligent_datasets(self):
        """Cargar datasets que maximizan probabilidad de dominios reales"""
        
        self.common_words = [
            'about', 'world', 'news', 'health', 'business', 'tech', 'digital', 'online',
            'cloud', 'data', 'smart', 'fast', 'easy', 'best', 'top', 'new', 'free',
            'home', 'work', 'life', 'love', 'money', 'time', 'people', 'water',
            'food', 'music', 'travel', 'photo', 'video', 'game', 'sport', 'car',
            'house', 'book', 'school', 'learn', 'help', 'support', 'service', 'shop',
            'store', 'market', 'sale', 'buy', 'cheap', 'price', 'deal', 'offer',
            'app', 'web', 'site', 'blog', 'forum', 'wiki', 'social', 'network',
            'mobile', 'phone', 'computer', 'internet', 'email', 'search', 'google',
            'facebook', 'twitter', 'youtube', 'amazon', 'apple', 'microsoft', 'netflix',
            'green', 'blue', 'red', 'black', 'white', 'gold', 'silver', 'royal',
            'super', 'mega', 'ultra', 'pro', 'plus', 'max', 'mini', 'micro',
            'global', 'local', 'national', 'international', 'american', 'european',
            'asian', 'african', 'central', 'north', 'south', 'east', 'west',
            'first', 'second', 'third', 'last', 'next', 'previous', 'current', 'future'
        ]
        
        self.first_names = [
            'john', 'james', 'robert', 'michael', 'william', 'david', 'richard', 'charles',
            'joseph', 'thomas', 'christopher', 'daniel', 'paul', 'mark', 'donald', 'george',
            'mary', 'patricia', 'jennifer', 'linda', 'elizabeth', 'barbara', 'susan', 'jessica',
            'sarah', 'karen', 'nancy', 'lisa', 'betty', 'helen', 'sandra', 'donna'
        ]
        
        self.last_names = [
            'smith', 'johnson', 'williams', 'brown', 'jones', 'garcia', 'miller', 'davis',
            'rodriguez', 'martinez', 'hernandez', 'lopez', 'gonzalez', 'wilson', 'anderson',
            'thomas', 'taylor', 'moore', 'jackson', 'martin', 'lee', 'perez', 'thompson'
        ]
        
        self.cities = [
            'newyork', 'london', 'paris', 'tokyo', 'sydney', 'toronto', 'berlin',
            'madrid', 'rome', 'amsterdam', 'vienna', 'zurich', 'moscow', 'dubai',
            'singapore', 'hongkong', 'bangkok', 'mumbai', 'delhi', 'shanghai',
            'beijing', 'seoul', 'oslo', 'stockholm', 'copenhagen', 'helsinki',
            'lisbon', 'athens', 'istanbul', 'cairo', 'lagos', 'nairobi', 'capetown'
        ]
        
        self.industries = [
            'tech', 'finance', 'health', 'education', 'retail', 'fashion', 'food',
            'travel', 'real estate', 'automotive', 'energy', 'media', 'gaming',
            'fitness', 'beauty', 'legal', 'consulting', 'marketing', 'design',
            'photography', 'music', 'art', 'sports', 'insurance', 'banking'
        ]
        
        self.tlds_weighted = {
            '.com': 70,
            '.org': 10,
            '.net': 8,
            '.io': 5,
            '.co': 3,
            '.app': 2,
            '.dev': 1,
            '.xyz': 1
        }
        
        # 6. Patrones temporales (años, eventos)
        current_year = datetime.now().year
        self.years = [str(year) for year in range(2000, current_year + 2)]
        self.decades = ['90s', '2000s', '2010s', '2020s']
        
        # 7. Números populares
        self.popular_numbers = [
            '1', '2', '3', '10', '20', '24', '100', '123', '2024', '2025',
            '007', '911', '365', '247', '360', '404', '500', '1000'
        ]
        
        logger.info("Datasets inteligentes cargados para alta probabilidad")
    
    def get_weighted_tld(self):
        """Seleccionar TLD basado en probabilidad ponderada"""
        tlds = list(self.tlds_weighted.keys())
        weights = list(self.tlds_weighted.values())
        return random.choices(tlds, weights=weights)[0]
    
    def generate_business_domains(self, count=50000):
        """Generar dominios tipo negocio/empresa"""
        domains = set()
        
        for industry in self.industries:
            for city in self.cities:
                if len(domains) >= count:
                    break
                tld = self.get_weighted_tld()
                patterns = [
                    f"{industry}{city}{tld}",
                    f"{city}{industry}{tld}",
                    f"{industry}-{city}{tld}",
                    f"{city}-{industry}{tld}",
                    f"best{industry}{tld}",
                    f"top{city}{tld}",
                    f"{industry}in{city}{tld}",
                    f"{city}{industry}guide{tld}"
                ]
                for pattern in patterns:
                    domains.add(pattern)
                    if len(domains) >= count:
                        break
        
        return list(domains)
    
    def generate_personal_domains(self, count=50000):
        """Generar dominios personales"""
        domains = set()
        
        for first in self.first_names:
            for last in self.last_names:
                if len(domains) >= count:
                    break
                tld = self.get_weighted_tld()
                patterns = [
                    f"{first}{last}{tld}",
                    f"{first}-{last}{tld}",
                    f"{first}.{last}{tld}",
                    f"{last}{first}{tld}",
                    f"{first}{last}blog{tld}",
                    f"{first}{last}site{tld}",
                    f"dr{first}{last}{tld}",
                    f"{first}{last}consulting{tld}"
                ]
                for pattern in patterns:
                    domains.add(pattern)
                    if len(domains) >= count:
                        break
        
        return list(domains)
    
    def generate_branded_domains(self, count=50000):
        """Generar dominios tipo marca/startup"""
        domains = set()
        
        for word1 in self.common_words[:100]:
            for word2 in self.common_words[:100]:
                if word1 == word2:
                    continue
                if len(domains) >= count:
                    break
                    
                tld = self.get_weighted_tld()
                patterns = [
                    f"{word1}{word2}{tld}",
                    f"{word1}-{word2}{tld}",
                    f"get{word1}{word2}{tld}",
                    f"my{word1}{word2}{tld}",
                    f"{word1}{word2}app{tld}",
                    f"{word1}{word2}hub{tld}",
                    f"{word1}{word2}lab{tld}",
                    f"the{word1}{word2}{tld}"
                ]
                for pattern in patterns:
                    domains.add(pattern)
                    if len(domains) >= count:
                        break
        
        return list(domains)
    
    def generate_trending_domains(self, count=30000):
        """Generar dominios con tendencias y números populares"""
        domains = set()
        
        trending_keywords = [
            'ai', 'crypto', 'blockchain', 'nft', 'metaverse', 'web3', 'defi',
            'saas', 'fintech', 'edtech', 'healthtech', 'cleantech', 'biotech',
            'sustainability', 'climate', 'carbon', 'solar', 'electric', 'ev',
            'remote', 'hybrid', 'digital', 'virtual', 'augmented', 'reality',
            'machine', 'learning', 'data', 'analytics', 'insights', 'automation'
        ]
        
        for keyword in trending_keywords:
            for word in self.common_words[:50]:
                for num in self.popular_numbers:
                    if len(domains) >= count:
                        break
                    tld = self.get_weighted_tld()
                    patterns = [
                        f"{keyword}{word}{tld}",
                        f"{word}{keyword}{tld}",
                        f"{keyword}{num}{tld}",
                        f"{keyword}{word}{num}{tld}",
                        f"next{keyword}{tld}",
                        f"future{keyword}{tld}",
                        f"{keyword}today{tld}",
                        f"{keyword}2024{tld}"
                    ]
                    for pattern in patterns:
                        domains.add(pattern)
                        if len(domains) >= count:
                            break
        
        return list(domains)
    
    def generate_geographic_domains(self, count=30000):
        """Generar dominios geográficos"""
        domains = set()
        
        geo_suffixes = ['guide', 'info', 'news', 'weather', 'events', 'hotels', 
                       'restaurants', 'things', 'visit', 'explore', 'discover']
        
        for city in self.cities:
            for suffix in geo_suffixes:
                for word in self.common_words[:30]:
                    if len(domains) >= count:
                        break
                    tld = self.get_weighted_tld()
                    patterns = [
                        f"{city}{suffix}{tld}",
                        f"{city}-{suffix}{tld}",
                        f"{city}{word}{tld}",
                        f"visit{city}{tld}",
                        f"explore{city}{tld}",
                        f"{city}travel{tld}",
                        f"{city}guide{tld}",
                        f"best{city}{tld}"
                    ]
                    for pattern in patterns:
                        domains.add(pattern)
                        if len(domains) >= count:
                            break
        
        return list(domains)
    
    def generate_typosquatting_domains(self, count=20000):
        """Generar dominios tipo typosquatting de sitios populares"""
        popular_sites = [
            'google', 'facebook', 'youtube', 'amazon', 'twitter', 'instagram',
            'linkedin', 'netflix', 'spotify', 'uber', 'airbnb', 'tesla',
            'apple', 'microsoft', 'adobe', 'zoom', 'slack', 'discord'
        ]
        
        domains = set()
        
        for site in popular_sites:
            if len(domains) >= count:
                break
                
            variations = [
                site.replace('o', '0'),
                site.replace('i', '1'),
                site.replace('e', '3'),
                site + 's',
                site + 'app',
                site + 'web',
                site + 'online',
                'my' + site,
                'the' + site,
                site + 'official'
            ]
            
            for variation in variations:
                tld = self.get_weighted_tld()
                domains.add(f"{variation}{tld}")
        
        return list(domains)
    
    async def __aenter__(self):
        connector = aiohttp.TCPConnector(limit=50, limit_per_host=10)
        timeout = aiohttp.ClientTimeout(total=3)
        self.session = aiohttp.ClientSession(connector=connector, timeout=timeout)
        
        self.db_conn = psycopg2.connect(**DB_CONFIG)
        return self
    
    async def __aexit__(self):
        self.flush_domains()
        if self.session:
            await self.session.close()
        if self.db_conn:
            self.db_conn.close()
    
    async def validate_dns(self, domain):
        """Validación DNS optimizada"""
        try:
            result = await self.dns_resolver.query(domain, 'A')
            if result:
                return result[0].host, True
        except:
            pass
        return None, False
    
    async def validate_http(self, domain):
        """Validación HTTP con más metadatos"""
        try:
            url = f"http://{domain}"
            async with self.session.get(url, allow_redirects=True) as response:
                if response.status < 500:
                    content = await response.text()
                    
                    title = "Unknown"
                    if '<title>' in content.lower():
                        start = content.lower().find('<title>') + 7
                        end = content.lower().find('</title>', start)
                        if end > start:
                            title = content[start:end].strip()[:1000]
                    
                    content_length = len(content)
                    server = response.headers.get('Server', '')[:200]
                    content_type = response.headers.get('Content-Type', '')[:100]
                    
                    return response.status, title, content_length, server, content_type
        except:
            pass
        return None, None, None, None, None
    
    def save_domain(self, domain, status, ip_address=None, http_status=None, 
                   title=None, content_length=None, server=None, content_type=None):
        """Agregar dominio al buffer con metadatos extendidos"""
        self.domain_buffer.append((
            domain, status, ip_address, http_status, title, 
            content_length, server, content_type
        ))
        
        if len(self.domain_buffer) >= self.batch_save_size:
            self.flush_domains()
    
    def flush_domains(self):
        """Guardado en lotes optimizado"""
        if not self.domain_buffer:
            return
            
        cursor = self.db_conn.cursor()
        try:
            from psycopg2.extras import execute_values
            
            execute_values(
                cursor,
                '''
                INSERT INTO domains (domain, status, ip_address, http_status, title, 
                                   content_length, server_header, content_type) 
                VALUES %s
                ON CONFLICT (domain) DO UPDATE SET
                    status = EXCLUDED.status,
                    ip_address = EXCLUDED.ip_address,
                    http_status = EXCLUDED.http_status,
                    title = EXCLUDED.title,
                    content_length = EXCLUDED.content_length,
                    server_header = EXCLUDED.server_header,
                    content_type = EXCLUDED.content_type,
                    validated_at = CURRENT_TIMESTAMP
                ''',
                self.domain_buffer,
                template=None,
                page_size=1000
            )
            
            self.db_conn.commit()
            saved_count = len(self.domain_buffer)
            logger.info(f"✅ Guardados {saved_count} dominios (Total validados: {self.domains_validated})")
            self.domain_buffer.clear()
            
        except Exception as e:
            logger.error(f"Error guardando lote: {e}")
            self.db_conn.rollback()
            self.domain_buffer.clear()
    
    async def validate_domain(self, domain):
        """Validación completa con metadatos extendidos"""
        ip_address, dns_valid = await self.validate_dns(domain)
        
        if not dns_valid:
            self.save_domain(domain, 'inactive')
            return False
        
        http_status, title, content_length, server, content_type = await self.validate_http(domain)
        
        if http_status:
            self.save_domain(domain, 'active', ip_address, http_status, 
                           title, content_length, server, content_type)
            return True
        else:
            self.save_domain(domain, 'dns_only', ip_address)
            return True
    
    async def process_batch(self, domains, batch_size=30):
        """Procesamiento optimizado para MacBook Air M2"""
        semaphore = asyncio.Semaphore(batch_size)
        
        async def validate_with_semaphore(domain):
            async with semaphore:
                result = await self.validate_domain(domain)
                self.domains_validated += 1
                
                if self.domains_validated % 500 == 0:
                    elapsed = time.time() - self.start_time
                    rate = self.domains_validated / elapsed
                    buffer_size = len(self.domain_buffer)
                    
                    estimated_gb = (self.domains_validated * 500) / (1024**3)  # ~500 bytes por dominio
                    
                    logger.info(f"✅ Validados: {self.domains_validated:,} | Rate: {rate:.1f}/sec | "
                              f"Buffer: {buffer_size} | Dataset: ~{estimated_gb:.2f}GB")
                
                # Guardado temporal cada 60 segundos
                current_time = time.time()
                if current_time - self.last_save_time > 60:
                    self.flush_domains()
                    self.last_save_time = current_time
                
                await asyncio.sleep(0.03)
                return result
        
        tasks = [validate_with_semaphore(domain) for domain in domains]
        results = await asyncio.gather(*tasks, return_exceptions=True)
        return results
    
    def get_detailed_stats(self):
        """Estadísticas detalladas del dataset"""
        cursor = self.db_conn.cursor()
        
        cursor.execute('''
            SELECT 
                status,
                COUNT(*) as count,
                ROUND(AVG(content_length)) as avg_size
            FROM domains 
            GROUP BY status
            ORDER BY count DESC
        ''')
        
        stats = {}
        total_domains = 0
        for row in cursor.fetchall():
            stats[row[0]] = {'count': row[1], 'avg_size': row[2] or 0}
            total_domains += row[1]
        
        cursor.execute('SELECT COUNT(*), SUM(COALESCE(content_length, 500)) FROM domains')
        total_count, total_size = cursor.fetchone()
        
        return {
            'total_domains': total_domains,
            'breakdown': stats,
            'estimated_size_gb': (total_size or 0) / (1024**3),
            'estimated_size_mb': (total_size or 0) / (1024**2)
        }

async def main():
    """Función principal optimizada para 10GB dataset"""
    logger.info("🚀 Iniciando High-Probability Domain Generator para 10GB dataset")
    
    try:
        async with HighProbabilityDomainGenerator() as generator:
            
            # Configuración para MacBook Air M2 - Optimizada para volumen
            batch_size = 5000       # Dominios por lote
            concurrent_validations = 30  # Validaciones simultáneas
            target_domains = 20_000_000  # 20 millones de dominios (~10GB)
            
            logger.info(f"🎯 Objetivo: {target_domains:,} dominios (~10GB dataset)")
            
            domains_generated = 0
            round_num = 0
            
            while domains_generated < target_domains:
                round_num += 1
                logger.info(f"\n=== 🔄 RONDA {round_num} ===")
                
                generation_strategies = [
                    ("Business Domains", generator.generate_business_domains),
                    ("Personal Domains", generator.generate_personal_domains), 
                    ("Branded Domains", generator.generate_branded_domains),
                    ("Trending Domains", generator.generate_trending_domains),
                    ("Geographic Domains", generator.generate_geographic_domains),
                    ("Typosquatting Domains", generator.generate_typosquatting_domains)
                ]
                
                for strategy_name, strategy_func in generation_strategies:
                    if domains_generated >= target_domains:
                        break
                        
                    logger.info(f"📋 Generando {strategy_name}...")
                    
                    domains = strategy_func(batch_size)
                    domains = list(set(domains))
                    logger.info(f"✅ Generados {len(domains):,} dominios únicos")
                    
                    chunk_size = concurrent_validations
                    for i in range(0, len(domains), chunk_size):
                        chunk = domains[i:i + chunk_size]
                        await generator.process_batch(chunk, concurrent_validations)
                        
                        domains_generated += len(chunk)
                        
                        if domains_generated >= target_domains:
                            break
                        
                        if domains_generated % 1000 == 0:
                            gc.collect()
                    
                    generator.flush_domains()
                    
                    progress = (domains_generated / target_domains) * 100
                    logger.info(f"📊 Progreso total: {progress:.1f}% ({domains_generated:,}/{target_domains:,})")
                
                # Pausa entre rondas para cooling
                await asyncio.sleep(3)
            
            final_stats = generator.get_detailed_stats()
            
            logger.info("\n" + "="*60)
            logger.info("🎉 GENERACIÓN COMPLETADA - ESTADÍSTICAS FINALES")
            logger.info("="*60)
            logger.info(f"📊 Total dominios procesados: {final_stats['total_domains']:,}")
            logger.info(f"💾 Tamaño estimado del dataset: {final_stats['estimated_size_gb']:.2f} GB")
            logger.info(f"💾 Tamaño estimado del dataset: {final_stats['estimated_size_mb']:.1f} MB")
            
            logger.info("\n📋 Desglose por estado:")
            for status, data in final_stats['breakdown'].items():
                percentage = (data['count'] / final_stats['total_domains']) * 100
                logger.info(f"   {status}: {data['count']:,} ({percentage:.1f}%) - Avg size: {data['avg_size']} bytes")
            
            elapsed_time = time.time() - generator.start_time
            hours = elapsed_time / 3600
            logger.info(f"\n⏱️  Tiempo total: {hours:.2f} horas")
            logger.info(f"🚀 Rate promedio: {final_stats['total_domains'] / elapsed_time:.1f} dominios/seg")
            
            logger.info("\n✅ Dataset de alta probabilidad generado exitosamente!")
            logger.info("💡 Estos dominios tienen mayor probabilidad de ser reales que generación aleatoria")
            
    except KeyboardInterrupt:
        logger.info("\n⏹️  Proceso interrumpido por el usuario")
        if 'generator' in locals():
            generator.flush_domains()
    except Exception as e:
        logger.error(f"❌ Error en el proceso principal: {e}")

if __name__ == "__main__":
    print("""
    🚀 HIGH-PROBABILITY DOMAIN GENERATOR
    ===================================
    """)
    
    asyncio.run(main())