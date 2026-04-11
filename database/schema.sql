-- 论文数据库系统 - 建表脚本（改进版）
-- 创建时间: 2025
-- 说明: 根据 table.txt 设计的完整数据库表结构
-- 基于实际 JSON 数据结构优化

-- ============================================================================
-- 1. papers（主表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS papers (
    paper_id SERIAL PRIMARY KEY,
    work_id VARCHAR(200),  -- 业务唯一标识符，用于跨数据库索引
    arxiv_id VARCHAR(50),
    title TEXT NOT NULL,
    abstract TEXT,
    keywords TEXT,
    pdf_url VARCHAR(500),
    source_url VARCHAR(500),
    year INTEGER,
    is_preprint BOOLEAN DEFAULT FALSE,
    is_published BOOLEAN DEFAULT FALSE,
    source VARCHAR(100) NOT NULL DEFAULT 'arxiv',
    platform VARCHAR(50),
    primary_category VARCHAR(50),
    doi VARCHAR(200),
    semantic_scholar_id VARCHAR(100),  -- Semantic Scholar ID
    pubmed_id VARCHAR(100),  -- PubMed ID
    journal_ref VARCHAR(500),
    comments TEXT,
    contribution_types JSONB,  -- 贡献类型数组，如 ["Method", "Theory"]
    created_at TIMESTAMP,  -- 首次提交时间，从 JSON 读取，不是默认当前时间
    updated_at TIMESTAMP,  -- 论文在 arXiv 的最后更新时间，从 JSON 读取，不是默认当前时间
    imported_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,  -- 论文导入到数据库的时间
    embedding_status SMALLINT DEFAULT 0,  -- 向量化状态: 0=raw(未处理), 1=exported(已导出为Arrow batch), 2=ready(已写入FAISS索引)
    shard_id INTEGER,  -- 文献所属的 shard ID，仅在 embedding_status=2 时赋值
    paper_type VARCHAR(100),  -- 论文类型
    primary_field VARCHAR(100),  -- 主要研究领域
    target_application_domain VARCHAR(200),  -- 目标应用领域
    is_llm_era BOOLEAN DEFAULT FALSE,  -- 是否为LLM时代论文
    short_reasoning VARCHAR(200)  -- 简短理由说明（最多50字）
);

-- 创建唯一约束
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_work_id ON papers(work_id) WHERE work_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_arxiv_id_unique ON papers(arxiv_id) WHERE arxiv_id IS NOT NULL;
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_doi_unique ON papers(doi) WHERE doi IS NOT NULL;

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_papers_source ON papers(source);
CREATE INDEX IF NOT EXISTS idx_papers_year ON papers(year);
CREATE INDEX IF NOT EXISTS idx_papers_arxiv_id ON papers(arxiv_id);
CREATE INDEX IF NOT EXISTS idx_papers_platform ON papers(platform);
CREATE INDEX IF NOT EXISTS idx_papers_primary_category ON papers(primary_category);
CREATE INDEX IF NOT EXISTS idx_papers_created_at ON papers(created_at);
CREATE INDEX IF NOT EXISTS idx_papers_imported_at ON papers(imported_at);
CREATE INDEX IF NOT EXISTS idx_papers_semantic_scholar_id ON papers(semantic_scholar_id) WHERE semantic_scholar_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_papers_pubmed_id ON papers(pubmed_id) WHERE pubmed_id IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_papers_contribution_types ON papers USING GIN(contribution_types) WHERE contribution_types IS NOT NULL;
CREATE INDEX IF NOT EXISTS idx_papers_embedding_status ON papers(embedding_status);
CREATE INDEX IF NOT EXISTS idx_papers_shard_id ON papers(shard_id);
CREATE INDEX IF NOT EXISTS idx_papers_embedding_status_shard_id ON papers(embedding_status, shard_id);
CREATE INDEX IF NOT EXISTS idx_papers_embedding_status_paper_id ON papers(embedding_status, paper_id);
CREATE INDEX IF NOT EXISTS idx_papers_paper_type ON papers(paper_type);
CREATE INDEX IF NOT EXISTS idx_papers_primary_field ON papers(primary_field);
CREATE INDEX IF NOT EXISTS idx_papers_target_application_domain ON papers(target_application_domain);
CREATE INDEX IF NOT EXISTS idx_papers_is_llm_era ON papers(is_llm_era);

-- 全文检索索引
CREATE INDEX IF NOT EXISTS idx_papers_title_fts ON papers USING GIN(to_tsvector('english', title));
CREATE INDEX IF NOT EXISTS idx_papers_abstract_fts ON papers USING GIN(to_tsvector('english', abstract));


-- ============================================================================
-- 2. paper_keywords（论文关键词表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_keywords (
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    keyword_type VARCHAR(50) NOT NULL,  -- task, method, contribution, dataset, metric, library, domain, concept, model
    keyword VARCHAR(200) NOT NULL,
    weight REAL DEFAULT 1.0 CHECK (weight >= 0.0 AND weight <= 1.0),  -- 权重/置信度 (0.0-1.0)
    source VARCHAR(50),  -- manual, ai_extract, keyword_match, llm_tag, paper_metadata
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (paper_id, keyword_type, keyword)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pk_paper_id ON paper_keywords(paper_id);
CREATE INDEX IF NOT EXISTS idx_pk_keyword_type ON paper_keywords(keyword_type);
CREATE INDEX IF NOT EXISTS idx_pk_keyword ON paper_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_pk_paper_id_keyword_type ON paper_keywords(paper_id, keyword_type);


-- ============================================================================
-- 3. paper_texts（PDF 文本 + embedding）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_texts (
    paper_id INTEGER PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
    pdf_path VARCHAR(500),
    pdf_hash VARCHAR(64),  -- SHA256 hash for deduplication
    embedding JSONB,  -- 存储向量（JSON格式存储向量）
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_paper_texts_pdf_hash ON paper_texts(pdf_hash);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_paper_texts_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_paper_texts_updated_at ON paper_texts;
CREATE TRIGGER trigger_update_paper_texts_updated_at
    BEFORE UPDATE ON paper_texts
    FOR EACH ROW
    EXECUTE FUNCTION update_paper_texts_updated_at();


-- ============================================================================
-- 4. paper_author_affiliation（作者—论文—机构表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_author_affiliation (
    paper_id INTEGER PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
    authors JSONB NOT NULL  -- 作者信息 JSON 数组，包含姓名、顺序、机构等
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_paa_paper_id ON paper_author_affiliation(paper_id);
CREATE INDEX IF NOT EXISTS idx_paa_authors ON paper_author_affiliation USING GIN(authors);


-- ============================================================================
-- 5. venues（期刊/会议/平台）
-- ============================================================================
CREATE TABLE IF NOT EXISTS venues (
    venue_id SERIAL PRIMARY KEY,
    venue_name VARCHAR(200) NOT NULL UNIQUE,
    venue_type VARCHAR(50) NOT NULL,  -- conference, journal, preprint, repository
    url VARCHAR(500),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_venues_type ON venues(venue_type);


-- ============================================================================
-- 6. paper_publications（论文最终发表信息）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_publications (
    pub_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    venue_id INTEGER NOT NULL REFERENCES venues(venue_id) ON DELETE CASCADE,
    publish_time DATE,
    presentation_type VARCHAR(50),  -- oral, poster, spotlight, null
    UNIQUE (paper_id, venue_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pp_paper_id ON paper_publications(paper_id);
CREATE INDEX IF NOT EXISTS idx_pp_venue_id ON paper_publications(venue_id);
CREATE INDEX IF NOT EXISTS idx_pp_publish_time ON paper_publications(publish_time);


-- ============================================================================
-- 7. categories（大领域/子领域）
-- ============================================================================
CREATE TABLE IF NOT EXISTS categories (
    cat_id SERIAL PRIMARY KEY,
    domain VARCHAR(50) NOT NULL,
    subdomain VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    UNIQUE (domain, subdomain)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_categories_domain ON categories(domain);
CREATE INDEX IF NOT EXISTS idx_categories_subdomain ON categories(subdomain);


-- ============================================================================
-- 8. paper_categories（论文分类 M:N 关联表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_categories (
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    cat_id INTEGER NOT NULL REFERENCES categories(cat_id) ON DELETE CASCADE,
    is_primary BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (paper_id, cat_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pc_paper_id ON paper_categories(paper_id);
CREATE INDEX IF NOT EXISTS idx_pc_cat_id ON paper_categories(cat_id);
CREATE INDEX IF NOT EXISTS idx_pc_is_primary ON paper_categories(is_primary);


-- ============================================================================
-- 9. paper_versions（论文版本管理表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_versions (
    version_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    version_num INTEGER NOT NULL,
    version VARCHAR(20),
    version_date DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE (paper_id, version_num)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pv_paper_id ON paper_versions(paper_id);
CREATE INDEX IF NOT EXISTS idx_pv_version_date ON paper_versions(version_date);


-- ============================================================================
-- 10. paper_citations（论文引用统计表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_citations (
    citation_id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL UNIQUE REFERENCES papers(paper_id) ON DELETE CASCADE,
    cited_by_count INTEGER DEFAULT 0,
    update_time DATE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pc_paper_id ON paper_citations(paper_id);
CREATE INDEX IF NOT EXISTS idx_pc_cited_by_count ON paper_citations(cited_by_count);
CREATE INDEX IF NOT EXISTS idx_pc_update_time ON paper_citations(update_time);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_paper_citations_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_paper_citations_updated_at ON paper_citations;
CREATE TRIGGER trigger_update_paper_citations_updated_at
    BEFORE UPDATE ON paper_citations
    FOR EACH ROW
    EXECUTE FUNCTION update_paper_citations_updated_at();


-- ============================================================================
-- 11. meta_update_logs（元数据更新日志）
-- ============================================================================
CREATE TABLE IF NOT EXISTS meta_update_logs (
    id SERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    field VARCHAR(100) NOT NULL,
    old_value TEXT,
    new_value TEXT,
    update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    source VARCHAR(50)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_mul_paper_id ON meta_update_logs(paper_id);
CREATE INDEX IF NOT EXISTS idx_mul_update_time ON meta_update_logs(update_time);
CREATE INDEX IF NOT EXISTS idx_mul_field ON meta_update_logs(field);


-- ============================================================================
-- 12. fields（研究领域细分类表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS fields (
    field_id SERIAL PRIMARY KEY,
    field_name VARCHAR(100) NOT NULL UNIQUE,
    field_name_en VARCHAR(200),
    description TEXT,
    domain VARCHAR(50),
    subdomain VARCHAR(100),
    parent_field_id INTEGER REFERENCES fields(field_id) ON DELETE SET NULL,
    is_active BOOLEAN DEFAULT TRUE,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_fields_domain ON fields(domain);
CREATE INDEX IF NOT EXISTS idx_fields_subdomain ON fields(subdomain);
CREATE INDEX IF NOT EXISTS idx_fields_parent ON fields(parent_field_id);
CREATE INDEX IF NOT EXISTS idx_fields_is_active ON fields(is_active);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_fields_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_fields_updated_at ON fields;
CREATE TRIGGER trigger_update_fields_updated_at
    BEFORE UPDATE ON fields
    FOR EACH ROW
    EXECUTE FUNCTION update_fields_updated_at();


-- ============================================================================
-- 13. paper_fields（论文-领域关联表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_fields (
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    field_id INTEGER NOT NULL REFERENCES fields(field_id) ON DELETE CASCADE,
    confidence FLOAT DEFAULT 1.0 CHECK (confidence >= 0.0 AND confidence <= 1.0),
    source VARCHAR(50) DEFAULT 'manual',
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    PRIMARY KEY (paper_id, field_id)
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pf_paper_id ON paper_fields(paper_id);
CREATE INDEX IF NOT EXISTS idx_pf_field_id ON paper_fields(field_id);
CREATE INDEX IF NOT EXISTS idx_pf_confidence ON paper_fields(confidence);


-- ============================================================================
-- 14. pubmed_additional_info（PubMed额外信息表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS pubmed_additional_info (
    paper_id INTEGER PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
    additional_info_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 创建索引
CREATE INDEX IF NOT EXISTS idx_pai_paper_id ON pubmed_additional_info(paper_id);
CREATE INDEX IF NOT EXISTS idx_pai_additional_info_json ON pubmed_additional_info USING GIN(additional_info_json);

-- 创建更新时间触发器
CREATE OR REPLACE FUNCTION update_pubmed_additional_info_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_pubmed_additional_info_updated_at ON pubmed_additional_info;
CREATE TRIGGER trigger_update_pubmed_additional_info_updated_at
    BEFORE UPDATE ON pubmed_additional_info
    FOR EACH ROW
    EXECUTE FUNCTION update_pubmed_additional_info_updated_at();


-- ============================================================================
-- 全文检索函数（用于标题和摘要的加权搜索）
-- ============================================================================
CREATE OR REPLACE FUNCTION fts_rank(
    title TEXT,
    abstract TEXT,
    query TEXT,
    title_weight FLOAT DEFAULT 0.7,
    abstract_weight FLOAT DEFAULT 0.3
)
RETURNS FLOAT AS $$
BEGIN
    RETURN (
        title_weight * ts_rank(to_tsvector('english', COALESCE(title, '')), plainto_tsquery('english', query)) +
        abstract_weight * ts_rank(to_tsvector('english', COALESCE(abstract, '')), plainto_tsquery('english', query))
    );
END;
$$ LANGUAGE plpgsql IMMUTABLE;


-- ============================================================================
-- 完成提示
-- ============================================================================
DO $$
BEGIN
    RAISE NOTICE '✅ 数据库表创建完成！';
    RAISE NOTICE '已创建的表：';
    RAISE NOTICE '  - papers';
    RAISE NOTICE '  - paper_keywords';
    RAISE NOTICE '  - paper_texts';
    RAISE NOTICE '  - paper_author_affiliation';
    RAISE NOTICE '  - venues';
    RAISE NOTICE '  - paper_publications';
    RAISE NOTICE '  - categories';
    RAISE NOTICE '  - paper_categories';
    RAISE NOTICE '  - paper_versions';
    RAISE NOTICE '  - paper_citations';
    RAISE NOTICE '  - meta_update_logs';
    RAISE NOTICE '  - fields';
    RAISE NOTICE '  - paper_fields';
    RAISE NOTICE '  - pubmed_additional_info';
END $$;

