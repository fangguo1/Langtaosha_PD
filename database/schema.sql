-- 论文数据库系统 - 建表脚本（0413 架构简化版）
-- 创建时间: 2025
-- 说明: 统一作品 + 多来源记录 + 原始元数据沉淀 + 来源文件追溯
-- 支持 Langtaosha、bioRxiv、arXiv、PubMed 并存
--
-- 简化说明（0413）：
--   暂时取消独立建模的表（信息统一进入 paper_source_metadata.normalized_json）：
--     paper_texts / venues / paper_publications / paper_versions
--     paper_citations / fields / paper_fields

-- ============================================================================
-- 1. papers（统一作品主表）
-- ============================================================================
-- 仅存 canonical 统一视图字段，来源特异字段进入 paper_sources / paper_source_metadata
CREATE TABLE IF NOT EXISTS papers (
    paper_id SERIAL PRIMARY KEY,
    work_id VARCHAR(200),  -- 全局唯一标识符（UUID v7 格式，前缀 W）
    canonical_title TEXT,
    canonical_abstract TEXT,
    canonical_language VARCHAR(32),
    canonical_publisher VARCHAR(200),
    submitted_at TIMESTAMP,
    online_at TIMESTAMP,
    published_at TIMESTAMP,
    merge_status VARCHAR(32) DEFAULT 'single_source',  -- single_source | merged
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- 兼容旧版本库：如果 papers 已存在，补齐缺失列。
ALTER TABLE papers ADD COLUMN IF NOT EXISTS work_id VARCHAR(200);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS canonical_title TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS canonical_abstract TEXT;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS canonical_language VARCHAR(32);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS canonical_publisher VARCHAR(200);
ALTER TABLE papers ADD COLUMN IF NOT EXISTS submitted_at TIMESTAMP;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS online_at TIMESTAMP;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS published_at TIMESTAMP;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS merge_status VARCHAR(32) DEFAULT 'single_source';
ALTER TABLE papers ADD COLUMN IF NOT EXISTS created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE papers ADD COLUMN IF NOT EXISTS updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP;

-- 兼容旧版本库：如果 paper_sources 已存在，补齐缺失列（包括 version 字段）
-- 注意：在首次建库时，paper_sources 还未创建，因此需要先判断表是否存在。
DO $$
BEGIN
    IF to_regclass('public.paper_sources') IS NOT NULL THEN
        ALTER TABLE paper_sources ADD COLUMN IF NOT EXISTS version VARCHAR(50);
    END IF;
END $$;

COMMENT ON TABLE papers IS '统一作品主表：每行代表一篇去重后的学术作品，仅存 canonical 统一视图字段，来源特异信息由 paper_sources 和 paper_source_metadata 承载。';

COMMENT ON COLUMN papers.work_id IS '全局唯一标识符（UUID v7 格式，前缀 W），用于：
1. Vector DB 向量与元数据的关联
2. 跨系统数据交换和迁移
3. API 对外接口（避免暴露自增 ID）
4. 分布式系统中的唯一识别

示例：W019b73d6-1634-77d3-9574-b6014f85b118';

-- 自动更新 updated_at
CREATE OR REPLACE FUNCTION update_papers_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_papers_updated_at ON papers;
CREATE TRIGGER trigger_update_papers_updated_at
    BEFORE UPDATE ON papers
    FOR EACH ROW
    EXECUTE FUNCTION update_papers_updated_at();


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

COMMENT ON TABLE paper_keywords IS '论文关键词表：存储论文的结构化关键词，支持 task/method/dataset 等多种类型，并记录来源与置信度权重。';

CREATE INDEX IF NOT EXISTS idx_pk_paper_id ON paper_keywords(paper_id);
CREATE INDEX IF NOT EXISTS idx_pk_keyword_type ON paper_keywords(keyword_type);
CREATE INDEX IF NOT EXISTS idx_pk_keyword ON paper_keywords(keyword);
CREATE INDEX IF NOT EXISTS idx_pk_paper_id_keyword_type ON paper_keywords(paper_id, keyword_type);


-- ============================================================================
-- 3. paper_author_affiliation（作者—论文—机构表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_author_affiliation (
    paper_id INTEGER PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
    authors JSONB NOT NULL  -- 作者信息 JSON 数组，包含姓名、顺序、机构等
);

COMMENT ON TABLE paper_author_affiliation IS '作者与机构关联表：以 JSONB 数组形式存储论文全部作者的姓名、排序及所属机构，与 papers 为 1:1 关系。';

CREATE INDEX IF NOT EXISTS idx_paa_paper_id ON paper_author_affiliation(paper_id);
CREATE INDEX IF NOT EXISTS idx_paa_authors ON paper_author_affiliation USING GIN(authors);


-- ============================================================================
-- 4. categories（大领域/子领域）
-- ============================================================================
CREATE TABLE IF NOT EXISTS categories (
    cat_id SERIAL PRIMARY KEY,
    domain VARCHAR(50) NOT NULL,
    subdomain VARCHAR(100) NOT NULL,
    description VARCHAR(500),
    UNIQUE (domain, subdomain)
);

COMMENT ON TABLE categories IS '学科领域字典表：存储大领域（如 cs）与子领域（如 cs.AI）的层级分类，供论文分类关联使用。';

CREATE INDEX IF NOT EXISTS idx_categories_domain ON categories(domain);
CREATE INDEX IF NOT EXISTS idx_categories_subdomain ON categories(subdomain);


-- ============================================================================
-- 5. paper_categories（论文分类 M:N 关联表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS paper_categories (
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    cat_id INTEGER NOT NULL REFERENCES categories(cat_id) ON DELETE CASCADE,
    is_primary BOOLEAN DEFAULT FALSE,
    PRIMARY KEY (paper_id, cat_id)
);

COMMENT ON TABLE paper_categories IS '论文-领域多对多关联表：建立论文与学科领域的关联，并标记主分类（is_primary）。';

CREATE INDEX IF NOT EXISTS idx_pc_paper_id ON paper_categories(paper_id);
CREATE INDEX IF NOT EXISTS idx_pc_cat_id ON paper_categories(cat_id);
CREATE INDEX IF NOT EXISTS idx_pc_is_primary ON paper_categories(is_primary);


-- ============================================================================
-- 6. meta_update_logs（元数据更新日志）
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

COMMENT ON TABLE meta_update_logs IS '元数据变更审计日志：记录论文字段的每次变更（字段名、旧值、新值、来源），支持数据溯源与回滚分析。';

CREATE INDEX IF NOT EXISTS idx_mul_paper_id ON meta_update_logs(paper_id);
CREATE INDEX IF NOT EXISTS idx_mul_update_time ON meta_update_logs(update_time);
CREATE INDEX IF NOT EXISTS idx_mul_field ON meta_update_logs(field);


-- ============================================================================
-- 7. pubmed_additional_info（PubMed额外信息表）
-- ============================================================================
CREATE TABLE IF NOT EXISTS pubmed_additional_info (
    paper_id INTEGER PRIMARY KEY REFERENCES papers(paper_id) ON DELETE CASCADE,
    additional_info_json JSONB NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE pubmed_additional_info IS 'PubMed 扩展元数据表：以 JSONB 形式存储 PubMed 特有字段（MeSH 主题词、期刊详情、关键词列表等），与 papers 为 1:1 关系。';

CREATE INDEX IF NOT EXISTS idx_pai_paper_id ON pubmed_additional_info(paper_id);
CREATE INDEX IF NOT EXISTS idx_pai_additional_info_json ON pubmed_additional_info USING GIN(additional_info_json);

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
-- 8. paper_sources（来源记录表）
-- ============================================================================
-- 每条记录表示该作品在某来源（langtaosha/biorxiv/arxiv/pubmed）的一次可追踪记录
CREATE TABLE IF NOT EXISTS paper_sources (
    paper_source_id BIGSERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    source_name VARCHAR(64) NOT NULL,          -- langtaosha, biorxiv, arxiv, pubmed
    platform VARCHAR(64),
    source_record_id VARCHAR(200),             -- 来源内唯一 ID
    source_url VARCHAR(500),
    abstract_url VARCHAR(500),
    pdf_url VARCHAR(500),
    title TEXT,
    abstract TEXT,
    publisher VARCHAR(200),
    language VARCHAR(32),
    doi VARCHAR(200),
    arxiv_id VARCHAR(50),
    pubmed_id VARCHAR(100),
    semantic_scholar_id VARCHAR(100),
    submitted_at TIMESTAMP,
    online_at TIMESTAMP,
    published_at TIMESTAMP,
    updated_at_source TIMESTAMP,               -- 来源侧最后更新时间
    version VARCHAR(50),                       -- 版本号（用于同 source 版本比较）
    is_preprint BOOLEAN DEFAULT FALSE,
    is_published BOOLEAN DEFAULT FALSE,
    is_primary_source BOOLEAN DEFAULT FALSE,   -- 是否为 canonical 优先来源
    sync_status VARCHAR(32) DEFAULT 'active',  -- active | deprecated | error
    last_synced_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE paper_sources IS '来源记录表：每行代表作品在某一数据来源（arXiv/bioRxiv/PubMed/Langtaosha）的可追踪记录，允许同一作品对应多个来源，存储来源侧原始标识与时间信息。';
COMMENT ON COLUMN paper_sources.pubmed_id IS '旧迁移中位于 papers.pubmed_id 的来源级标识，现迁移到 paper_sources 以支持多来源并存。';
COMMENT ON COLUMN paper_sources.semantic_scholar_id IS '旧迁移中位于 papers.semantic_scholar_id 的来源级标识，现迁移到 paper_sources 以支持多来源并存。';
COMMENT ON COLUMN paper_sources.version IS '来源记录版本号；兼容旧迁移新增的 version 字段，并用于同一来源内版本比较。';

CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_record_unique
    ON paper_sources(source_name, source_record_id)
    WHERE source_record_id IS NOT NULL;

-- 同 source 下的唯一性约束（DOI）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_doi
    ON paper_sources(source_name, doi)
    WHERE doi IS NOT NULL;

-- 同 source 下的唯一性约束（arXiv ID）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_arxiv
    ON paper_sources(source_name, arxiv_id)
    WHERE arxiv_id IS NOT NULL;

-- 同 source 下的唯一性约束（PubMed ID）
CREATE UNIQUE INDEX IF NOT EXISTS idx_ps_source_pubmed
    ON paper_sources(source_name, pubmed_id)
    WHERE pubmed_id IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_ps_paper_id ON paper_sources(paper_id);
CREATE INDEX IF NOT EXISTS idx_ps_source_name ON paper_sources(source_name);
CREATE INDEX IF NOT EXISTS idx_ps_doi ON paper_sources(doi);
CREATE INDEX IF NOT EXISTS idx_ps_arxiv_id ON paper_sources(arxiv_id);
CREATE INDEX IF NOT EXISTS idx_ps_pubmed_id ON paper_sources(pubmed_id);
CREATE INDEX IF NOT EXISTS idx_ps_updated_at_source ON paper_sources(updated_at_source);
CREATE INDEX IF NOT EXISTS idx_ps_last_synced_at ON paper_sources(last_synced_at);

CREATE OR REPLACE FUNCTION update_paper_sources_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_paper_sources_updated_at ON paper_sources;
CREATE TRIGGER trigger_update_paper_sources_updated_at
    BEFORE UPDATE ON paper_sources
    FOR EACH ROW
    EXECUTE FUNCTION update_paper_sources_updated_at();


-- ============================================================================
-- papers.canonical_source_id（循环外键，建于 paper_sources 之后）
-- ============================================================================
ALTER TABLE papers
    ADD COLUMN IF NOT EXISTS canonical_source_id BIGINT
        REFERENCES paper_sources(paper_source_id) ON DELETE SET NULL;

CREATE INDEX IF NOT EXISTS idx_papers_canonical_source_id
    ON papers(canonical_source_id)
    WHERE canonical_source_id IS NOT NULL;

-- ============================================================================
-- papers.work_id（全局唯一标识符）
-- ============================================================================
CREATE UNIQUE INDEX IF NOT EXISTS idx_papers_work_id
    ON papers(work_id)
    WHERE work_id IS NOT NULL;

COMMENT ON INDEX idx_papers_work_id IS 'work_id 唯一索引，确保全局唯一性';


-- ============================================================================
-- 9. paper_source_metadata（来源元数据沉淀表）
-- ============================================================================
-- 保存每条来源记录的原始 payload 与归一化结果
-- normalized_json 结构：
-- {
--   "common_normalized": {
--     "title": "string", "abstract": "string", "language": "en",
--     "publisher": "string", "authors": [], "categories": [],
--     "keywords": [],
--     "contribution_types": [],
--     "paper_type": "survey",
--     "primary_field": "biology",
--     "target_application_domain": "genomics",
--     "is_llm_era": false,
--     "short_reasoning": "string",
--     "pub_info": {"venue_name": "string", "venue_type": "journal",
--                  "publish_time": "2025-01-01", "presentation_type": null},
--     "versions": [],
--     "citations": {"cited_by_count": 0, "update_time": "2025-01-01"},
--     "fields": []
--   },
--   "source_specific": {}
-- }
CREATE TABLE IF NOT EXISTS paper_source_metadata (
    paper_source_id BIGINT PRIMARY KEY
        REFERENCES paper_sources(paper_source_id) ON DELETE CASCADE,
    raw_metadata_json JSONB NOT NULL,          -- 完整原始记录，确保可审计与可重放
    normalized_json JSONB,                     -- {"common_normalized": {...}, "source_specific": {...}}
    parser_version VARCHAR(64),
    source_schema_version VARCHAR(64),         -- 来源 payload 版本，可空
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE paper_source_metadata IS '来源元数据沉淀表：以 JSONB 形式保存每条来源记录的完整原始 payload（raw_metadata_json）及归一化结果（normalized_json）。normalized_json.common_normalized 存放跨来源通用字段（含 keywords/contribution_types/paper_type/primary_field/target_application_domain/is_llm_era/short_reasoning/pub_info/versions/citations/fields），来源特异字段落在 normalized_json.source_specific，确保可审计与可重放。';

CREATE INDEX IF NOT EXISTS idx_psm_raw ON paper_source_metadata USING GIN(raw_metadata_json);
CREATE INDEX IF NOT EXISTS idx_psm_normalized ON paper_source_metadata USING GIN(normalized_json);

CREATE OR REPLACE FUNCTION update_paper_source_metadata_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_paper_source_metadata_updated_at ON paper_source_metadata;
CREATE TRIGGER trigger_update_paper_source_metadata_updated_at
    BEFORE UPDATE ON paper_source_metadata
    FOR EACH ROW
    EXECUTE FUNCTION update_paper_source_metadata_updated_at();


-- ============================================================================
-- 10. paper_references（来源参考文献表）
-- ============================================================================
-- 保存来源提供的参考文献原文，区别于被引统计
CREATE TABLE IF NOT EXISTS paper_references (
    reference_id BIGSERIAL PRIMARY KEY,
    paper_id INTEGER NOT NULL REFERENCES papers(paper_id) ON DELETE CASCADE,
    paper_source_id BIGINT REFERENCES paper_sources(paper_source_id) ON DELETE CASCADE,
    reference_order INTEGER,                   -- 参考文献在原文中的顺序
    reference_text TEXT NOT NULL,              -- 参考文献原文字符串
    reference_raw_json JSONB,                  -- 来源提供的结构化参考文献数据
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE paper_references IS '来源参考文献表：保存来源侧提供的参考文献原始文本与结构化数据，按顺序关联到具体论文及来源记录，区别于 paper_citations 的被引统计。';

CREATE INDEX IF NOT EXISTS idx_pref_paper_id ON paper_references(paper_id);
CREATE INDEX IF NOT EXISTS idx_pref_paper_source_id ON paper_references(paper_source_id);


-- ============================================================================
-- 11. paper_source_artifacts（来源文件追溯表）
-- ============================================================================
-- 记录来源文件位置与校验信息（本地文件、对象存储等）
CREATE TABLE IF NOT EXISTS paper_source_artifacts (
    artifact_id BIGSERIAL PRIMARY KEY,
    paper_source_id BIGINT NOT NULL
        REFERENCES paper_sources(paper_source_id) ON DELETE CASCADE,
    artifact_type VARCHAR(64) NOT NULL,        -- raw_jsonl, manifest, raw_page, pdf
    storage_backend VARCHAR(32) NOT NULL,      -- local, s3, oss
    storage_uri VARCHAR(1000) NOT NULL,        -- e.g. local://bioarxiv_history/records/2024/2024-12.jsonl
    line_no INTEGER,                           -- 文件内行号，可空
    byte_offset BIGINT,                        -- 文件内字节偏移，可空
    sha256 VARCHAR(64),                        -- 文件内容校验，可空
    captured_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE paper_source_artifacts IS '来源文件追溯表：记录来源数据文件的存储位置（本地/S3/OSS）、类型、行号、字节偏移及 SHA256 校验值，支持原始文件可追溯与完整性验证。';

CREATE INDEX IF NOT EXISTS idx_psa_paper_source_id ON paper_source_artifacts(paper_source_id);
CREATE INDEX IF NOT EXISTS idx_psa_storage_backend ON paper_source_artifacts(storage_backend);
CREATE INDEX IF NOT EXISTS idx_psa_artifact_type ON paper_source_artifacts(artifact_type);
CREATE INDEX IF NOT EXISTS idx_psa_sha256 ON paper_source_artifacts(sha256) WHERE sha256 IS NOT NULL;


-- ============================================================================
-- 12. embedding_status（向量化状态表）
-- ============================================================================
-- 记录每篇 paper 的当前向量化状态（简化三态）
CREATE TABLE IF NOT EXISTS embedding_status (
    paper_id INTEGER PRIMARY KEY
        REFERENCES papers(paper_id) ON DELETE CASCADE,
    work_id VARCHAR(200) NOT NULL,
    canonical_source_id BIGINT,
    source_name VARCHAR(64),
    text_type VARCHAR(32) NOT NULL DEFAULT 'abstract',
    status VARCHAR(16) NOT NULL
        CHECK (status IN ('pending', 'succeeded', 'failed')),
    attempt_count INTEGER NOT NULL DEFAULT 0,
    last_error_message TEXT,
    last_attempt_at TIMESTAMP,
    last_success_at TIMESTAMP,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

COMMENT ON TABLE embedding_status IS '向量化状态表：记录每篇 paper 当前向量化状态（pending/succeeded/failed）及最近尝试结果。';

CREATE INDEX IF NOT EXISTS idx_es_status ON embedding_status(status);
CREATE INDEX IF NOT EXISTS idx_es_source_status ON embedding_status(source_name, status);
CREATE INDEX IF NOT EXISTS idx_es_work_id ON embedding_status(work_id);

CREATE OR REPLACE FUNCTION update_embedding_status_updated_at()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = CURRENT_TIMESTAMP;
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS trigger_update_embedding_status_updated_at ON embedding_status;
CREATE TRIGGER trigger_update_embedding_status_updated_at
    BEFORE UPDATE ON embedding_status
    FOR EACH ROW
    EXECUTE FUNCTION update_embedding_status_updated_at();


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
    RAISE NOTICE '已创建的表（0413 简化版，共 12 张）：';
    RAISE NOTICE '  1.  papers（统一作品主表）';
    RAISE NOTICE '  2.  paper_keywords（论文关键词表）';
    RAISE NOTICE '  3.  paper_author_affiliation（作者机构表）';
    RAISE NOTICE '  4.  categories（学科领域字典）';
    RAISE NOTICE '  5.  paper_categories（论文-领域关联）';
    RAISE NOTICE '  6.  meta_update_logs（元数据变更日志）';
    RAISE NOTICE '  7.  pubmed_additional_info（PubMed扩展元数据）';
    RAISE NOTICE '  8.  paper_sources（来源记录表）';
    RAISE NOTICE '  9.  paper_source_metadata（来源元数据沉淀）';
    RAISE NOTICE '  10. paper_references（来源参考文献）';
    RAISE NOTICE '  11. paper_source_artifacts（来源文件追溯）';
    RAISE NOTICE '  12. embedding_status（向量化状态）';
    RAISE NOTICE '';
    RAISE NOTICE '暂时取消建模的表（信息进入 normalized_json）：';
    RAISE NOTICE '  paper_texts / venues / paper_publications / paper_versions';
    RAISE NOTICE '  paper_citations / fields / paper_fields';
END $$;
