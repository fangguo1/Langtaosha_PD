"""查询操作"""
import sys
import json
from pathlib import Path
from typing import Dict, Any, List, Optional
from sqlalchemy import text
from sqlalchemy.engine import Engine, Connection

# 添加项目根目录到路径
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

from src.config.config_loader import init_config, get_db_engine, get_db_connection

# 注意：配置需要在调用这些函数之前通过 init_config(config_path) 初始化

# 向后兼容：提供 get_engine 和 get_connection 别名
get_engine = get_db_engine
get_connection = get_db_connection


def read_paper(
    work_id: Optional[str] = None, 
    paper_id: Optional[int] = None,
    title: Optional[str] = None
) -> Optional[Dict[str, Any]]:
    """读取论文记录（包含所有关联数据）
    
    Args:
        work_id: 工作ID
        paper_id: 论文ID（数据库内部ID）
        title: 论文标题（精确匹配）
        
    Returns:
        Dict: 论文数据字典，包含所有关联信息，如果不存在则返回None
        如果通过title查询且有多篇论文，返回第一个匹配的结果
        
    Raises:
        ValueError: 如果提供了多个参数或没有提供任何参数
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        # 先找到paper_id
        if work_id:
            cursor.execute("SELECT paper_id FROM papers WHERE work_id = %s", (work_id,))
        elif paper_id:
            cursor.execute("SELECT paper_id FROM papers WHERE paper_id = %s", (paper_id,))
        elif title:
            cursor.execute("SELECT paper_id FROM papers WHERE title = %s LIMIT 1", (title,))
        else:
            raise ValueError("必须提供work_id、paper_id或title中的一个")
        
        result = cursor.fetchone()
        if not result:
            return None
        
        paper_id = result[0]
        
        # 获取主表数据
        cursor.execute("SELECT * FROM papers WHERE paper_id = %s", (paper_id,))
        columns = [desc[0] for desc in cursor.description]
        row = cursor.fetchone()
        paper_data = dict(zip(columns, row))
        
        # 处理JSONB字段（contribution_types）
        import json
        if 'contribution_types' in paper_data and paper_data['contribution_types']:
            if isinstance(paper_data['contribution_types'], str):
                try:
                    paper_data['contribution_types'] = json.loads(paper_data['contribution_types'])
                except:
                    pass
        
        # 获取authors
        cursor.execute("SELECT authors FROM paper_author_affiliation WHERE paper_id = %s", (paper_id,))
        author_row = cursor.fetchone()
        if author_row:
            import json
            paper_data['authors'] = json.loads(author_row[0]) if isinstance(author_row[0], str) else author_row[0]
        else:
            paper_data['authors'] = []
        
        # 获取categories
        cursor.execute("""
            SELECT c.domain, c.subdomain, pc.is_primary
            FROM paper_categories pc
            JOIN categories c ON pc.cat_id = c.cat_id
            WHERE pc.paper_id = %s
            ORDER BY pc.is_primary DESC, c.subdomain
        """, (paper_id,))
        categories = cursor.fetchall()
        paper_data['categories'] = [{'domain': c[0], 'subdomain': c[1], 'is_primary': c[2]} for c in categories]
        
        # 获取publication信息
        cursor.execute("""
            SELECT v.venue_name, v.venue_type, pp.publish_time, pp.presentation_type
            FROM paper_publications pp
            JOIN venues v ON pp.venue_id = v.venue_id
            WHERE pp.paper_id = %s
        """, (paper_id,))
        pub_row = cursor.fetchone()
        if pub_row:
            paper_data['pub_info'] = {
                'venue_name': pub_row[0],
                'venue_type': pub_row[1],
                'publish_time': str(pub_row[2]) if pub_row[2] else None,
                'presentation_type': pub_row[3]
            }
        else:
            paper_data['pub_info'] = None
        
        # 获取citations
        cursor.execute("""
            SELECT cited_by_count, update_time
            FROM paper_citations
            WHERE paper_id = %s
        """, (paper_id,))
        citation_row = cursor.fetchone()
        if citation_row:
            paper_data['citations'] = {
                'cited_by_count': citation_row[0] or 0,
                'update_time': str(citation_row[1]) if citation_row[1] else None
            }
        else:
            paper_data['citations'] = {'cited_by_count': 0, 'update_time': None}
        
        # 获取versions
        cursor.execute("""
            SELECT COUNT(*) FROM paper_versions WHERE paper_id = %s
        """, (paper_id,))
        version_count = cursor.fetchone()[0]
        paper_data['version_count'] = version_count
        
        # 获取fields
        cursor.execute("""
            SELECT f.field_name, f.field_name_en, pf.confidence, pf.source
            FROM paper_fields pf
            JOIN fields f ON pf.field_id = f.field_id
            WHERE pf.paper_id = %s
        """, (paper_id,))
        fields = cursor.fetchall()
        paper_data['fields'] = [{
            'field_name': f[0],
            'field_name_en': f[1],
            'confidence': f[2],
            'source': f[3]
        } for f in fields]
        
        # 获取keywords（如果表存在）
        try:
            cursor.execute("""
                SELECT keyword_type, keyword, weight, source
                FROM paper_keywords
                WHERE paper_id = %s
                ORDER BY keyword_type, weight DESC
            """, (paper_id,))
            keywords = cursor.fetchall()
            paper_data['keywords'] = [{
                'keyword_type': k[0],
                'keyword': k[1],
                'weight': k[2],
                'source': k[3]
            } for k in keywords]
        except Exception as e:
            # 如果表不存在，返回空列表（兼容性处理）
            error_msg = str(e)
            if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                paper_data['keywords'] = []
            else:
                paper_data['keywords'] = []
        
        return paper_data
        
    finally:
        cursor.close()
        conn.close()


def search_papers(
    title: Optional[str] = None,
    author: Optional[str] = None,
    category: Optional[str] = None,
    year: Optional[int] = None,
    limit: int = 100
) -> List[Dict[str, Any]]:
    """搜索论文（包含关联数据）
    
    Args:
        title: 标题关键词
        author: 作者名称
        category: 分类
        year: 年份
        limit: 返回结果数量限制
        
    Returns:
        List[Dict]: 论文数据列表，每个论文包含关联信息
    """
    conn = get_connection()
    cursor = conn.cursor()
    
    try:
        conditions = []
        params = []
        
        if title:
            conditions.append("p.title ILIKE %s")
            params.append(f"%{title}%")
        
        if author:
            conditions.append("EXISTS (SELECT 1 FROM paper_author_affiliation paa WHERE paa.paper_id = p.paper_id AND paa.authors::text ILIKE %s)")
            params.append(f"%{author}%")
        
        if category:
            conditions.append("EXISTS (SELECT 1 FROM paper_categories pc JOIN categories c ON pc.cat_id = c.cat_id WHERE pc.paper_id = p.paper_id AND c.subdomain = %s)")
            params.append(category)
        
        if year:
            conditions.append("p.year = %s")
            params.append(year)
        
        where_clause = " AND ".join(conditions) if conditions else "1=1"
        
        # 获取主表数据
        query = f"""
            SELECT p.* FROM papers p
            WHERE {where_clause}
            ORDER BY p.created_at DESC
            LIMIT %s
        """
        params.append(limit)
        
        cursor.execute(query, params)
        rows = cursor.fetchall()
        columns = [desc[0] for desc in cursor.description]
        results = []
        
        # 为每个结果获取关联数据
        import json
        for row in rows:
            paper_data = dict(zip(columns, row))
            paper_id = paper_data['paper_id']
            
            # 处理JSONB字段
            if 'contribution_types' in paper_data and paper_data['contribution_types']:
                if isinstance(paper_data['contribution_types'], str):
                    try:
                        paper_data['contribution_types'] = json.loads(paper_data['contribution_types'])
                    except:
                        pass
            
            # 获取authors
            cursor.execute("SELECT authors FROM paper_author_affiliation WHERE paper_id = %s", (paper_id,))
            author_row = cursor.fetchone()
            if author_row:
                import json
                paper_data['authors'] = json.loads(author_row[0]) if isinstance(author_row[0], str) else author_row[0]
            else:
                paper_data['authors'] = []
            
            # 获取categories
            cursor.execute("""
                SELECT c.domain, c.subdomain, pc.is_primary
                FROM paper_categories pc
                JOIN categories c ON pc.cat_id = c.cat_id
                WHERE pc.paper_id = %s
                ORDER BY pc.is_primary DESC
            """, (paper_id,))
            categories = cursor.fetchall()
            paper_data['categories'] = [{'domain': c[0], 'subdomain': c[1], 'is_primary': c[2]} for c in categories]
            
            # 获取publication信息
            cursor.execute("""
                SELECT v.venue_name, v.venue_type, pp.publish_time, pp.presentation_type
                FROM paper_publications pp
                JOIN venues v ON pp.venue_id = v.venue_id
                WHERE pp.paper_id = %s
                LIMIT 1
            """, (paper_id,))
            pub_row = cursor.fetchone()
            if pub_row:
                paper_data['pub_info'] = {
                    'venue_name': pub_row[0],
                    'venue_type': pub_row[1],
                    'publish_time': str(pub_row[2]) if pub_row[2] else None,
                    'presentation_type': pub_row[3]
                }
            else:
                paper_data['pub_info'] = None
            
            # 获取citations
            cursor.execute("""
                SELECT cited_by_count, update_time
                FROM paper_citations
                WHERE paper_id = %s
            """, (paper_id,))
            citation_row = cursor.fetchone()
            if citation_row:
                paper_data['citations'] = {
                    'cited_by_count': citation_row[0] or 0,
                    'update_time': str(citation_row[1]) if citation_row[1] else None
                }
            else:
                paper_data['citations'] = {'cited_by_count': 0, 'update_time': None}
            
            # 获取版本数
            cursor.execute("SELECT COUNT(*) FROM paper_versions WHERE paper_id = %s", (paper_id,))
            version_count = cursor.fetchone()[0]
            paper_data['version_count'] = version_count
            
            # 获取fields
            cursor.execute("""
                SELECT f.field_name, f.field_name_en, pf.confidence, pf.source
                FROM paper_fields pf
                JOIN fields f ON pf.field_id = f.field_id
                WHERE pf.paper_id = %s
            """, (paper_id,))
            fields = cursor.fetchall()
            paper_data['fields'] = [{
                'field_name': f[0],
                'field_name_en': f[1],
                'confidence': f[2],
                'source': f[3]
            } for f in fields]
            
            # 获取keywords（如果表存在）
            try:
                cursor.execute("""
                    SELECT keyword_type, keyword, weight, source
                    FROM paper_keywords
                    WHERE paper_id = %s
                    ORDER BY keyword_type, weight DESC
                """, (paper_id,))
                keywords = cursor.fetchall()
                paper_data['keywords'] = [{
                    'keyword_type': k[0],
                    'keyword': k[1],
                    'weight': k[2],
                    'source': k[3]
                } for k in keywords]
            except Exception as e:
                # 如果表不存在，返回空列表（兼容性处理）
                error_msg = str(e)
                if "does not exist" in error_msg or "UndefinedTable" in str(type(e).__name__):
                    paper_data['keywords'] = []
                else:
                    paper_data['keywords'] = []
            
            results.append(paper_data)
        
        return results
        
    finally:
        cursor.close()
        conn.close()

