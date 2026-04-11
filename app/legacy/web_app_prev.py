#!/usr/bin/env python3
"""
简单的Web测试界面
用于测试论文数据库的查询功能
"""

import os
import sys
from pathlib import Path

# 项目根：app/legacy/web_app_prev.py -> legacy -> app -> 根
ROOT = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(ROOT / "src"))

# 初始化配置（必须在导入其他模块之前）
from config.config_loader import init_config, load_config_from_yaml, get_shard_ids_by_routing

_default_cfg = ROOT / "tests" / "db" / "config_backend_server_test.yaml"
CONFIG_PATH = Path(os.environ.get("PD_TEST_CONFIG", str(_default_cfg)))
CONFIG_ROUTING_NAME='domain:life_sci'
DEFAULT_WRITABLE_SHARD_ID=99

_config_path = CONFIG_PATH
init_config(_config_path)

from flask import Flask, render_template, request, jsonify
from src.docset_hub.indexing import PaperIndexer
from src.docset_hub.input_adapters import JSONAdapter
from src.docset_hub.crud import update_paper


routing_shards = get_shard_ids_by_routing(CONFIG_ROUTING_NAME)

print(f"routing_shards: {routing_shards}")

config_readonly_shard_ids = routing_shards.get('readonly_shard_ids', [])
config_writable_shard_ids = routing_shards.get('writable_shard_ids', [])

print(f"config_readonly_shard_ids: {config_readonly_shard_ids}")
print(f"config_writable_shard_ids: {config_writable_shard_ids}")

# 初始化索引器（传入 config_path）
indexer = PaperIndexer(
    config_path=_config_path,
    enable_vectorization=True,
    readonly_shard_ids=config_readonly_shard_ids,  # 只读 shard IDs
    writable_shard_ids=config_writable_shard_ids,  # 可写 shard ID
    vector_auto_save=True
)

app = Flask(__name__, root_path=str(ROOT), template_folder="templates")

# app.py 或 create_app 后面，确保会执行到
import time

@app.before_request
def _log_headers():
    if request.path == "/api/stats":
        print(
            f"[stats-hit] ts={time.strftime('%H:%M:%S')} "
            f"ua={request.headers.get('User-Agent','-')} "
            f"referer={request.headers.get('Referer','-')}"
        )

# 通过环境变量指定数据库（可选）
# 支持的值：'metadata_db'（统一元数据库）, 'metadata_db_pubmed', 'metadata_db_arxiv'
# 如果不设置，默认使用 'metadata_db'（统一元数据库）
_db_key = os.getenv('DB_KEY', 'metadata_db')

print(f"[web_app] 使用配置文件: {_config_path}")
print(f"[web_app] 连接数据库: {_db_key}")


@app.route('/')
def index():
    """主页"""
    return render_template('index.html')


@app.route('/api/search', methods=['GET'])
def api_search():
    """搜索API - 基于条件的元数据搜索"""
    try:
        title = request.args.get('title', '')
        author = request.args.get('author', '')
        category = request.args.get('category', '')
        year = request.args.get('year', type=int)
        limit = request.args.get('limit', 50, type=int)
        
        results = indexer.search_by_condition(
            title=title if title else None,
            author=author if author else None,
            category=category if category else None,
            year=year,
            limit=limit
        )
        
        return jsonify({
            'success': True,
            'count': len(results),
            'database': _db_key,  # 返回当前使用的数据库键名
            'results': results
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/get', methods=['GET'])
def api_get():
    """获取单篇论文API"""
    try:
        work_id = request.args.get('work_id')
        paper_id = request.args.get('paper_id', type=int)
        title = request.args.get('title')
        
        paper = indexer.read_paper(
            work_id=work_id,
            paper_id=paper_id,
            title=title
        )
        
        if paper:
            return jsonify({
                'success': True,
                'paper': paper
            })
        else:
            return jsonify({
                'success': False,
                'error': '论文不存在'
            }), 404
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/create', methods=['POST'])
def api_create():
    """创建论文API - 自动存储到默认 shard"""
    try:
        data = request.json
        result = indexer.add_doc(data, shard_id=DEFAULT_WRITABLE_SHARD_ID)
        
        if result['success']:
            return jsonify({
                'success': True,
                'paper_id': result['paper_id'],
                'work_id': result['work_id'],
                'message': result['message'],
                'vectorization': result.get('vectorization', {})
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('message', '论文创建失败'),
                'error_type': result.get('error_type'),
                'error_detail': result.get('error_detail')
            }), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/update', methods=['POST'])
def api_update():
    """更新论文API"""
    try:
        data = request.json
        identifier = data.get('identifier')
        work_id = data.get('work_id')
        title = data.get('title')
        updates = data.get('updates', {})
        
        update_paper(
            identifier=identifier,
            work_id=work_id,
            title=title,
            updates=updates
        )
        
        return jsonify({
            'success': True,
            'message': '论文更新成功'
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/delete', methods=['POST'])
def api_delete():
    """删除论文API"""
    try:
        data = request.json
        identifier = data.get('identifier')
        by_title = data.get('by_title', False)
        
        if not identifier:
            return jsonify({
                'success': False,
                'error': 'identifier不能为空'
            }), 400
        
        result = indexer.delete_doc(identifier=identifier, by_title=by_title)
        
        if result['success']:
            return jsonify({
                'success': True,
                'message': result['message'],
                'work_id': result.get('work_id'),
                'paper_id': result.get('paper_id'),
                'vector_deletion': result.get('vector_deletion', {})
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('message', '论文删除失败')
            }), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/vector_search', methods=['GET'])
def api_vector_search():
    """向量相似度搜索API
    
    基于语义相似度搜索论文，支持自然语言查询
    
    Query参数:
        query: 搜索查询文本（必需）
        top_k: 返回结果数量（默认10）
        min_score: 最小相似度分数（默认0.0）
    """
    try:
        query = request.args.get('query', '').strip()
        top_k = request.args.get('top_k', 10, type=int)
        min_score = request.args.get('min_score', 0.0, type=float)
        
        if not query:
            return jsonify({
                'success': False,
                'error': '查询文本不能为空'
            }), 400
        
        # 检查向量数据库是否可用
        if not indexer.enable_vectorization or not indexer.vector_db:
            return jsonify({
                'success': False,
                'error': '向量数据库未启用或不可用'
            }), 503
        
        # 使用 PaperIndexer 的 search 方法
        search_results = indexer.search(
            query=query,
            filters=None,
            limit=top_k
        )
        
        # 过滤低相似度结果并重命名字段
        papers = []
        for paper in search_results:
            similarity = paper.get('similarity', 0.0)
            if similarity < min_score:
                continue
            
            # 将 similarity 重命名为 similarity_score 以保持兼容性
            paper['similarity_score'] = round(similarity, 4)
            if 'similarity' in paper:
                del paper['similarity']
            
            papers.append(paper)
        
        return jsonify({
            'success': True,
            'query': query,
            'count': len(papers),
            'results': papers
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/hybrid_search', methods=['GET'])
def api_hybrid_search():
    """混合搜索API
    
    结合关键词搜索和向量相似度搜索
    
    Query参数:
        query: 搜索查询文本（用于向量搜索）
        title: 标题关键词（用于关键词搜索）
        author: 作者（用于关键词搜索）
        category: 分类（用于关键词搜索）
        year: 年份（用于关键词搜索）
        top_k: 向量搜索结果数量（默认10）
        keyword_limit: 关键词搜索结果数量（默认50）
        vector_weight: 向量搜索权重（0.0-1.0，默认0.5）
    """
    try:
        query = request.args.get('query', '').strip()
        title = request.args.get('title', '').strip()
        author = request.args.get('author', '').strip()
        category = request.args.get('category', '').strip()
        year = request.args.get('year', type=int)
        top_k = request.args.get('top_k', 10, type=int)
        keyword_limit = request.args.get('keyword_limit', 50, type=int)
        vector_weight = request.args.get('vector_weight', 0.5, type=float)
        
        # 至少需要一个搜索条件
        if not query and not title and not author and not category and not year:
            return jsonify({
                'success': False,
                'error': '至少需要提供一个搜索条件'
            }), 400
        
        results = []
        vector_results = {}
        keyword_results = {}
        
        # 向量搜索（如果提供了查询文本）
        if query and indexer.enable_vectorization and indexer.vector_db:
            try:
                vector_search_results = indexer.vector_db.search(
                    query=query,
                    top_k=top_k
                )
                for entry, similarity_score in vector_search_results:
                    vector_results[entry.work_id] = similarity_score
            except Exception as e:
                print(f"向量搜索失败: {e}")
        
        # 关键词搜索
        keyword_search_results = indexer.search_by_condition(
            title=title if title else None,
            author=author if author else None,
            category=category if category else None,
            year=year,
            limit=keyword_limit
        )
        
        # 为关键词搜索结果分配分数
        for paper in keyword_search_results:
            work_id = paper.get('work_id')
            if work_id:
                keyword_results[work_id] = 1.0  # 关键词匹配给固定分数
        
        # 合并结果
        all_work_ids = set(vector_results.keys()) | set(keyword_results.keys())
        
        for work_id in all_work_ids:
            paper = indexer.read_paper(work_id=work_id)
            if paper:
                # 计算综合分数
                vector_score = vector_results.get(work_id, 0.0)
                keyword_score = keyword_results.get(work_id, 0.0)
                combined_score = vector_weight * vector_score + (1 - vector_weight) * keyword_score
                
                paper['similarity_score'] = round(combined_score, 4)
                paper['vector_score'] = round(vector_score, 4) if vector_score > 0 else None
                paper['keyword_score'] = round(keyword_score, 4) if keyword_score > 0 else None
                results.append(paper)
        
        # 按综合分数排序
        results.sort(key=lambda x: x.get('similarity_score', 0), reverse=True)
        
        return jsonify({
            'success': True,
            'query': query,
            'count': len(results),
            'results': results
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/recommend', methods=['GET'])
def api_recommend():
    """相关论文推荐API
    
    基于当前论文推荐相似论文
    
    Query参数:
        work_id: 论文work_id（必需）
        top_k: 推荐数量（默认5）
    """
    try:
        work_id = request.args.get('work_id', '').strip()
        top_k = request.args.get('top_k', 5, type=int)
        
        if not work_id:
            return jsonify({
                'success': False,
                'error': 'work_id不能为空'
            }), 400
        
        # 检查向量数据库是否可用
        if not indexer.enable_vectorization or not indexer.vector_db:
            return jsonify({
                'success': False,
                'error': '向量数据库未启用或不可用'
            }), 503
        
        # 获取当前论文的摘要作为查询
        current_paper = indexer.read_paper(work_id=work_id)
        if not current_paper:
            return jsonify({
                'success': False,
                'error': '论文不存在'
            }), 404
        
        # 使用摘要作为查询（如果没有摘要，使用标题）
        query_text = current_paper.get('abstract', '') or current_paper.get('title', '')
        if not query_text:
            return jsonify({
                'success': False,
                'error': '论文没有摘要或标题，无法推荐'
            }), 400
        
        # 执行向量搜索（top_k+1 因为会包含自己）
        search_results = indexer.vector_db.search(
            query=query_text,
            top_k=top_k + 1
        )
        
        # 过滤掉当前论文，并获取完整信息
        recommendations = []
        for entry, similarity_score in search_results:
            if entry.work_id == work_id:
                continue  # 跳过自己
            
            paper = indexer.read_paper(work_id=entry.work_id)
            if paper:
                paper['similarity_score'] = round(similarity_score, 4)
                recommendations.append(paper)
            
            if len(recommendations) >= top_k:
                break
        
        return jsonify({
            'success': True,
            'work_id': work_id,
            'count': len(recommendations),
            'recommendations': recommendations
        })
        
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/stats', methods=['GET'])
def api_stats():
    """数据库统计API（优化版，使用 SQLAlchemy 和连接池）"""
    try:
        from sqlalchemy import text
        
        print(f"[api/stats] 开始获取统计信息，使用数据库: {_db_key}")
        
        # 使用已初始化的 indexer.metadata_db.engine，避免配置未初始化的问题
        engine = indexer.metadata_db.engine
        print(f"[api/stats] 数据库引擎获取成功")
        
        # 使用单个查询获取统计值，包括数据源统计
        with engine.connect() as conn:
            print(f"[api/stats] 数据库连接成功，开始查询")
            # 获取总数和work_id数量
            result = conn.execute(text("""
                SELECT 
                    COUNT(*) as total_papers,
                    COUNT(DISTINCT work_id) FILTER (WHERE work_id IS NOT NULL) as total_work_ids
                FROM papers
            """))
            row = result.fetchone()
            total_papers = row[0] if row[0] is not None else 0
            total_work_ids = row[1] if row[1] is not None else 0
            print(f"[api/stats] 查询结果: total_papers={total_papers}, total_work_ids={total_work_ids}")
        
        # 添加向量数据库统计
        vector_db_enabled = False
        if indexer.enable_vectorization and indexer.vector_db:
            vector_db_enabled = True
            # 注意：新的 VectorDB (多 shard 版本) 没有 get_all_work_ids() 方法
            # 向量化论文数量统计暂时不提供
        
        result_data = {
            'success': True,
            'database': _db_key,  # 返回当前使用的数据库键名
            'stats': {
                'total_papers': int(total_papers),
                'total_work_ids': int(total_work_ids),
                'vector_db_enabled': vector_db_enabled
            }
        }
        print(f"[api/stats] 返回数据: {result_data}")
        return jsonify(result_data)
    except Exception as e:
        import traceback
        error_trace = traceback.format_exc()
        print(f"[api/stats] 发生错误: {str(e)}")
        print(f"[api/stats] 错误堆栈:\n{error_trace}")
        return jsonify({
            'success': False,
            'error': str(e),
            'error_type': type(e).__name__
        }), 500


@app.route('/api/add_paper', methods=['POST'])
def api_add_paper():
    """添加论文API - 存储到默认 shard
    
    接收 DocSet 格式的 JSON 数据，自动存储到元数据库和向量数据库（使用 DEFAULT_WRITABLE_SHARD_ID）
    
    Body参数:
        JSON格式的论文数据（DocSet格式）
    """
    try:
        data = request.json
        if not data:
            return jsonify({
                'success': False,
                'error': '请求体不能为空'
            }), 400
        
        result = indexer.add_doc(data, shard_id=DEFAULT_WRITABLE_SHARD_ID)
        
        if result['success']:
            return jsonify({
                'success': True,
                'paper_id': result['paper_id'],
                'work_id': result['work_id'],
                'message': result['message'],
                'vectorization': result.get('vectorization', {})
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('message', '论文添加失败'),
                'error_type': result.get('error_type'),
                'error_detail': result.get('error_detail')
            }), 400
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/add_paper')
def add_paper_page():
    """添加论文页面"""
    return render_template('add_paper.html')


@app.route('/api/batch_import_pubmed', methods=['POST'])
def api_batch_import_pubmed():
    """批量导入 PubMed JSON 文件 API（后台调用）
    
    Body参数:
        folder_path: 文件夹路径（必需）
        date: 更新日期（可选，格式：YYYY-MM-DD，默认今天）
        skip_existing: 是否跳过已存在文件（默认True）
        limit: 导入数量限制（可选）
    """
    try:
        data = request.json
        if not data:
            return jsonify({
                'success': False,
                'error': '请求体不能为空'
            }), 400
        
        folder_path = data.get('folder_path')
        if not folder_path:
            return jsonify({
                'success': False,
                'error': 'folder_path不能为空'
            }), 400
        
        date = data.get('date')
        skip_existing = data.get('skip_existing', True)
        limit = data.get('limit')
        
        result = indexer.batch_import_from_folder(
            folder_path=folder_path,
            date=date,
            skip_existing=skip_existing,
            limit=limit
        )
        
        if result['success']:
            return jsonify({
                'success': True,
                'total_files': result['total_files'],
                'success_count': result['success_count'],
                'fail_count': result['fail_count'],
                'skip_count': result['skip_count'],
                'duration_seconds': result.get('duration_seconds', 0),
                'message': result['message'],
                'errors': result.get('errors', [])
            })
        else:
            return jsonify({
                'success': False,
                'error': result.get('message', '批量导入失败'),
                'errors': result.get('errors', [])
            }), 400
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/daily_updates', methods=['GET'])
def api_daily_updates():
    """获取每日更新统计列表 API
    
    Query参数:
        start_date: 开始日期（可选，格式：YYYY-MM-DD）
        end_date: 结束日期（可选，格式：YYYY-MM-DD）
        limit: 返回数量限制（默认30）
    """
    try:
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        limit = request.args.get('limit', 30, type=int)
        
        updates = indexer.metadata_db.get_daily_updated_papers(
            start_date=start_date,
            end_date=end_date,
            limit=limit
        )
        
        return jsonify({
            'success': True,
            'count': len(updates),
            'updates': updates
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/api/daily_update_detail', methods=['GET'])
def api_daily_update_detail():
    """获取指定日期的更新详情 API
    
    Query参数:
        date: 日期（必需，格式：YYYY-MM-DD）
    """
    try:
        date = request.args.get('date')
        if not date:
            return jsonify({
                'success': False,
                'error': 'date参数不能为空'
            }), 400
        
        detail = indexer.metadata_db.get_daily_updated_papers_detail(date=date)
        
        return jsonify({
            'success': True,
            'detail': detail
        })
            
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@app.route('/daily_updates')
def daily_updates_page():
    """每日更新展示页面"""
    return render_template('daily_updates.html')


if __name__ == '__main__':
    import os
    # 支持通过环境变量指定端口，默认使用5003端口（统一元数据库）
    port = int(os.getenv('PORT', 5004))
    
    print("=" * 60)
    print("启动Web测试界面")
    print("=" * 60)
    print(f"访问地址: http://localhost:{port}")
    print(f"当前数据库: {_db_key} (统一元数据库 meta_database)")
    print("按 Ctrl+C 停止服务器")
    print("=" * 60)
    app.run(host='0.0.0.0', port=port, debug=True)


