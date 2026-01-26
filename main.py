import os
import re
from typing import List, Optional

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="MiniIzi API")

# Views usadas no MVP
VIEW_7D = "v_preco_stats_7d"
VIEW_90D = "v_preco_stats_90d"


def get_conn():
    """
    Conexão com MySQL (lazy import):
    - Não importa mysql.connector no topo (evita quebrar deploy).
    - Se o driver não estiver instalado, devolve erro claro.
    """
    try:
        import mysql.connector  # lazy import
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"MySQL driver ausente. Instale 'mysql-connector-python' no requirements.txt. Detalhe: {e}",
        )

    try:
        return mysql.connector.connect(
            host=os.getenv("DB_HOST"),
            port=int(os.getenv("DB_PORT", "3306")),
            user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASS"),
            database=os.getenv("DB_NAME"),
            charset="utf8mb4",
            collation="utf8mb4_general_ci",
        )
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro conectando no MySQL: {e}")


@app.get("/health")
def health():
    # Saúde da API: NÃO depende de banco
    return {"ok": True, "service": "miniizi-api"}


@app.get("/ip")
def ip():
    """
    Retorna o IP de saída (outbound) do serviço no Render.
    Usado apenas para o admin liberar o acesso no MySQL.
    """
    try:
        import requests

        ip_txt = requests.get("https://api.ipify.org", timeout=8).text.strip()
        return {"ip": ip_txt}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro obtendo IP externo: {e}")


@app.get("/health/db")
def health_db():
    # Saúde do banco: testa conexão e SELECT 1
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    conn.close()
    return {"ok": True, "db": "ok"}


@app.get("/debug/views")
def list_views():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute(
        """
        SELECT table_name
        FROM information_schema.views
        WHERE table_schema = %s
        ORDER BY table_name
        """,
        (os.getenv("DB_NAME"),),
    )
    rows = [r[0] for r in cur.fetchall()]
    cur.close()
    conn.close()
    return {"views": rows}


def escolhe_view_por_ncm(pr_ncm: Optional[str]) -> str:
    """
    Regra canônica (MVP):
    - Se NCM (8 dígitos) <= 08000000 => janela 7d
    - Caso contrário => janela 90d
    - Se NCM vazio/ inválido => 90d (fallback seguro)
    """
    if not pr_ncm:
        return VIEW_90D

    digits = re.sub(r"\D", "", str(pr_ncm))

    if len(digits) < 8:
        digits = digits.zfill(8)

    if len(digits) != 8:
        return VIEW_90D

    n = int(digits)
    return VIEW_7D if n <= 8_000_000 else VIEW_90D


class ItemXML(BaseModel):
    pr_nomeProduto: str
    pr_unidade: str
    pr_ncm: Optional[str] = None


class AnalisaRequest(BaseModel):
    itens: List[ItemXML]
    janela_padrao_dias: int = 30
    limite_fornecedores: int = 5
    n_min: int = 3

@app.post("/analisa", tags=["miniizi"])
def analisa(req: AnalisaRequest):
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    resultados = []

    for item in req.itens:
        nome_prod = (item.pr_nomeProduto or "").strip()
        unidade = (item.pr_unidade or "").strip()

        view_stats = escolhe_view_por_ncm(item.pr_ncm)

        # 1) mapear nome do XML -> nome normativo
        cur.execute(
            """
            SELECT pr_nomeNorm, ocorrencias
            FROM v_mapa_nomeproduto_norm
            WHERE pr_nomeProduto = %s AND pr_unidade = %s
            ORDER BY ocorrencias DESC
            LIMIT 1
            """,
            (nome_prod, unidade),
        )
        row_map = cur.fetchone()

        if not row_map:
            resultados.append(
                {
                    "pr_nomeProduto": nome_prod,
                    "pr_unidade": unidade,
                    "status": "SEM_MAPEAMENTO",
                    "fornecedores": [],
                }
            )
            continue

        nome_norm = row_map["pr_nomeNorm"]

        # 2) buscar melhores fornecedores (7d/90d) com filtro n >= n_min
        cur.execute(
            f"""
            SELECT
              pr_nomeFornecedor,
              pr_cnpjFornecedor,
              pr_nomeNorm,
              pr_unidade,
              pr_ncm,
              preco_media,
              preco_min,
              n
            FROM {view_stats}
            WHERE pr_nomeNorm = %s
              AND pr_unidade = %s
              AND n >= %s
            ORDER BY preco_media ASC
            LIMIT %s
            """,
            (nome_norm, unidade, req.n_min, req.limite_fornecedores),
        )
        fornecedores = cur.fetchall()

        if not fornecedores:
            resultados.append(
                {
                    "pr_nomeProduto": nome_prod,
                    "pr_unidade": unidade,
                    "pr_nomeNorm": nome_norm,
                    "status": "DADOS_INSUFICIENTES",
                    "fornecedores": [],
                }
            )
            continue

        resultados.append(
            {
                "pr_nomeProduto": nome_prod,
                "pr_unidade": unidade,
                "pr_nomeNorm": nome_norm,
                "status": "OK",
                "fornecedores": fornecedores,
            }
        )

    cur.close()
    conn.close()
    return {"itens": resultados}
