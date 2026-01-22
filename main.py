import os
import re
from typing import List, Optional

import mysql.connector
from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

load_dotenv()

app = FastAPI(title="MiniIzi API")


def get_conn():
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
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("SELECT 1")
    cur.fetchone()
    cur.close()
    conn.close()
    return {"ok": True}


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


# --- Janela automática por NCM (regra MVP) ---
VIEW_7D = "v_preco_stats_7d"
VIEW_90D = "v_preco_stats_90d"


def escolhe_view_por_ncm(pr_ncm: Optional[str]) -> str:
    """
    Regra canônica (MVP):
    - Se NCM (8 dígitos) <= 08000000 => janela 7d (hortifruti/carnes/pescados, por sua regra)
    - Caso contrário => janela 90d
    - Se NCM vazio/ inválido => 90d (fallback seguro)
    """
    if not pr_ncm:
        return VIEW_90D

    digits = re.sub(r"\D", "", str(pr_ncm))

    # NCM padrão tem 8 dígitos; completa com zeros à esquerda se vier curto
    if len(digits) < 8:
        digits = digits.zfill(8)

    if len(digits) != 8:
        return VIEW_90D

    n = int(digits)  # ex.: "07019000" -> 7019000
    return VIEW_7D if n <= 8_000_000 else VIEW_90D


class ItemXML(BaseModel):
    pr_nomeProduto: str
    pr_unidade: str
    pr_ncm: Optional[str] = None  # usado para roteamento 7d/90d


class AnalisaRequest(BaseModel):
    itens: List[ItemXML]
    janela_padrao_dias: int = 30  # reservado p/ futuro (mantido)
    limite_fornecedores: int = 5
    n_min: int = 3


@app.post("/analisa", tags=["miniizi"])
def analisa(req: AnalisaRequest):
    """
    Recebe lista de itens (nome do XML + unidade) e devolve:
    - nome normativo provável (via v_mapa_nomeproduto_norm)
    - top fornecedores (via v_preco_stats_7d ou v_preco_stats_90d, automático por NCM)
    Com filtro de confiabilidade: n >= n_min
    """
    conn = get_conn()
    cur = conn.cursor(dictionary=True)

    resultados = []

    for item in req.itens:
        nome_prod = (item.pr_nomeProduto or "").strip()
        unidade = (item.pr_unidade or "").strip()

        # escolhe view automaticamente por NCM
        view_stats = escolhe_view_por_ncm(item.pr_ncm)

        # 1) mapear nome do XML -> nome normativo (pega o mais frequente)
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

        # 2) buscar melhores fornecedores (janela automática 7d/90d) com filtro n >= n_min
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
