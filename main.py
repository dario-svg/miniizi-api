import os
import re
import hashlib
from typing import List, Optional, Dict

from dotenv import load_dotenv
from fastapi import FastAPI, HTTPException, UploadFile, File
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel

# XML seguro (recomendado)
from defusedxml import ElementTree as ET

load_dotenv()

app = FastAPI(title="MiniIzi API")

# ✅ CORS (necessário para o frontend do Lovable chamar a API)
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=False,
    allow_methods=["*"],
    allow_headers=["*"],
)

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
            detail=f"MySQL driver ausente. Instale 'mysql-connector-python'. Detalhe: {e}",
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


def salva_xml_no_banco(
    xml_bytes: bytes,
    filename: Optional[str],
    content_type: Optional[str],
) -> str:
    """
    Salva o XML bruto no MySQL e retorna o hash SHA-256 (dedup).
    Se já existir, não duplica.
    Requer tabela: xml_bruto (hash_sha256 UNIQUE).
    """
    h = hashlib.sha256(xml_bytes).hexdigest()

    conn = get_conn()
    cur = conn.cursor()

    cur.execute(
        """
        INSERT IGNORE INTO xml_bruto
          (hash_sha256, filename, content_type, tamanho_bytes, xml_blob)
        VALUES
          (%s, %s, %s, %s, %s)
        """,
        (h, filename, content_type, len(xml_bytes), xml_bytes),
    )

    conn.commit()
    cur.close()
    conn.close()

    return h


@app.get("/health")
def health():
    return {"ok": True, "service": "miniizi-api"}


@app.get("/ip")
def ip():
    try:
        import requests

        ip_txt = requests.get("https://api.ipify.org", timeout=8).text.strip()
        return {"ip": ip_txt}
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Erro obtendo IP externo: {e}")


@app.get("/health/db")
def health_db():
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
    - Se NCM (8 dígitos) <= 08000000 => janela curta
    - Caso contrário => janela longa
    - Se NCM vazio/ inválido => longa (fallback seguro)
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


# ----------------------------
# Parse de XML NF-e / NFC-e
# ----------------------------

def _strip_ns(tag: str) -> str:
    return tag.split("}", 1)[-1] if "}" in tag else tag


def _findall_no_ns(root, wanted: str):
    return [el for el in root.iter() if _strip_ns(el.tag) == wanted]


def parse_nfe_itens(xml_bytes: bytes) -> List[Dict]:
    try:
        root = ET.fromstring(xml_bytes)
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"XML inválido: {e}")

    det_nodes = _findall_no_ns(root, "det")
    itens: List[Dict] = []

    def to_float(s: Optional[str]) -> Optional[float]:
        if not s:
            return None
        s = s.strip().replace(",", ".")
        s = re.sub(r"[^0-9.]+", "", s)
        try:
            return float(s) if s else None
        except Exception:
            return None

    for det in det_nodes:
        prod_nodes = [n for n in det.iter() if _strip_ns(n.tag) == "prod"]
        if not prod_nodes:
            continue
        prod = prod_nodes[0]

        def get_text(tagname: str) -> Optional[str]:
            for n in prod.iter():
                if _strip_ns(n.tag) == tagname:
                    return (n.text or "").strip()
            return None

        xprod = get_text("xProd") or ""
        ucom = get_text("uCom") or ""
        ncm = get_text("NCM") or ""

        if xprod:
            itens.append(
                {
                    "pr_nomeProduto": xprod,
                    "pr_unidade": ucom,
                    "pr_ncm": ncm,
                }
            )

    if not itens:
        raise HTTPException(
            status_code=422,
            detail="Não encontrei itens no XML (estrutura NF-e esperada).",
        )

    return itens


@app.post("/analisa_xml", tags=["miniizi"])
async def analisa_xml(
    xml: UploadFile = File(...),
    limite_fornecedores: int = 5,
    n_min: int = 3,
):
    """
    Pipeline 1:
    - Recebe XML real
    - Salva XML bruto no MySQL (dedup por hash)
    - Extrai itens do XML
    - Retorna itens_extraidos + hash_xml
    """
    xml_bytes = await xml.read()

    hash_xml = salva_xml_no_banco(
        xml_bytes=xml_bytes,
        filename=getattr(xml, "filename", None),
        content_type=getattr(xml, "content_type", None),
    )

    itens = parse_nfe_itens(xml_bytes)

    return {
        "ok": True,
        "hash_xml": hash_xml,
        "limite_fornecedores": limite_fornecedores,
        "n_min": n_min,
        "count": len(itens),
        "itens_extraidos": itens,
    }


@app.post("/analisa_xml_full", tags=["miniizi"])
async def analisa_xml_full(
    xml: UploadFile = File(...),
    limite_fornecedores: int = 5,
    n_min: int = 3,
):
    xml_bytes = await xml.read()
    itens_extraidos = parse_nfe_itens(xml_bytes)

    req = AnalisaRequest(
        itens=[
            ItemXML(
                pr_nomeProduto=(it.get("pr_nomeProduto") or "").strip(),
                pr_unidade=(it.get("pr_unidade") or "").strip(),
                pr_ncm=(it.get("pr_ncm") or None),
            )
            for it in itens_extraidos
            if (it.get("pr_nomeProduto") or "").strip()
        ],
        limite_fornecedores=limite_fornecedores,
        n_min=n_min,
        janela_padrao_dias=30,
    )

    return analisa(req)
