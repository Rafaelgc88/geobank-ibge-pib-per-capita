#!/usr/bin/env python3
"""Extrai o PIB per capita oficial dos Municipios do Nordeste a partir do IBGE."""

from __future__ import annotations

import csv
import hashlib
from html.parser import HTMLParser
import io
import json
import re
import sys
import unicodedata
from urllib.parse import unquote, urljoin, urlparse
import urllib.request
import zipfile
from datetime import datetime, timezone
from pathlib import Path


RAIZ_IBGE = "https://ftp.ibge.gov.br/Pib_Municipios/"
FALLBACK_FONTE_ORIGINAL_IBGE = (
    "https://ftp.ibge.gov.br/Pib_Municipios/2022_2023/base/"
    "base_de_dados_2010_2023_txt.zip"
)
PREFIXOS_NORDESTE = ("21", "22", "23", "24", "25", "26", "27", "28", "29")
MIN_REGISTROS_NORDESTE = 1700
CAMPO_PIB_PER_CAPITA = "Produto Interno Bruto per capita, a preços correntes (R$ 1,00)"
LAYOUT_FIXO_IBGE_2010_2023 = {
    "Ano": (0, 4),
    "Código do Município": (46, 53),
    CAMPO_PIB_PER_CAPITA: (952, 971),
}

ROOT_DIR = Path(__file__).resolve().parents[1]
DATA_DIR = ROOT_DIR / "data"
CSV_SAIDA = DATA_DIR / "ibge_pib_per_capita_nordeste.csv"
METADATA_SAIDA = DATA_DIR / "ibge_pib_per_capita_metadata.json"


class LinkParser(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.links: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag.lower() != "a":
            return
        for nome, valor in attrs:
            if nome.lower() == "href" and valor:
                self.links.append(valor)
                return


def log(mensagem: str) -> None:
    print(f"[pib-per-capita] {mensagem}", flush=True)


def normalizar(texto: str) -> str:
    sem_acento = unicodedata.normalize("NFKD", texto)
    sem_acento = "".join(c for c in sem_acento if not unicodedata.combining(c))
    sem_acento = sem_acento.replace("\ufeff", "")
    sem_acento = re.sub(r"\s+", " ", sem_acento)
    return sem_acento.strip().lower()


def baixar_zip(url: str) -> bytes:
    log(f"Baixando ZIP oficial do IBGE: {url}")
    with urllib.request.urlopen(url, timeout=120) as resposta:
        conteudo = resposta.read()
    log(f"ZIP baixado: {len(conteudo):,} bytes")
    return conteudo


def baixar_html(url: str) -> str:
    log(f"Lendo indice do FTP do IBGE: {url}")
    with urllib.request.urlopen(url, timeout=60) as resposta:
        conteudo = resposta.read()
    return conteudo.decode("utf-8", errors="replace")


def extrair_links(html: str) -> list[str]:
    parser = LinkParser()
    parser.feed(html)
    return parser.links


def nome_link(link: str) -> str:
    return unquote(urlparse(link).path.rstrip("/").split("/")[-1])


def ano_final_pasta(link: str) -> int | None:
    nome = nome_link(link)
    match = re.fullmatch(r"(\d{4})(?:_(\d{4}))?", nome)
    if not match:
        return None
    return int(match.group(2) or match.group(1))


def descobrir_fonte_mais_recente() -> tuple[str, bool, bool]:
    """Retorna URL da base, flag de descoberta automatica habilitada e flag de fallback."""
    try:
        links_raiz = extrair_links(baixar_html(RAIZ_IBGE))
        pastas: list[tuple[int, str]] = []
        for link in links_raiz:
            ano_final = ano_final_pasta(link)
            if ano_final is None:
                continue
            pasta_url = urljoin(RAIZ_IBGE, link)
            if not pasta_url.endswith("/"):
                pasta_url += "/"
            pastas.append((ano_final, pasta_url))

        if not pastas:
            raise RuntimeError("Nenhuma pasta YYYY ou YYYY_YYYY encontrada.")

        pastas.sort(key=lambda item: item[0], reverse=True)
        log(
            "Pastas candidatas encontradas: "
            + ", ".join(f"{url.rstrip('/').split('/')[-1]} ({ano})" for ano, url in pastas[:8])
        )

        bases_encontradas: list[tuple[int, int, str]] = []

        for indice, (ano_pasta, pasta_url) in enumerate(pastas):
            base_url = urljoin(pasta_url, "base/")
            try:
                links_base = extrair_links(baixar_html(base_url))
            except Exception as exc:
                log(f"Nao foi possivel ler {base_url}: {exc}")
                continue

            zips: list[tuple[int, str]] = []
            for link in links_base:
                nome = nome_link(link)
                match = re.fullmatch(r"base_de_dados_2010_(\d{4})_txt\.zip", nome)
                if not match:
                    continue
                ano_zip = int(match.group(1))
                zips.append((ano_zip, urljoin(base_url, link)))

            if not zips:
                log(f"Nenhum ZIP TXT de base encontrado em {base_url}")
                continue

            zips.sort(key=lambda item: item[0], reverse=True)
            ano_zip, url_zip = zips[0]
            bases_encontradas.append((ano_zip, ano_pasta, url_zip))

            proxima_pasta = pastas[indice + 1][0] if indice + 1 < len(pastas) else -1
            maior_ano_zip = max(base[0] for base in bases_encontradas)
            if maior_ano_zip >= proxima_pasta:
                break

        if bases_encontradas:
            bases_encontradas.sort(key=lambda item: (item[0], item[1]), reverse=True)
            ano_zip, ano_pasta, url_zip = bases_encontradas[0]
            log(
                "Base descoberta automaticamente: "
                f"pasta final {ano_pasta}, arquivo final {ano_zip}"
            )
            log(f"URL escolhida: {url_zip}")
            return url_zip, True, False

        raise RuntimeError("Nenhuma pasta candidata continha base TXT ZIP compativel.")
    except Exception as exc:
        log(f"Descoberta automatica falhou: {exc}")
        log(f"Fallback usado: {FALLBACK_FONTE_ORIGINAL_IBGE}")
        return FALLBACK_FONTE_ORIGINAL_IBGE, True, True


def sha256(conteudo: bytes) -> str:
    return hashlib.sha256(conteudo).hexdigest()


def escolher_txt(zf: zipfile.ZipFile) -> zipfile.ZipInfo:
    txts = [
        info
        for info in zf.infolist()
        if not info.is_dir() and info.filename.lower().endswith(".txt")
    ]
    if not txts:
        raise RuntimeError("Nenhum arquivo .txt encontrado dentro do ZIP oficial.")
    txts.sort(key=lambda info: info.file_size, reverse=True)
    escolhido = txts[0]
    log(
        "Arquivo TXT interno selecionado: "
        f"{escolhido.filename} ({escolhido.file_size:,} bytes descompactado)"
    )
    return escolhido


def decodificar_txt(conteudo: bytes) -> str:
    tentativas = ("utf-8-sig", "latin-1", "iso-8859-1")
    for encoding in tentativas:
        try:
            texto = conteudo.decode(encoding)
            log(f"Encoding detectado/aceito para o TXT: {encoding}")
            return texto
        except UnicodeDecodeError:
            continue
    raise RuntimeError("Nao foi possivel decodificar o TXT do IBGE.")


def detectar_dialeto(amostra: str) -> csv.Dialect:
    try:
        dialect = csv.Sniffer().sniff(amostra, delimiters=";,\t|")
        log(f"Separador CSV detectado: {repr(dialect.delimiter)}")
        return dialect
    except csv.Error:
        class DialetoPadrao(csv.excel):
            delimiter = ";"

        log("Nao foi possivel detectar separador; usando ';'.")
        return DialetoPadrao


def encontrar_indices(cabecalho: list[str]) -> tuple[int, int, int]:
    normalizado = [normalizar(coluna) for coluna in cabecalho]

    def indice_ano() -> int:
        for i, coluna in enumerate(normalizado):
            if coluna == "ano":
                return i
        raise RuntimeError("Coluna 'Ano' nao encontrada no TXT do IBGE.")

    def indice_codigo_municipio() -> int:
        for i, coluna in enumerate(normalizado):
            if "codigo" in coluna and "municipio" in coluna:
                return i
        raise RuntimeError("Coluna 'Codigo do Municipio' nao encontrada no TXT do IBGE.")

    def indice_pib_per_capita() -> int:
        for i, coluna in enumerate(normalizado):
            if (
                "produto interno bruto per capita" in coluna
                and "precos correntes" in coluna
            ):
                return i
        colunas = "\n".join(f"- {coluna}" for coluna in cabecalho)
        raise RuntimeError(
            "Campo oficial de PIB per capita nao encontrado. "
            "Esperado algo como 'Produto Interno Bruto per capita, "
            "a precos correntes (R$ 1,00)'.\n"
            f"Colunas encontradas:\n{colunas}"
        )

    return indice_ano(), indice_codigo_municipio(), indice_pib_per_capita()


def ler_tabela(texto: str) -> tuple[list[str], list[list[str]], csv.Dialect]:
    linhas = [linha for linha in texto.splitlines() if linha.strip()]
    if not linhas:
        raise RuntimeError("O TXT do IBGE esta vazio.")

    amostra = "\n".join(linhas[:25])
    dialect = detectar_dialeto(amostra)
    leitor = csv.reader(io.StringIO("\n".join(linhas)), dialect)

    for numero_linha, linha in enumerate(leitor, start=1):
        if not linha or not any(campo.strip() for campo in linha):
            continue
        try:
            encontrar_indices(linha)
            cabecalho = [campo.strip() for campo in linha]
            dados = [registro for registro in leitor if any(campo.strip() for campo in registro)]
            log(f"Cabecalho detectado na linha {numero_linha}.")
            return cabecalho, dados, dialect
        except RuntimeError:
            continue

    raise RuntimeError(
        "Nao foi possivel detectar o cabecalho com Ano, Codigo do Municipio "
        "e PIB per capita no TXT do IBGE."
    )


def extrair_campo_fixo(linha: str, nome_coluna: str) -> str:
    inicio, fim = LAYOUT_FIXO_IBGE_2010_2023[nome_coluna]
    return linha[inicio:fim].strip()


def parece_largura_fixa_ibge(texto: str) -> bool:
    for linha in texto.splitlines():
        if not linha.strip():
            continue
        if len(linha) < LAYOUT_FIXO_IBGE_2010_2023[CAMPO_PIB_PER_CAPITA][1]:
            return False
        ano = extrair_campo_fixo(linha, "Ano")
        codigo = extrair_campo_fixo(linha, "Código do Município")
        pib_per_capita = extrair_campo_fixo(linha, CAMPO_PIB_PER_CAPITA)
        return (
            ano.isdigit()
            and codigo.isdigit()
            and len(codigo) == 7
            and bool(re.fullmatch(r"\d+(?:[.,]\d+)?", pib_per_capita))
        )
    return False


def extrair_registros_largura_fixa(texto: str) -> tuple[int, list[dict[str, str]]]:
    log("Usando layout oficial de largura fixa do IBGE 2010-2023.")
    log(
        "Colunas identificadas por posicao: "
        "Ano='0:4', Codigo do Municipio='46:53', "
        f"PIB per capita='952:971' ({CAMPO_PIB_PER_CAPITA})"
    )

    candidatos: list[dict[str, str]] = []
    anos: set[int] = set()

    for numero_linha, linha in enumerate(texto.splitlines(), start=1):
        if not linha.strip():
            continue
        if len(linha) < LAYOUT_FIXO_IBGE_2010_2023[CAMPO_PIB_PER_CAPITA][1]:
            raise RuntimeError(
                "Linha menor que o layout fixo esperado no TXT do IBGE: "
                f"linha {numero_linha} tem {len(linha)} caracteres."
            )

        ano_texto = extrair_campo_fixo(linha, "Ano")
        codigo = extrair_campo_fixo(linha, "Código do Município")
        pib_per_capita = extrair_campo_fixo(linha, CAMPO_PIB_PER_CAPITA)

        if not ano_texto.isdigit():
            raise RuntimeError(
                f"Valor de Ano invalido na linha {numero_linha}: {ano_texto!r}."
            )
        if not codigo.isdigit() or len(codigo) != 7:
            raise RuntimeError(
                "Valor de Codigo do Municipio invalido na linha "
                f"{numero_linha}: {codigo!r}."
            )

        if not codigo.startswith(PREFIXOS_NORDESTE):
            continue
        if not pib_per_capita:
            raise RuntimeError(
                "Campo oficial de PIB per capita vazio na linha "
                f"{numero_linha}, municipio {codigo}."
            )

        ano = int(ano_texto)
        anos.add(ano)
        candidatos.append(
            {
                "cod_ibge": codigo,
                "ano": str(ano),
                "pib_per_capita": pib_per_capita,
            }
        )

    if not anos:
        raise RuntimeError("Nenhum registro de municipio do Nordeste encontrado no TXT.")

    ano_usado = max(anos)
    registros = [registro for registro in candidatos if int(registro["ano"]) == ano_usado]
    registros.sort(key=lambda registro: registro["cod_ibge"])

    if len(registros) < MIN_REGISTROS_NORDESTE:
        raise RuntimeError(
            "Quantidade de registros do Nordeste abaixo do esperado: "
            f"{len(registros)} encontrados para {ano_usado}; minimo exigido "
            f"{MIN_REGISTROS_NORDESTE}."
        )

    log(f"Ano mais recente disponivel: {ano_usado}")
    log(f"Registros do Nordeste no ano usado: {len(registros):,}")
    return ano_usado, registros


def extrair_registros(cabecalho: list[str], linhas: list[list[str]]) -> tuple[int, list[dict[str, str]]]:
    idx_ano, idx_codigo, idx_pib = encontrar_indices(cabecalho)
    log(
        "Colunas identificadas: "
        f"Ano='{cabecalho[idx_ano]}', "
        f"Codigo='{cabecalho[idx_codigo]}', "
        f"PIB per capita='{cabecalho[idx_pib]}'"
    )

    candidatos: list[dict[str, str]] = []
    anos: set[int] = set()
    maior_indice = max(idx_ano, idx_codigo, idx_pib)

    for linha in linhas:
        if len(linha) <= maior_indice:
            continue

        ano_texto = linha[idx_ano].strip()
        codigo = re.sub(r"\D", "", linha[idx_codigo])
        pib_per_capita = linha[idx_pib].strip()

        if not ano_texto.isdigit() or not codigo.startswith(PREFIXOS_NORDESTE):
            continue
        if len(codigo) != 7 or not pib_per_capita:
            continue

        ano = int(ano_texto)
        anos.add(ano)
        candidatos.append(
            {
                "cod_ibge": codigo,
                "ano": str(ano),
                "pib_per_capita": pib_per_capita,
            }
        )

    if not anos:
        raise RuntimeError("Nenhum registro de municipio do Nordeste encontrado no TXT.")

    ano_usado = max(anos)
    registros = [registro for registro in candidatos if int(registro["ano"]) == ano_usado]
    registros.sort(key=lambda registro: registro["cod_ibge"])

    if len(registros) < MIN_REGISTROS_NORDESTE:
        raise RuntimeError(
            "Quantidade de registros do Nordeste abaixo do esperado: "
            f"{len(registros)} encontrados para {ano_usado}; minimo exigido "
            f"{MIN_REGISTROS_NORDESTE}."
        )

    log(f"Ano mais recente disponivel: {ano_usado}")
    log(f"Registros do Nordeste no ano usado: {len(registros):,}")
    return ano_usado, registros


def escrever_csv(registros: list[dict[str, str]]) -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with CSV_SAIDA.open("w", newline="", encoding="utf-8") as arquivo:
        escritor = csv.DictWriter(
            arquivo,
            fieldnames=("cod_ibge", "ano", "pib_per_capita"),
            lineterminator="\n",
        )
        escritor.writeheader()
        escritor.writerows(registros)
    log(f"CSV gerado: {CSV_SAIDA.relative_to(ROOT_DIR)}")


def escrever_metadata(
    fonte_original_ibge: str,
    ano_usado: int,
    sha_zip: str,
    arquivo_interno: str,
    quantidade_registros: int,
    descoberta_automatica_base: bool,
    fallback_usado: bool,
) -> None:
    metadata = {
        "fonte_original_ibge": fonte_original_ibge,
        "data_extracao": datetime.now(timezone.utc).isoformat(),
        "ano_usado": ano_usado,
        "sha256_zip_original": sha_zip,
        "arquivo_interno": arquivo_interno,
        "quantidade_registros": quantidade_registros,
        "metodo": "Campo oficial do IBGE, sem cálculo local PIB/população.",
        "descoberta_automatica_base": descoberta_automatica_base,
        "fallback_usado": fallback_usado,
        "observacao": (
            "CSV filtrado para municipios do Nordeste, identificados pelos "
            "prefixos IBGE 21, 22, 23, 24, 25, 26, 27, 28 e 29."
        ),
    }
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with METADATA_SAIDA.open("w", encoding="utf-8") as arquivo:
        json.dump(metadata, arquivo, ensure_ascii=False, indent=2)
        arquivo.write("\n")
    log(f"Metadata gerado: {METADATA_SAIDA.relative_to(ROOT_DIR)}")


def main() -> int:
    try:
        fonte_original_ibge, descoberta_automatica_base, fallback_usado = (
            descobrir_fonte_mais_recente()
        )
        zip_bytes = baixar_zip(fonte_original_ibge)
        sha_zip = sha256(zip_bytes)
        log(f"SHA-256 do ZIP original: {sha_zip}")

        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as zf:
            txt_info = escolher_txt(zf)
            txt_bytes = zf.read(txt_info)

        texto = decodificar_txt(txt_bytes)
        if parece_largura_fixa_ibge(texto):
            ano_usado, registros = extrair_registros_largura_fixa(texto)
        else:
            try:
                cabecalho, linhas, _dialect = ler_tabela(texto)
                ano_usado, registros = extrair_registros(cabecalho, linhas)
            except RuntimeError as exc:
                raise RuntimeError(
                    "Nao foi possivel identificar o layout/cabecalho do TXT do IBGE "
                    f"nem localizar o campo oficial de PIB per capita. Detalhe: {exc}"
                ) from exc

        escrever_csv(registros)
        escrever_metadata(
            fonte_original_ibge,
            ano_usado,
            sha_zip,
            txt_info.filename,
            len(registros),
            descoberta_automatica_base,
            fallback_usado,
        )
        log("Extracao concluida com sucesso.")
        return 0
    except Exception as exc:
        print(f"ERRO: {exc}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
