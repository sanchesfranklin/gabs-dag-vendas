"""Leitura e gravação fora do catálogo: a origem (PostgreSQL) e os arquivos da landing.

As camadas bronze, silver e gold não precisam de conector próprio: são
tabelas Delta, lidas e gravadas com gabs.tables.
"""
